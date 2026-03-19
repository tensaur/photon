use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use lasso::ThreadedRodeo;

use photon_batch::run_batch_thread;
use photon_core::types::config::{BatchConfig, UplinkConfig};
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::codec::CodecChoice;
use photon_protocol::compressor::CompressorChoice;
use photon_transport::TransportChoice;
use photon_uplink::run_uplink_thread;
use photon_wal::{WalChoice, WalManager};

use crate::accumulator::Accumulator;
use crate::error::StartError;
use crate::run::{Run, UplinkHandle};

/// Configure and start a [`Run`].
///
/// All fields have defaults. The simplest usage is:
/// ```ignore
/// let mut run = Run::builder().start()?;
/// ```
pub struct RunBuilder {
    run_id: Option<RunId>,
    wal_dir: Option<PathBuf>,
    wal: WalChoice,
    channel_capacity: usize,
    batch: BatchConfig,
    endpoint: Option<String>,
    transport: TransportChoice,
    codec: CodecChoice,
    compressor: CompressorChoice,
}

impl Default for RunBuilder {
    fn default() -> Self {
        Self {
            run_id: None,
            wal_dir: None,
            wal: WalChoice::Disk,
            channel_capacity: 65_536,
            batch: BatchConfig::default(),
            endpoint: None,
            transport: TransportChoice::default(),
            codec: CodecChoice::default(),
            compressor: CompressorChoice::default(),
        }
    }
}

impl RunBuilder {
    /// Use a specific run ID instead of generating one.
    pub fn run_id(mut self, id: RunId) -> Self {
        self.run_id = Some(id);
        self
    }

    pub fn wal_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.wal_dir = Some(dir.into());
        self
    }

    pub fn wal(mut self, wal: WalChoice) -> Self {
        self.wal = wal;
        self
    }

    /// Maximum points per batch before a flush is triggered.
    pub fn max_points_per_batch(mut self, n: usize) -> Self {
        self.batch.max_points = n;
        self
    }

    pub fn batch_interval(mut self, interval: Duration) -> Self {
        self.batch.batch_interval = interval;
        self
    }

    pub fn channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }

    pub fn codec(mut self, codec: CodecChoice) -> Self {
        self.codec = codec;
        self
    }

    pub fn compressor(mut self, compressor: CompressorChoice) -> Self {
        self.compressor = compressor;
        self
    }

    /// Select the transport adapter. Defaults to TCP.
    pub fn transport(mut self, transport: TransportChoice) -> Self {
        self.transport = transport;
        self
    }

    /// Connect to a remote photon server for metric ingestion.
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }

    /// Pick adapters and start the pipeline.
    pub fn start(self) -> Result<Run, StartError> {
        let run_id = self.run_id.unwrap_or_default();

        let (wal_appender, wal_manager) = self.wal.open(self.wal_dir.as_deref(), run_id)?;

        let interner = Arc::new(ThreadedRodeo::default());
        let (accumulator, rx) = Accumulator::new(self.channel_capacity);

        let start_sequence = wal_manager
            .read_from(SequenceNumber::ZERO)
            .ok()
            .and_then(|batches: Vec<_>| batches.last().map(|b| b.sequence_number.next()))
            .unwrap_or_else(|| SequenceNumber::ZERO.next());

        let (batch_tx, batch_rx) = crossbeam_channel::bounded(64);

        let batch_interner = Arc::clone(&interner);
        let batch_handle = std::thread::Builder::new()
            .name(format!("photon-batch-{run_id}"))
            .spawn(move || {
                run_batch_thread(
                    run_id,
                    batch_interner,
                    self.codec,
                    self.compressor,
                    wal_appender,
                    start_sequence,
                    rx,
                    batch_tx,
                    self.batch,
                )
            })
            .expect("failed to spawn batch thread");

        let uplink_wal = wal_manager;
        let uplink_handle = self.endpoint.map(|endpoint| {
            let (shutdown_tx, shutdown_rx) = oneshot::channel();

            let handle = std::thread::Builder::new()
                .name(format!("photon-uplink-{run_id}"))
                .spawn(move || {
                    run_uplink_thread(
                        self.transport,
                        endpoint,
                        run_id,
                        uplink_wal,
                        UplinkConfig::default(),
                        shutdown_rx,
                        batch_rx,
                    )
                })
                .expect("failed to spawn uplink thread");

            UplinkHandle {
                shutdown_tx: Some(shutdown_tx),
                handle,
            }
        });

        Ok(Run::new(
            run_id,
            accumulator,
            interner,
            batch_handle,
            uplink_handle,
            self.wal,
            self.wal_dir,
        ))
    }
}
