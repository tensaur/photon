use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use photon_core::types::config::{BatchConfig, SenderConfig};
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_flush::{FlushService, MetricKeyInterner, run_flush_thread};
use photon_protocol::codec::CodecChoice;
use photon_protocol::compressor::CompressorChoice;
use photon_send::run_sender_thread;
use photon_transport::TransportChoice;
use photon_wal::{WalChoice, WalManager};

use crate::accumulator::Accumulator;
use crate::error::StartError;
use crate::run::{Run, SenderHandle};

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

    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.batch.flush_interval = interval;
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

        // 1. Open WAL
        let (appender, manager) = self
            .wal
            .open(self.wal_dir.as_deref(), run_id)?;

        // 2. Create accumulator + interner
        let interner = Arc::new(MetricKeyInterner::new());
        let (accumulator, rx) = Accumulator::new(self.channel_capacity);

        // 3. Compute start sequence from WAL
        let start_sequence = manager
            .read_from(SequenceNumber::ZERO)
            .ok()
            .and_then(|batches: Vec<_>| batches.last().map(|b| b.sequence_number.next()))
            .unwrap_or_else(|| SequenceNumber::ZERO.next());

        // 4. Create FlushService
        let flush_service = FlushService::new(
            run_id,
            Arc::clone(&interner),
            self.codec,
            self.compressor,
            appender,
            start_sequence,
        );

        // 5. Create batch channel
        let (batch_tx, batch_rx) = crossbeam_channel::bounded(64);

        // 6. Spawn flush thread
        let batch_config = self.batch;
        let flush_handle = std::thread::Builder::new()
            .name(format!("photon-flush-{run_id}"))
            .spawn(move || run_flush_thread(flush_service, rx, batch_tx, batch_config))
            .expect("failed to spawn flush thread");

        // 7. Spawn sender thread (if endpoint configured)
        let endpoint = self.endpoint;
        let transport = self.transport;
        let sender_manager = manager.clone();

        let sender_handle = endpoint.map(|endpoint| {
            let (shutdown_tx, shutdown_rx) = oneshot::channel();

            let handle = std::thread::Builder::new()
                .name(format!("photon-sender-{run_id}"))
                .spawn(move || {
                    run_sender_thread(
                        transport,
                        endpoint,
                        run_id,
                        sender_manager,
                        SenderConfig::default(),
                        shutdown_rx,
                        batch_rx,
                    )
                })
                .expect("failed to spawn sender thread");

            SenderHandle {
                shutdown_tx: Some(shutdown_tx),
                handle,
            }
        });

        Ok(Run::new(
            run_id,
            accumulator,
            interner,
            flush_handle,
            sender_handle,
            manager,
        ))
    }
}
