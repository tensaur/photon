use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tokio::sync::oneshot;

use lasso::ThreadedRodeo;

use photon_batch::run_batch_thread;
use photon_core::types::config::{BatchConfig, UplinkConfig};
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::CompressorKind;
use photon_transport::TransportKind;
use photon_transport::codec::CodecTransport;
use photon_uplink::{TransportError as UplinkTransportError, UplinkThreadError, run_uplink};
use photon_wal::{DiskWalConfig, Wal, WalKind};

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
    wal: WalKind,
    channel_capacity: usize,
    batch: BatchConfig,
    endpoint: Option<String>,
    transport: TransportKind,
    codec: CodecKind,
    compressor: CompressorKind,
}

impl Default for RunBuilder {
    fn default() -> Self {
        Self {
            run_id: None,
            wal_dir: None,
            wal: WalKind::default(),
            channel_capacity: 65_536,
            batch: BatchConfig::default(),
            endpoint: None,
            transport: TransportKind::default(),
            codec: CodecKind::default(),
            compressor: CompressorKind::default(),
        }
    }
}

impl RunBuilder {
    pub fn run_id(mut self, id: RunId) -> Self {
        self.run_id = Some(id);
        self
    }

    pub fn wal_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.wal_dir = Some(dir.into());
        self
    }

    pub fn wal(mut self, wal: WalKind) -> Self {
        self.wal = wal;
        self
    }

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

    pub fn codec(mut self, codec: CodecKind) -> Self {
        self.codec = codec;
        self
    }

    pub fn compressor(mut self, compressor: CompressorKind) -> Self {
        self.compressor = compressor;
        self
    }

    pub fn transport(mut self, transport: TransportKind) -> Self {
        self.transport = transport;
        self
    }

    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }

    pub fn start(self) -> Result<Run, StartError> {
        let run_id = self.run_id.unwrap_or_default();

        let (wal_appender, wal) =
            self.wal
                .open(self.wal_dir.as_deref(), run_id, DiskWalConfig::default())?;

        let interner = Arc::new(ThreadedRodeo::default());
        let (accumulator, rx) = Accumulator::new(self.channel_capacity);

        let start_sequence = wal
            .read_from(SequenceNumber::ZERO)
            .ok()
            .and_then(|batches: Vec<_>| batches.last().map(|b| b.sequence_number.next()))
            .unwrap_or_else(|| SequenceNumber::ZERO.next());

        let (batch_tx, batch_rx) = crossbeam_channel::bounded(64);

        let codec = self.codec;
        let batch_interner = Arc::clone(&interner);
        let batch_handle = thread::Builder::new()
            .name(format!("photon-batch-{run_id}"))
            .spawn(move || {
                run_batch_thread(
                    run_id,
                    batch_interner,
                    codec,
                    self.compressor,
                    wal_appender,
                    start_sequence,
                    rx,
                    batch_tx,
                    self.batch,
                )
            })
            .expect("failed to spawn batch thread");

        let uplink_handle = self.endpoint.map(|endpoint| {
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let uplink_wal = wal.clone();
            let uplink_config = UplinkConfig::default();
            let transport_kind = self.transport;

            let handle = thread::Builder::new()
                .name(format!("photon-uplink-{run_id}"))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(UplinkThreadError::Runtime)?;

                    rt.block_on(async {
                        let bt = transport_kind
                            .connect(&endpoint)
                            .await
                            .map_err(UplinkTransportError::from)?;

                        let transport = CodecTransport::new(codec, bt);

                        run_uplink(
                            transport,
                            run_id,
                            uplink_wal,
                            uplink_config,
                            shutdown_rx,
                            batch_rx,
                        )
                        .await
                    })
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
            wal,
        ))
    }
}
