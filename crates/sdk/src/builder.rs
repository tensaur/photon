use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use photon_core::types::batch::WireBatch;
use photon_core::types::config::{BatchConfig, SenderConfig};
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::codec::CodecChoice;
use photon_protocol::compressor::CompressorChoice;
use photon_transport::TransportChoice;

use crate::domain::pipeline::accumulator::Accumulator;
use crate::domain::pipeline::interner::MetricKeyInterner;
use crate::domain::pipeline::pipeline::Pipeline;
use crate::domain::pipeline::sender;
use crate::domain::ports::error::SenderThreadError;
use crate::domain::ports::wal::WalStorage;
use crate::domain::service::{SenderHandle, Service};
use crate::inbound::error::SdkError;
use crate::inbound::run::Run;
use crate::outbound::wal::WalChoice;

/// Configure and start a [`Run`].
///
/// All fields have defaults. The simplest usage is:
/// ```ignore
/// let mut run = Run::builder().start()?;
/// ```
pub struct RunBuilder {
    run_id: Option<RunId>,
    wal_dir: Option<PathBuf>,
    in_memory_wal: bool,
    channel_capacity: usize,
    spill_capacity: usize,
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
            in_memory_wal: false,
            channel_capacity: 65_536,
            spill_capacity: 16_384,
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

    pub fn in_memory_wal(mut self) -> Self {
        self.in_memory_wal = true;
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
    pub fn start(self) -> Result<Run<Service<WalChoice>>, SdkError> {
        let run_id = self.run_id.unwrap_or_default();

        // 1. Open WAL
        let wal = WalChoice::open(self.wal_dir.as_deref(), run_id, self.in_memory_wal)
            .map_err(SdkError::WalRecoveryFailed)?;

        // 2. Create accumulator + interner
        let interner = Arc::new(MetricKeyInterner::new());
        let (accumulator, rx) = Accumulator::new(self.channel_capacity, self.spill_capacity);

        // 3. Compute start sequence from WAL
        let start_sequence = wal
            .read_from(SequenceNumber::ZERO)
            .ok()
            .and_then(|batches: Vec<_>| batches.last().map(|b| b.sequence_number.next()))
            .unwrap_or_else(|| SequenceNumber::ZERO.next());

        // 4. Create channel from builder → sender
        let (batch_tx, batch_rx) = crossbeam_channel::bounded(64);

        // 5. Spawn pipeline thread
        let batch_wal = wal.clone();
        let pipeline_handle = Pipeline::new(
            run_id,
            rx,
            Arc::clone(&interner),
            self.codec,
            batch_wal,
            self.compressor,
            self.batch,
            start_sequence,
            batch_tx,
        )
        .spawn();

        // 6. Spawn sender thread (if endpoint configured)
        let sender_wal = wal.clone();
        let sender_handle = self.endpoint.map(|endpoint| {
            let (shutdown_tx, shutdown_rx) = oneshot::channel();

            let handle = std::thread::Builder::new()
                .name(format!("photon-sender-{run_id}"))
                .spawn(move || {
                    sender::run_thread(
                        self.transport,
                        endpoint,
                        run_id,
                        sender_wal,
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

        // 7. Assemble service from pre-built parts
        let service = Service::new(
            run_id,
            accumulator,
            interner,
            pipeline_handle,
            sender_handle,
            wal,
        );

        Ok(Run { service })
    }
}
