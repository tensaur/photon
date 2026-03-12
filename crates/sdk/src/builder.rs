use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use photon_core::types::config::{BatchConfig, SenderConfig};
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::codec::CodecChoice;
use photon_protocol::compressor::CompressorChoice;

use crate::domain::pipeline::accumulator::Accumulator;
use crate::domain::pipeline::ack_tracker::AckTracker;
use crate::domain::pipeline::batch_builder::BatchBuilder;
use crate::domain::pipeline::interner::{InternResolver, MetricKeyInterner};
use crate::domain::pipeline::recovery::RecoveryManager;
use crate::domain::pipeline::sender::{Sender, SenderStats};
use crate::domain::ports::error::SenderThreadError;
use crate::domain::ports::wal::WalStorage;
use crate::domain::service::{SenderHandle, Service};
use crate::inbound::error::SdkError;
use crate::inbound::run::Run;
use crate::outbound::grpc::GrpcTransport;
use crate::outbound::wal::{DiskWalConfig, SharedDiskWal};

/// Configure and start a [`Run`].
///
/// All fields have defaults. The simplest usage is:
/// ```ignore
/// let mut run = Run::builder().start()?;
/// ```
pub struct RunBuilder {
    run_id: Option<RunId>,
    wal_dir: Option<PathBuf>,
    channel_capacity: usize,
    spill_capacity: usize,
    batch: BatchConfig,
    endpoint: Option<String>,
    codec: CodecChoice,
    compressor: CompressorChoice,
}

impl Default for RunBuilder {
    fn default() -> Self {
        Self {
            run_id: None,
            wal_dir: None,
            channel_capacity: 65_536,
            spill_capacity: 16_384,
            batch: BatchConfig::default(),
            endpoint: None,
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

    /// Connect to a remote photon server for metric ingestion.
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }

    /// Pick adapters and start the pipeline.
    pub fn start(self) -> Result<Run<Service<SharedDiskWal>>, SdkError> {
        let run_id = self.run_id.unwrap_or_default();

        // 1. Open WAL
        let wal_dir = self.wal_dir.as_deref();
        let wal = SharedDiskWal::open(wal_dir, run_id, DiskWalConfig::default())
            .map_err(SdkError::WalRecoveryFailed)?;

        // 2. Create accumulator + interner
        let interner = Arc::new(MetricKeyInterner::new());
        let resolver = InternResolver::new(Arc::clone(&interner));
        let (accumulator, rx) = Accumulator::new(self.channel_capacity, self.spill_capacity);

        // 3. Compute start sequence from WAL
        let start_sequence = wal
            .read_from(SequenceNumber::ZERO)
            .ok()
            .and_then(|batches: Vec<_>| batches.last().map(|b| b.sequence_number.next()))
            .unwrap_or_else(|| SequenceNumber::ZERO.next());

        // 4. Spawn batch builder thread
        let batch_wal = wal.clone();
        let builder_handle = BatchBuilder::new(
            run_id, rx, resolver, self.codec, batch_wal,
            self.compressor, self.batch, start_sequence,
        )
        .spawn();

        // 5. Spawn sender thread (if endpoint configured)
        let sender_wal = wal.clone();
        let sender_handle = self.endpoint.map(|endpoint| {
            let (shutdown_tx, shutdown_rx) = oneshot::channel();

            let handle = std::thread::Builder::new()
                .name(format!("photon-sender-{run_id}"))
                .spawn(move || run_sender(endpoint, run_id, sender_wal, SenderConfig::default(), shutdown_rx))
                .expect("failed to spawn sender thread");

            SenderHandle {
                shutdown_tx: Some(shutdown_tx),
                handle,
            }
        });

        // 6. Assemble service from pre-built parts
        let service = Service::new(
            run_id,
            accumulator,
            interner,
            builder_handle,
            sender_handle,
            wal,
        );

        Ok(Run { service })
    }
}

fn run_sender(
    endpoint: String,
    run_id: RunId,
    wal: SharedDiskWal,
    config: SenderConfig,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<SenderStats, SenderThreadError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(SenderThreadError::Runtime)?;

    rt.block_on(async move {
        let transport = GrpcTransport::connect(&endpoint, 64)
            .await?;

        let recovery = RecoveryManager::new(wal.clone(), run_id);

        let watermark = if recovery
            .is_clean()
            .map_err(|e| SenderThreadError::Recovery(e.into()))?
        {
            tracing::debug!(run_id = %run_id, "clean WAL, skipping recovery");
            SequenceNumber::ZERO
        } else {
            let result = recovery
                .recover(&transport)
                .await?;

            if result.batches_to_replay > 0 {
                tracing::info!(
                    run_id = %run_id,
                    batches = result.batches_to_replay,
                    bytes = result.bytes_to_replay,
                    "replaying uncommitted batches from WAL"
                );
            }

            result.effective_watermark
        };

        let mut sender = Sender::new(
            transport,
            wal.clone(),
            config,
            watermark,
            shutdown_rx,
        );

        let mut ack_tracker = AckTracker::new(wal, watermark, 10);

        let mut result = sender
            .run(|seq| {
                if let Err(e) = ack_tracker.on_ack(seq) {
                    tracing::error!("ack tracker error for seq {seq}: {e}");
                }
            })
            .await?;

        match ack_tracker.shutdown() {
            Ok(ack_stats) => {
                result.watermark_advances = ack_stats.watermark_advances;
                result.segments_truncated = ack_stats.segments_truncated;
            }
            Err(e) => tracing::error!("ack tracker shutdown failed: {e}"),
        }

        Ok(result)
    })
}
