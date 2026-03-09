use std::path::PathBuf;
use std::time::Duration;

use photon_core::types::config::{BatchConfig, SenderConfig};
use photon_core::types::id::RunId;
use photon_protocol::codec::protobuf::codec::ProtobufCodec;
use photon_protocol::compressor::noop::NoopCompressor;

use crate::domain::service::{PipelineConfig, PipelineService, PipelineStats, Service};
use crate::inbound::error::SdkError;
use crate::outbound::wal::{DiskWalConfig, InMemoryWal, SharedDiskWal};

/// A logging run.
///
/// # Example
/// ```ignore
/// let mut run = Run::builder().start()?;
///
/// for step in 0..100 {
///     run.log("train/loss", loss, step)?;
///     run.log("train/accuracy", acc, step)?;
/// }
///
/// let stats = run.finish()?;
/// ```
pub struct Run<S: PipelineService> {
    service: S,
}

#[derive(Clone, Debug)]
pub struct RunStats {
    pub points: u64,
    pub points_dropped: u64,
    pub batches: u64,
    pub bytes_compressed: u64,
    pub bytes_uncompressed: u64,
    pub batches_sent: u64,
    pub batches_acked: u64,
}

impl From<PipelineStats> for RunStats {
    fn from(s: PipelineStats) -> Self {
        Self {
            points: s.points_logged,
            points_dropped: s.points_dropped,
            batches: s.batches_flushed,
            bytes_compressed: s.bytes_compressed,
            bytes_uncompressed: s.bytes_uncompressed,
            batches_sent: s.batches_sent,
            batches_acked: s.batches_acked,
        }
    }
}

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
}

impl Run<Service> {
    pub fn builder() -> RunBuilder {
        RunBuilder::default()
    }
}

impl<S: PipelineService> Run<S> {
    /// Log a single metric data point.
    pub fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), SdkError> {
        self.service.log(key, value, step)
    }

    pub fn points_logged(&self) -> u64 {
        self.service.points_logged()
    }

    pub fn points_dropped(&self) -> u64 {
        self.service.points_dropped()
    }

    pub fn id(&self) -> RunId {
        self.service.run_id()
    }

    /// Flushes remaining points and waits for the pipeline to drain.
    pub fn finish(self) -> Result<RunStats, SdkError> {
        self.service.finish().map(RunStats::from)
    }
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

    /// Use an in-memory WAL (no disk I/O). Useful for testing.
    pub fn in_memory(mut self) -> Self {
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

    /// Connect to a remote photon server for metric ingestion.
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }

    /// Pick adapters and start the pipeline.
    pub fn start(self) -> Result<Run<Service>, SdkError> {
        let run_id = self.run_id.unwrap_or_default();
        let codec = ProtobufCodec;
        let compressor = NoopCompressor;

        let config = PipelineConfig {
            channel_capacity: self.channel_capacity,
            spill_capacity: self.spill_capacity,
            batch: self.batch,
            endpoint: self.endpoint,
            sender: SenderConfig::default(),
        };

        let service = if self.in_memory_wal {
            let wal = InMemoryWal::new();
            Service::start(run_id, wal, codec, compressor, config)
        } else {
            let wal_dir = self.wal_dir.as_deref();
            let wal = SharedDiskWal::open(wal_dir, run_id, DiskWalConfig::default())
                .map_err(SdkError::WalRecoveryFailed)?;
            Service::start(run_id, wal, codec, compressor, config)
        };

        Ok(Run { service })
    }
}
