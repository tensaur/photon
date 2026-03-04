use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use photon_core::pipeline::accumulator::Accumulator;
use photon_core::pipeline::batch_builder::{BatchBuilder, BatchBuilderError, BuilderStats};
use photon_core::types::config::BatchConfig;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::compressor::NoopCompressor;
use crate::error::SdkError;
use crate::interner::{InternResolver, MetricKeyInterner, RawPoint};
use crate::wal::{DiskWalConfig, InMemoryWal, SharedDiskWal};

use photon_proto::codec::ProtobufCodec;

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
pub struct Run {
    run_id: RunId,
    accumulator: Accumulator<RawPoint>,
    interner: Arc<MetricKeyInterner>,
    builder_handle: Option<JoinHandle<Result<BuilderStats, BatchBuilderError>>>,
    points_logged: u64,
}

/// Statistics returned when a run is finished.
#[derive(Clone, Debug)]
pub struct RunStats {
    pub points: u64,
    pub points_dropped: u64,
    pub batches: u64,
    pub bytes_compressed: u64,
    pub bytes_uncompressed: u64,
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
}

impl Run {
    pub fn builder() -> RunBuilder {
        RunBuilder::default()
    }

    /// Log a single metric data point.
    pub fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), SdkError> {
        let metric_key = self.interner.get_or_intern(key)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        self.accumulator.push(RawPoint {
            key: metric_key,
            value,
            step,
            timestamp_ns: now,
        });

        self.points_logged += 1;
        Ok(())
    }

    pub fn points_logged(&self) -> u64 {
        self.points_logged
    }

    pub fn points_dropped(&self) -> u64 {
        self.accumulator.points_dropped()
    }

    pub fn id(&self) -> RunId {
        self.run_id
    }

    /// Flushes remaining points and waits for the pipeline to drain.
    pub fn finish(mut self) -> Result<RunStats, SdkError> {
        let points_dropped = self.accumulator.points_dropped();
        let points_logged = self.points_logged;

        // Replaces accumulator with a dummy so the real one drops,
        // which closes the channel and lets the builder drain.
        let _old = std::mem::replace(&mut self.accumulator, {
            let (acc, _rx) = Accumulator::new(1, 1);
            acc
        });
        drop(_old);

        let handle = self
            .builder_handle
            .take()
            .expect("finish called more than once");

        let builder_stats = handle
            .join()
            .map_err(|_| SdkError::Unknown(anyhow::anyhow!("builder thread panicked")))?
            .map_err(|e| SdkError::Unknown(e.into()))?;

        Ok(RunStats {
            points: points_logged,
            points_dropped,
            batches: builder_stats.batches_flushed,
            bytes_compressed: builder_stats.bytes_compressed,
            bytes_uncompressed: builder_stats.bytes_uncompressed,
        })
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

    pub fn start(self) -> Result<Run, SdkError> {
        let run_id = self.run_id.unwrap_or_default();
        let interner = Arc::new(MetricKeyInterner::new());
        let resolver = InternResolver::new(Arc::clone(&interner));

        let codec = ProtobufCodec;
        let compressor = NoopCompressor;

        let (accumulator, rx) = Accumulator::new(self.channel_capacity, self.spill_capacity);

        let builder_handle = if self.in_memory_wal {
            let wal = InMemoryWal::new();
            BatchBuilder::new(
                run_id,
                rx,
                resolver,
                codec,
                wal,
                compressor,
                self.batch,
                SequenceNumber::ZERO,
            )
            .spawn()
        } else {
            let wal_dir = self.wal_dir.as_deref();
            let wal = SharedDiskWal::open(wal_dir, run_id, DiskWalConfig::default())
                .map_err(SdkError::WalRecoveryFailed)?;
            BatchBuilder::new(
                run_id,
                rx,
                resolver,
                codec,
                wal,
                compressor,
                self.batch,
                SequenceNumber::ZERO,
            )
            .spawn()
        };

        Ok(Run {
            run_id,
            accumulator,
            interner,
            builder_handle: Some(builder_handle),
            points_logged: 0,
        })
    }
}
