use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use photon_core::types::config::BatchConfig;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_core::types::metric::MetricBatch;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;

use crate::domain::pipeline::accumulator::Accumulator;
use crate::domain::pipeline::batch_builder::{BatchBuilder, BatchBuilderError, BuilderStats};
use crate::domain::pipeline::interner::{InternResolver, MetricKeyInterner, RawPoint};
use crate::domain::ports::wal::WalStorage;
use crate::inbound::error::SdkError;

pub struct PipelineConfig {
    pub channel_capacity: usize,
    pub spill_capacity: usize,
    pub batch: BatchConfig,
}

#[derive(Clone, Debug)]
pub struct PipelineStats {
    pub points_logged: u64,
    pub points_dropped: u64,
    pub batches_flushed: u64,
    pub bytes_compressed: u64,
    pub bytes_uncompressed: u64,
}

pub trait PipelineService {
    fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), SdkError>;
    fn finish(self) -> Result<PipelineStats, SdkError>;

    fn run_id(&self) -> RunId;
    fn points_logged(&self) -> u64;
    fn points_dropped(&self) -> u64;
}

pub struct Service {
    run_id: RunId,
    accumulator: Accumulator<RawPoint>,
    interner: Arc<MetricKeyInterner>,
    builder_handle: Option<JoinHandle<Result<BuilderStats, BatchBuilderError>>>,
    points_logged: u64,
}

impl Service {
    pub fn start<W, K, C>(
        run_id: RunId,
        wal: W,
        codec: K,
        compressor: C,
        config: PipelineConfig,
    ) -> Self
    where
        W: WalStorage,
        K: Codec<MetricBatch>,
        C: Compressor,
    {
        let interner = Arc::new(MetricKeyInterner::new());
        let resolver = InternResolver::new(Arc::clone(&interner));
        let (accumulator, rx) = Accumulator::new(config.channel_capacity, config.spill_capacity);

        let builder_handle = BatchBuilder::new(
            run_id,
            rx,
            resolver,
            codec,
            wal,
            compressor,
            config.batch,
            SequenceNumber::ZERO,
        )
        .spawn();

        Self {
            run_id,
            accumulator,
            interner,
            builder_handle: Some(builder_handle),
            points_logged: 0,
        }
    }
}

impl PipelineService for Service {
    fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), SdkError> {
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

    /// Flushes remaining points and waits for the pipeline to drain.
    fn finish(mut self) -> Result<PipelineStats, SdkError> {
        let points_logged = self.points_logged;
        let points_dropped = self.accumulator.points_dropped();

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

        Ok(PipelineStats {
            points_logged,
            points_dropped,
            batches_flushed: builder_stats.batches_flushed,
            bytes_compressed: builder_stats.bytes_compressed,
            bytes_uncompressed: builder_stats.bytes_uncompressed,
        })
    }

    fn run_id(&self) -> RunId {
        self.run_id
    }

    fn points_logged(&self) -> u64 {
        self.points_logged
    }

    fn points_dropped(&self) -> u64 {
        self.accumulator.points_dropped()
    }
}
