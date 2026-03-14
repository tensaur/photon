use photon_core::types::id::RunId;

use crate::domain::service::PipelineStats;
use crate::domain::service::SdkService;
use crate::inbound::error::SdkError;

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
pub struct Run<S: SdkService> {
    pub(crate) service: S,
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
    pub batches_rejected: u64,
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
            batches_rejected: s.batches_rejected,
        }
    }
}

impl<S: SdkService> Run<S> {
    /// Log a single metric data point.
    pub fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), SdkError> {
        self.service.log(key, value, step).map_err(Into::into)
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
        self.service
            .finish()
            .map(RunStats::from)
            .map_err(Into::into)
    }
}
