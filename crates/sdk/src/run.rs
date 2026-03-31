use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use lasso::ThreadedRodeo;

use photon_batch::{BatchError, BatchStats};
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricKey, RawPoint, Step};
use photon_uplink::{UplinkStats, UplinkThreadError};
use photon_wal::Wal;
use tokio::sync::oneshot;

use crate::accumulator::Accumulator;
use crate::error::{FinishError, LogError};

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

pub(crate) struct UplinkHandle {
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub handle: JoinHandle<Result<UplinkStats, UplinkThreadError>>,
}

pub struct Run {
    id: RunId,
    accumulator: Accumulator<RawPoint>,
    interner: Arc<ThreadedRodeo>,
    batch_handle: JoinHandle<Result<BatchStats, BatchError>>,
    uplink_handle: Option<UplinkHandle>,
    wal: Arc<dyn Wal>,
    points_logged: u64,
    last_step: Vec<Option<Step>>,
}

impl Run {
    pub(crate) fn new(
        id: RunId,
        accumulator: Accumulator<RawPoint>,
        interner: Arc<ThreadedRodeo>,
        batch_handle: JoinHandle<Result<BatchStats, BatchError>>,
        uplink_handle: Option<UplinkHandle>,
        wal: Arc<dyn Wal>,
    ) -> Self {
        Self {
            id,
            accumulator,
            interner,
            batch_handle,
            uplink_handle,
            wal,
            points_logged: 0,
            last_step: Vec::new(),
        }
    }

    /// Log a single metric data point.
    ///
    /// Steps must be monotonically increasing per metric key. Logging a step
    /// less than or equal to the previous step for the same key returns an error.
    pub fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), LogError> {
        Metric::new(key)?;
        let spur = self.interner.get_or_intern(key);
        let metric_key = MetricKey::new(lasso::Key::into_usize(spur));
        let step = Step::new(step);

        let idx = metric_key.index();
        if idx >= self.last_step.len() {
            self.last_step.resize(idx + 1, None);
        }
        if let Some(last) = self.last_step[idx]
            && step <= last
        {
            return Err(LogError::StepNotMonotonic {
                key: key.to_owned(),
                step: step.as_u64(),
                last: last.as_u64(),
            });
        }
        self.last_step[idx] = Some(step);

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
        self.id
    }

    /// Flushes remaining points and waits for the pipeline to drain.
    pub fn finish(mut self) -> Result<RunStats, FinishError> {
        let points_logged = self.points_logged;
        let points_dropped = self.accumulator.points_dropped();

        // Drop the accumulator to close the channel, signaling the batch thread to flush.
        drop(std::mem::replace(&mut self.accumulator, {
            let (acc, _rx) = Accumulator::new(1);
            acc
        }));

        let batch_stats = self
            .batch_handle
            .join()
            .map_err(|_| FinishError::Panicked)?
            .map_err(FinishError::Batch)?;

        let (batches_sent, batches_acked, batches_rejected) = match self.uplink_handle {
            Some(mut ctx) => {
                drop(ctx.shutdown_tx.take());

                let uplink_stats = ctx
                    .handle
                    .join()
                    .map_err(|_| FinishError::Panicked)?
                    .map_err(FinishError::Uplink)?;

                (
                    uplink_stats.batches_sent,
                    uplink_stats.batches_acked,
                    uplink_stats.rejections_received,
                )
            }
            None => (0, 0, 0),
        };

        if batches_sent == 0 || batches_acked >= batches_sent {
            let _ = self.wal.close();
        }

        Ok(RunStats {
            points: points_logged,
            points_dropped,
            batches: batch_stats.batches_created,
            bytes_compressed: batch_stats.bytes_compressed,
            bytes_uncompressed: batch_stats.bytes_uncompressed,
            batches_sent,
            batches_acked,
            batches_rejected,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::Run;
    use crate::error::LogError;

    fn new_run() -> Run {
        Run::builder().start().expect("start should succeed")
    }

    #[test]
    fn test_monotonic_steps_accepted() {
        let mut run = new_run();
        run.log("train/loss", 0.5, 1).unwrap();
        run.log("train/loss", 0.4, 2).unwrap();
        run.log("train/loss", 0.3, 3).unwrap();
    }

    #[test]
    fn test_duplicate_step_rejected() {
        let mut run = new_run();
        run.log("train/loss", 0.5, 1).unwrap();
        let err = run.log("train/loss", 0.4, 1).unwrap_err();
        assert!(matches!(err, LogError::StepNotMonotonic { .. }));
    }

    #[test]
    fn test_decreasing_step_rejected() {
        let mut run = new_run();
        run.log("train/loss", 0.5, 5).unwrap();
        let err = run.log("train/loss", 0.4, 3).unwrap_err();
        assert!(matches!(
            err,
            LogError::StepNotMonotonic {
                step: 3,
                last: 5,
                ..
            }
        ));
    }

    #[test]
    fn test_different_metrics_independent() {
        let mut run = new_run();
        run.log("train/loss", 0.5, 1).unwrap();
        run.log("eval/loss", 0.6, 1).unwrap(); // same step, different metric — ok
        run.log("train/loss", 0.4, 2).unwrap();
        run.log("eval/loss", 0.5, 2).unwrap();
    }

    #[test]
    fn test_large_step_gap_accepted() {
        let mut run = new_run();
        run.log("train/loss", 0.5, 1).unwrap();
        run.log("train/loss", 0.4, 1000).unwrap(); // big jump is fine
    }
}
