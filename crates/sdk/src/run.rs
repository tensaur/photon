use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use photon_core::types::id::RunId;
use photon_flush::{FlushError, FlushStats, MetricKeyInterner, RawPoint};
use photon_send::{SenderStats, SenderThreadError};
use photon_wal::{WalManager, WalManagerChoice};
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

pub(crate) struct SenderHandle {
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub handle: JoinHandle<Result<SenderStats, SenderThreadError>>,
}

pub struct Run {
    run_id: RunId,
    accumulator: Accumulator<RawPoint>,
    interner: Arc<MetricKeyInterner>,
    flush_handle: JoinHandle<Result<FlushStats, FlushError>>,
    sender_handle: Option<SenderHandle>,
    wal: WalManagerChoice,
    points_logged: u64,
}

impl Run {
    pub(crate) fn new(
        run_id: RunId,
        accumulator: Accumulator<RawPoint>,
        interner: Arc<MetricKeyInterner>,
        flush_handle: JoinHandle<Result<FlushStats, FlushError>>,
        sender_handle: Option<SenderHandle>,
        wal: WalManagerChoice,
    ) -> Self {
        Self {
            run_id,
            accumulator,
            interner,
            flush_handle,
            sender_handle,
            wal,
            points_logged: 0,
        }
    }

    /// Log a single metric data point.
    pub fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), LogError> {
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
    pub fn finish(mut self) -> Result<RunStats, FinishError> {
        let points_logged = self.points_logged;
        let points_dropped = self.accumulator.points_dropped();

        // Close the channel by dropping the real accumulator.
        let _old = std::mem::replace(&mut self.accumulator, {
            let (acc, _rx) = Accumulator::new(1);
            acc
        });
        drop(_old);

        // Join flush thread.
        let flush_stats = self
            .flush_handle
            .join()
            .map_err(|_| FinishError::Panicked)?
            .map_err(FinishError::Flush)?;

        // Signal sender shutdown and join.
        let (batches_sent, batches_acked, batches_rejected) = match self.sender_handle {
            Some(mut ctx) => {
                drop(ctx.shutdown_tx.take());

                let sender_stats = ctx
                    .handle
                    .join()
                    .map_err(|_| FinishError::Panicked)?
                    .map_err(FinishError::Sender)?;

                (
                    sender_stats.batches_sent,
                    sender_stats.batches_acked,
                    sender_stats.rejections_received,
                )
            }
            None => (0, 0, 0),
        };

        // Clean shutdown: all batches acked → delete WAL.
        if batches_acked == flush_stats.batches_flushed {
            let _ = self.wal.delete_all();
        }

        Ok(RunStats {
            points: points_logged,
            points_dropped,
            batches: flush_stats.batches_flushed,
            bytes_compressed: flush_stats.bytes_compressed,
            bytes_uncompressed: flush_stats.bytes_uncompressed,
            batches_sent,
            batches_acked,
            batches_rejected,
        })
    }
}
