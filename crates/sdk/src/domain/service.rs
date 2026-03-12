use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::oneshot;

use photon_core::types::id::RunId;

use crate::domain::pipeline::accumulator::Accumulator;
use crate::domain::pipeline::batch_builder::{BatchBuilderError, BuilderStats};
use crate::domain::pipeline::sender::SenderStats;
use crate::domain::pipeline::interner::{MetricKeyInterner, RawPoint};
use crate::domain::ports::error::{FinishError, LogError, SenderThreadError};
use crate::domain::ports::wal::WalStorage;

#[derive(Clone, Debug)]
pub struct PipelineStats {
    pub points_logged: u64,
    pub points_dropped: u64,
    pub batches_flushed: u64,
    pub bytes_compressed: u64,
    pub bytes_uncompressed: u64,
    pub batches_sent: u64,
    pub batches_acked: u64,
}

pub(crate) struct SenderHandle {
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub handle: std::thread::JoinHandle<Result<SenderStats, SenderThreadError>>,
}

pub trait SdkService {
    fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), LogError>;
    fn finish(self) -> Result<PipelineStats, FinishError>;
    fn run_id(&self) -> RunId;
    fn points_logged(&self) -> u64;
    fn points_dropped(&self) -> u64;
}

pub struct Service<W: WalStorage> {
    run_id: RunId,
    accumulator: Accumulator<RawPoint>,
    interner: Arc<MetricKeyInterner>,
    builder_handle: JoinHandle<Result<BuilderStats, BatchBuilderError>>,
    sender_handle: Option<SenderHandle>,
    wal: W,
    points_logged: u64,
}

impl<W: WalStorage> Service<W> {
    pub(crate) fn new(
        run_id: RunId,
        accumulator: Accumulator<RawPoint>,
        interner: Arc<MetricKeyInterner>,
        builder_handle: JoinHandle<Result<BuilderStats, BatchBuilderError>>,
        sender_handle: Option<SenderHandle>,
        wal: W,
    ) -> Self {
        Self {
            run_id,
            accumulator,
            interner,
            builder_handle,
            sender_handle,
            wal,
            points_logged: 0,
        }
    }
}

impl<W: WalStorage> SdkService for Service<W> {
    fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), LogError> {
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

    fn finish(mut self) -> Result<PipelineStats, FinishError> {
        let points_logged = self.points_logged;
        let points_dropped = self.accumulator.points_dropped();

        // Replaces accumulator with a dummy so the real one drops,
        // which closes the channel and lets the builder drain.
        let _old = std::mem::replace(&mut self.accumulator, {
            let (acc, _rx) = Accumulator::new(1, 1);
            acc
        });
        drop(_old);

        let builder_stats = self.builder_handle
            .join()
            .map_err(|_| FinishError::Panicked)?
            .map_err(FinishError::Builder)?;

        // Drop the keep-alive so the sender exits once WAL is empty, then join.
        let (batches_sent, batches_acked) = match self.sender_handle.take() {
            Some(mut ctx) => {
                drop(ctx.shutdown_tx.take());

                let sender_stats = ctx
                    .handle
                    .join()
                    .map_err(|_| FinishError::Panicked)?
                    .map_err(FinishError::Sender)?;

                (sender_stats.batches_sent, sender_stats.batches_acked)
            }
            None => (0, 0),
        };

        // Clean shutdown: all batches were acked, so the WAL can be removed.
        if batches_acked == builder_stats.batches_flushed {
            let _ = self.wal.delete_all();
        }

        Ok(PipelineStats {
            points_logged,
            points_dropped,
            batches_flushed: builder_stats.batches_flushed,
            bytes_compressed: builder_stats.bytes_compressed,
            bytes_uncompressed: builder_stats.bytes_uncompressed,
            batches_sent,
            batches_acked,
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
