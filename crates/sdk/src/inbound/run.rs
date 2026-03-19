use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender, select, tick};
use tokio::sync::oneshot;

use photon_core::types::batch::WireBatch;
use photon_core::types::config::{BatchConfig, SenderConfig};
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_transport::TransportChoice;

use crate::domain::flush::{FlushError, FlushService};
use crate::domain::interner::MetricKeyInterner;
use crate::domain::ports::error::{FinishError, LogError, SenderThreadError, TransportError};
use crate::domain::ports::wal::{WalAppender, WalManager};
use crate::domain::send::{SenderService, SenderStats};
use crate::domain::types::{FlushStats, RawPoint};
use crate::inbound::accumulator::Accumulator;
use crate::inbound::error::SdkError;
use crate::outbound::wal::WalManagerChoice;

// ---------------------------------------------------------------------------
// RunStats
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// SenderHandle (orchestration bookkeeping)
// ---------------------------------------------------------------------------

pub(crate) struct SenderHandle {
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub handle: JoinHandle<Result<SenderStats, SenderThreadError>>,
}

// ---------------------------------------------------------------------------
// Run — concrete, no generics in public API
// ---------------------------------------------------------------------------

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
    pub fn log(&mut self, key: &str, value: f64, step: u64) -> Result<(), SdkError> {
        let metric_key = self.interner.get_or_intern(key).map_err(LogError::from)?;

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

// ---------------------------------------------------------------------------
// Orchestration: flush thread
// ---------------------------------------------------------------------------

pub(crate) fn run_flush_thread<K, C, A>(
    mut service: FlushService<K, C, A>,
    rx: Receiver<RawPoint>,
    batch_tx: Sender<WireBatch>,
    config: BatchConfig,
) -> Result<FlushStats, FlushError>
where
    K: Codec<MetricBatch>,
    C: Compressor,
    A: WalAppender,
{
    let ticker = tick(config.flush_interval);
    let mut pending: Vec<RawPoint> = Vec::with_capacity(config.max_points);
    let mut stats = FlushStats::default();

    loop {
        select! {
            recv(rx) -> msg => {
                match msg {
                    Ok(point) => {
                        pending.push(point);

                        while pending.len() < config.max_points {
                            match rx.try_recv() {
                                Ok(p) => pending.push(p),
                                Err(_) => break,
                            }
                        }

                        if pending.len() >= config.max_points {
                            let wire = service.flush(&pending, &mut stats)?;
                            let _ = batch_tx.send(wire);
                            pending.clear();
                        }
                    }
                    Err(_) => {
                        if !pending.is_empty() {
                            let wire = service.flush(&pending, &mut stats)?;
                            let _ = batch_tx.send(wire);
                            pending.clear();
                        }

                        return Ok(stats);
                    }
                }
            }

            recv(ticker) -> _ => {
                if !pending.is_empty() {
                    let wire = service.flush(&pending, &mut stats)?;
                    let _ = batch_tx.send(wire);
                    pending.clear();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Orchestration: sender thread
// ---------------------------------------------------------------------------

pub(crate) fn run_sender_thread(
    transport: TransportChoice,
    endpoint: String,
    run_id: RunId,
    wal: WalManagerChoice,
    config: SenderConfig,
    mut shutdown_rx: oneshot::Receiver<()>,
    batch_rx: crossbeam_channel::Receiver<WireBatch>,
) -> Result<SenderStats, SenderThreadError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(SenderThreadError::Runtime)?;

    rt.block_on(async move {
        transport
            .connect(&endpoint)
            .await
            .map_err(TransportError::from)?;

        let tick_interval = config.idle_poll_interval;
        let mut sender = SenderService::new(transport, wal, config, run_id);

        sender.recover().await?;

        let mut shutdown_signaled = false;

        loop {
            let batch = batch_rx.try_recv().ok();

            if !shutdown_signaled {
                shutdown_signaled = !matches!(
                    shutdown_rx.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                );
            }

            let had_work = batch.is_some() || shutdown_signaled;

            if sender.step(batch, shutdown_signaled).await? {
                return Ok(sender.stats());
            }

            // Only sleep when genuinely idle — no batches flowing and not
            // draining shutdown. The old Sender::run() only slept on the
            // `!sent && !acked` path; this preserves that behavior.
            if !had_work {
                tokio::time::sleep(tick_interval).await;
            }
        }
    })
}
