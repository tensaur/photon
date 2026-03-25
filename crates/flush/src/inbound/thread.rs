use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use photon_core::types::metric::MetricBatch;
use photon_core::types::wal::WalOffset;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::metric::MetricWriter;
use photon_wal::Wal;

use crate::domain::service::FlushService;

pub struct FlushConfig {
    pub poll_interval: Duration,
    pub max_batch_read: usize,
    pub concurrency: usize,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            max_batch_read: 100,
            concurrency: 3,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct FlushStats {
    pub batches_flushed: u64,
    pub points_flushed: u64,
}

pub async fn run<C, K, M, W>(
    mut wal: W,
    notify: Arc<tokio::sync::Notify>,
    service: FlushService<C, K, M>,
    config: FlushConfig,
    cancel: CancellationToken,
) -> FlushStats
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: Wal,
{
    tracing::info!("flush consumer started");
    let mut cursor = wal
        .read_meta()
        .map(|m| m.consumed)
        .unwrap_or(WalOffset::ZERO);

    let read_limit = config.max_batch_read * config.concurrency;
    let start = Instant::now();
    let mut stats = FlushStats::default();

    loop {
        let mut batches = match wal.read_from(cursor) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("WAL read failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        batches.truncate(read_limit);

        if !batches.is_empty() {
            let count = batches.len() as u64;
            let points: u64 = batches.iter().map(|b| b.point_count as u64).sum();
            let chunk_size = config.max_batch_read.max(1);
            let futures: Vec<_> = batches.chunks(chunk_size).map(|c| service.write(c)).collect();

            match futures_util::future::try_join_all(futures).await {
                Ok(_) => {
                    stats.batches_flushed += count;
                    stats.points_flushed += points;
                    log_flush(&stats, start);

                    cursor = cursor.advance(count);
                    if let Err(e) = wal.truncate_through(cursor) {
                        tracing::error!("WAL truncate failed: {e}");
                    }
                    let _ = wal.sync();
                }
                Err(e) => {
                    tracing::error!("flush failed: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            continue;
        }

        // Caught up
        if cancel.is_cancelled() {
            break;
        }

        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(config.poll_interval) => {}
            _ = cancel.cancelled() => break,
        }
    }

    tracing::info!(
        batches = stats.batches_flushed,
        points = stats.points_flushed,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "flush consumer shut down"
    );

    stats
}

fn log_flush(stats: &FlushStats, start: Instant) {
    let elapsed = start.elapsed().as_secs_f64();
    let throughput = if elapsed > 0.0 {
        stats.points_flushed as f64 / elapsed / 1_000_000.0
    } else {
        0.0
    };
    tracing::info!(
        batches = stats.batches_flushed,
        points = stats.points_flushed,
        throughput_mpts = format!("{throughput:.2}"),
        "flushed"
    );
}
