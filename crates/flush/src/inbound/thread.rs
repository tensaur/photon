use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::WalOffset;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::metric::MetricWriter;
use photon_wal::Wal;

use crate::domain::service::{FlushService, FlushStats};

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

pub async fn run<C, K, M>(
    mut wal: Box<dyn Wal>,
    notify: Arc<tokio::sync::Notify>,
    service: FlushService<C, K, M>,
    config: FlushConfig,
    cancel: CancellationToken,
) where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
{
    tracing::info!("flush consumer started");
    let mut cursor = wal
        .read_meta()
        .map(|m| m.consumed)
        .unwrap_or(WalOffset::ZERO);

    let read_limit = config.max_batch_read * config.concurrency;
    let start = Instant::now();
    let mut total_points: u64 = 0;
    let mut total_batches: u64 = 0;

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
            let chunk_size = config.max_batch_read.max(1);
            let futures: Vec<_> = batches.chunks(chunk_size).map(|c| service.process(c)).collect();

            match futures_util::future::try_join_all(futures).await {
                Ok(results) => {
                    let stats = merge_stats(results);
                    total_points += stats.points as u64;
                    total_batches += stats.batches as u64;
                    log_flush(&stats, total_points, start);

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

        // Caught up — wait or exit
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
        total_points,
        total_batches,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "flush consumer shut down"
    );
}

fn merge_stats(results: Vec<FlushStats>) -> FlushStats {
    let mut merged = FlushStats {
        batches: 0,
        points: 0,
        decode_ms: 0,
        write_ms: 0,
    };
    for s in results {
        merged.batches += s.batches;
        merged.points += s.points;
        merged.decode_ms = merged.decode_ms.max(s.decode_ms);
        merged.write_ms = merged.write_ms.max(s.write_ms);
    }
    merged
}

fn log_flush(stats: &FlushStats, total_points: u64, start: Instant) {
    let elapsed = start.elapsed().as_secs_f64();
    let throughput = if elapsed > 0.0 {
        total_points as f64 / elapsed / 1_000_000.0
    } else {
        0.0
    };
    tracing::info!(
        batches = stats.batches,
        points = stats.points,
        decode_ms = stats.decode_ms,
        write_ms = stats.write_ms,
        throughput_mpts = format!("{throughput:.2}"),
        "flushed"
    );
}
