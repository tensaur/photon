use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::metric::MetricWriter;
use photon_wal::server::ServerWalReader;

use crate::domain::service::{FlushService, FlushStats};

pub struct FlushConfig {
    /// Maximum time to wait for new entries before flushing what we have.
    pub poll_interval: Duration,
    /// Maximum number of batches per ClickHouse INSERT.
    pub max_batch_read: usize,
    /// Number of concurrent ClickHouse INSERTs per flush cycle.
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
    mut reader: ServerWalReader,
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
    let mut total_points: u64 = 0;
    let mut total_batches: u64 = 0;
    let start = Instant::now();

    let read_limit = config.max_batch_read * config.concurrency;

    loop {
        let batches = match reader.read_available(read_limit) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("WAL read failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        if !batches.is_empty() {
            match flush_concurrent(&service, &batches, &mut reader, &config).await {
                Ok(stats) => {
                    total_points += stats.points as u64;
                    total_batches += stats.batches as u64;
                    log_stats(&stats, total_points, start);
                }
                Err(e) => {
                    tracing::error!("flush failed, retrying: {e}");
                    loop {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        if cancel.is_cancelled() {
                            tracing::error!("flush failed during shutdown, data in WAL for recovery");
                            log_totals(total_points, total_batches, start);
                            return;
                        }
                        match flush_concurrent(&service, &batches, &mut reader, &config).await {
                            Ok(stats) => {
                                total_points += stats.points as u64;
                                total_batches += stats.batches as u64;
                                log_stats(&stats, total_points, start);
                                break;
                            }
                            Err(e) => {
                                tracing::error!("flush retry failed: {e}");
                            }
                        }
                    }
                }
            }
            continue;
        }

        // Caught up
        if cancel.is_cancelled() {
            tracing::info!("flush consumer shut down (caught up)");
            log_totals(total_points, total_batches, start);
            return;
        }

        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(config.poll_interval) => {}
            _ = cancel.cancelled() => {
                drain(&mut reader, &service, &config, &mut total_points, &mut total_batches, start).await;
                tracing::info!("flush consumer shut down (drained)");
                log_totals(total_points, total_batches, start);
                return;
            }
        }
    }
}

async fn flush_concurrent<C, K, M>(
    service: &FlushService<C, K, M>,
    batches: &[photon_core::types::batch::WireBatch],
    reader: &mut ServerWalReader,
    config: &FlushConfig,
) -> Result<FlushStats, crate::domain::service::FlushError>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
{
    let chunk_size = config.max_batch_read.max(1);
    let futures: Vec<_> = batches.chunks(chunk_size).map(|c| service.process(c)).collect();
    let results = futures_util::future::try_join_all(futures).await?;

    let mut merged = FlushStats {
        batches: 0,
        points: 0,
        decode_ms: 0,
        write_ms: 0,
        watermarks: HashMap::new(),
    };
    for stats in results {
        merged.batches += stats.batches;
        merged.points += stats.points;
        merged.decode_ms = merged.decode_ms.max(stats.decode_ms);
        merged.write_ms = merged.write_ms.max(stats.write_ms);
        merge_watermarks(&mut merged.watermarks, &stats.watermarks);
    }

    if let Err(e) = reader.commit(&merged.watermarks) {
        tracing::error!("WAL commit failed: {e}");
    }
    Ok(merged)
}

fn merge_watermarks(
    target: &mut HashMap<RunId, SequenceNumber>,
    source: &HashMap<RunId, SequenceNumber>,
) {
    for (run_id, seq) in source {
        target
            .entry(*run_id)
            .and_modify(|existing| {
                if *seq > *existing {
                    *existing = *seq;
                }
            })
            .or_insert(*seq);
    }
}

fn log_stats(stats: &FlushStats, total_points: u64, start: Instant) {
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
        total_points,
        throughput_mpts = format!("{throughput:.2}"),
        "flushed"
    );
}

fn log_totals(total_points: u64, total_batches: u64, start: Instant) {
    let elapsed = start.elapsed();
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        total_points as f64 / elapsed.as_secs_f64() / 1_000_000.0
    } else {
        0.0
    };

    tracing::info!(
        total_points,
        total_batches,
        elapsed_ms = elapsed.as_millis() as u64,
        throughput_mpts = format!("{throughput:.2}"),
        "flush consumer totals"
    );
}

async fn drain<C, K, M>(
    reader: &mut ServerWalReader,
    service: &FlushService<C, K, M>,
    config: &FlushConfig,
    total_points: &mut u64,
    total_batches: &mut u64,
    start: Instant,
) where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
{
    let read_limit = config.max_batch_read * config.concurrency;

    loop {
        let batches = match reader.read_available(read_limit) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("WAL read failed during drain: {e}");
                return;
            }
        };

        if batches.is_empty() {
            return;
        }

        match flush_concurrent(service, &batches, reader, config).await {
            Ok(stats) => {
                *total_points += stats.points as u64;
                *total_batches += stats.batches as u64;
                log_stats(&stats, *total_points, start);
            }
            Err(e) => {
                tracing::error!("drain flush failed, data in WAL for recovery: {e}");
                return;
            }
        }
    }
}
