use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use photon_core::types::metric::MetricBatch;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::metric::MetricWriter;
use photon_store::ports::watermark::WatermarkStore;
use photon_wal::server::ServerWalReader;

use crate::domain::service::FlushService;

pub struct FlushConfig {
    /// Maximum time to wait for new entries before flushing what we have.
    pub poll_interval: Duration,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
        }
    }
}

pub async fn run<C, K, M, W>(
    mut reader: ServerWalReader,
    notify: Arc<tokio::sync::Notify>,
    service: FlushService<C, K, M, W>,
    config: FlushConfig,
    cancel: CancellationToken,
) where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkStore,
{
    tracing::info!("flush consumer started");

    loop {
        // Read all available entries from WAL
        let batches = match reader.read_available() {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("WAL read failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        if !batches.is_empty() {
            let count = batches.len();
            let points: usize = batches.iter().map(|b| b.point_count).sum();

            // Process: decompress, decode, write to store
            loop {
                match service.process(&batches).await {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::error!("flush failed, retrying: {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;

                        if cancel.is_cancelled() {
                            tracing::error!("flush failed during shutdown, data may be lost");
                            return;
                        }
                    }
                }
            }

            // Commit: persist offset, GC consumed segments
            if let Err(e) = reader.commit() {
                tracing::error!("WAL commit failed: {e}");
            }

            tracing::info!(batches = count, points, "flushed to store");
            continue;
        }

        // Nothing available — wait for signal or timeout
        if cancel.is_cancelled() {
            tracing::info!("flush consumer shutting down");
            return;
        }

        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(config.poll_interval) => {}
            _ = cancel.cancelled() => {
                // Drain remaining
                if let Ok(remaining) = reader.read_available() {
                    if !remaining.is_empty() {
                        tracing::info!(batches = remaining.len(), "draining remaining WAL entries");
                        if let Err(e) = service.process(&remaining).await {
                            tracing::error!("drain flush failed: {e}");
                        } else {
                            let _ = reader.commit();
                        }
                    }
                }
                tracing::info!("flush consumer shut down");
                return;
            }
        }
    }
}
