pub mod bucket;
pub mod compaction;
pub mod metric;
pub mod watermark;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::MissedTickBehavior;

use self::metric::MetricRow;
use self::watermark::WatermarkRow;

const MAX_CONCURRENT_FLUSHES: usize = 8;
const FLUSH_THRESHOLD: usize = 100_000;
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) enum WriteOp {
    Metrics(Vec<MetricRow>),
    Watermark(WatermarkRow),
    Flush(oneshot::Sender<()>),
}

/// Shared background writer for batching inserts into ClickHouse.
#[derive(Clone)]
pub struct BackgroundWriter {
    pub(crate) write_tx: mpsc::UnboundedSender<WriteOp>,
}

impl BackgroundWriter {
    pub fn new(client: clickhouse::Client) -> Self {
        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let flush_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_FLUSHES));
        tokio::spawn(background_writer(client, write_rx, flush_semaphore));
        Self { write_tx }
    }

    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.write_tx.send(WriteOp::Flush(tx));
        let _ = rx.await;
    }
}

pub fn client_from_env() -> clickhouse::Client {
    let url = std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into());
    let db = std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "photon".into());
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    clickhouse::Client::default()
        .with_url(&url)
        .with_database(&db)
        .with_user(&user)
        .with_password(&password)
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0")
}

async fn background_writer(
    client: clickhouse::Client,
    mut rx: mpsc::UnboundedReceiver<WriteOp>,
    semaphore: Arc<Semaphore>,
) {
    let mut metric_buf: Vec<MetricRow> = Vec::new();
    let mut watermark_buf: Vec<WatermarkRow> = Vec::new();
    let mut interval = tokio::time::interval(FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;

            msg = rx.recv() => {
                match msg {
                    Some(WriteOp::Metrics(rows)) => {
                        metric_buf.extend(rows);
                        if metric_buf.len() >= FLUSH_THRESHOLD {
                            spawn_flush(&client, &semaphore, &mut metric_buf, &mut watermark_buf).await;
                        }
                    }
                    Some(WriteOp::Watermark(row)) => {
                        watermark_buf.push(row);
                    }
                    Some(WriteOp::Flush(done)) => {
                        spawn_flush(&client, &semaphore, &mut metric_buf, &mut watermark_buf).await;
                        wait_all(&semaphore).await;
                        let _ = done.send(());
                    }
                    None => {
                        spawn_flush(&client, &semaphore, &mut metric_buf, &mut watermark_buf).await;
                        wait_all(&semaphore).await;
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                spawn_flush(&client, &semaphore, &mut metric_buf, &mut watermark_buf).await;
            }
        }
    }
}

async fn spawn_flush(
    client: &clickhouse::Client,
    semaphore: &Arc<Semaphore>,
    metric_buf: &mut Vec<MetricRow>,
    watermark_buf: &mut Vec<WatermarkRow>,
) {
    if metric_buf.is_empty() && watermark_buf.is_empty() {
        return;
    }

    let permit = semaphore.clone().acquire_owned().await;
    let metrics = std::mem::take(metric_buf);
    let watermarks = std::mem::take(watermark_buf);
    let client = client.clone();

    tokio::spawn(async move {
        if !metrics.is_empty() {
            if let Err(e) = flush_rows(&client, "metrics", &metrics).await {
                tracing::error!("failed to flush metrics: {e}");
            }
        }
        if !watermarks.is_empty() {
            if let Err(e) = flush_rows(&client, "watermarks", &watermarks).await {
                tracing::error!("failed to flush watermarks: {e}");
            }
        }
        drop(permit);
    });
}

async fn wait_all(semaphore: &Semaphore) {
    let _ = semaphore
        .acquire_many(MAX_CONCURRENT_FLUSHES as u32)
        .await;
}

async fn flush_rows<T: clickhouse::Row + serde::Serialize>(
    client: &clickhouse::Client,
    table: &str,
    rows: &[T],
) -> Result<(), clickhouse::error::Error> {
    let mut insert = client.insert(table)?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}
