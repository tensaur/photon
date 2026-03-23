pub mod bucket;
pub mod compaction;
pub mod metric;
pub mod watermark;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::MissedTickBehavior;

use self::metric::MetricWriteRow;
use self::watermark::WatermarkRow;

const MAX_CONCURRENT_FLUSHES: usize = 8;
const FLUSH_THRESHOLD: usize = 100_000;
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) enum WriteOp {
    Metrics(Vec<MetricWriteRow>),
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

pub struct ClientBuilder {
    url: String,
    database: String,
    user: String,
    password: String,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            url: "http://localhost:8123".into(),
            database: "photon".into(),
            user: "default".into(),
            password: String::new(),
        }
    }

    pub fn with_env(mut self) -> Self {
        if let Ok(v) = std::env::var("CLICKHOUSE_URL") {
            self.url = v;
        }
        if let Ok(v) = std::env::var("CLICKHOUSE_DATABASE") {
            self.database = v;
        }
        if let Ok(v) = std::env::var("CLICKHOUSE_USER") {
            self.user = v;
        }
        if let Ok(v) = std::env::var("CLICKHOUSE_PASSWORD") {
            self.password = v;
        }
        self
    }

    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }

    pub fn build(self) -> clickhouse::Client {
        clickhouse::Client::default()
            .with_url(&self.url)
            .with_database(&self.database)
            .with_user(&self.user)
            .with_password(&self.password)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0")
    }
}

pub async fn migrate(client: &clickhouse::Client) -> Result<(), clickhouse::error::Error> {
    client
        .query("CREATE DATABASE IF NOT EXISTS photon")
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS photon.metrics (
                run_id UUID,
                key String,
                step UInt64,
                value Float64,
                timestamp_ms UInt64
            ) ENGINE = MergeTree()
            ORDER BY (run_id, key, step)",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS photon.watermarks (
                run_id UUID,
                sequence UInt64
            ) ENGINE = ReplacingMergeTree(sequence)
            ORDER BY (run_id)",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS photon.buckets (
                run_id UUID,
                key String,
                tier UInt32,
                step_start UInt64,
                step_end UInt64,
                value Float64,
                min Float64,
                max Float64
            ) ENGINE = MergeTree()
            ORDER BY (run_id, key, tier, step_start)",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS photon.compaction_cursors (
                run_id UUID,
                key String,
                tier UInt32,
                offset UInt64
            ) ENGINE = ReplacingMergeTree(offset)
            ORDER BY (run_id, key, tier)",
        )
        .execute()
        .await?;

    Ok(())
}

async fn background_writer(
    client: clickhouse::Client,
    mut rx: mpsc::UnboundedReceiver<WriteOp>,
    semaphore: Arc<Semaphore>,
) {
    let mut metric_buf: Vec<MetricWriteRow> = Vec::new();
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
    metric_buf: &mut Vec<MetricWriteRow>,
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
