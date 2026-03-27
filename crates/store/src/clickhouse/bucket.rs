use std::ops::Range;

use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::types::bucket::{Bucket, BucketEntry};
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, Step};

use crate::ports::bucket::{BucketReader, BucketWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Row, Serialize, Deserialize)]
struct BucketWriteRow {
    #[serde(with = "clickhouse::serde::uuid")]
    run_id: uuid::Uuid,
    key: String,
    tier: u32,
    step_start: u64,
    step_end: u64,
    value: f64,
    min: f64,
    max: f64,
}

#[derive(Row, Serialize, Deserialize)]
struct BucketReadRow {
    step_start: u64,
    step_end: u64,
    value: f64,
    min: f64,
    max: f64,
}

#[derive(Clone)]
pub struct ClickHouseBucketStore {
    client: clickhouse::Client,
}

impl ClickHouseBucketStore {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }
}

impl BucketWriter for ClickHouseBucketStore {
    async fn write_buckets(
        &self,
        run_id: &RunId,
        entries: &[BucketEntry],
    ) -> Result<(), WriteError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("buckets")
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        let run_uuid: uuid::Uuid = (*run_id).into();
        for entry in entries {
            insert
                .write(&BucketWriteRow {
                    run_id: run_uuid,
                    key: entry.key.as_str().to_owned(),
                    tier: entry.tier as u32,
                    step_start: entry.bucket.step_start.as_u64(),
                    step_end: entry.bucket.step_end.as_u64(),
                    value: entry.bucket.value,
                    min: entry.bucket.min,
                    max: entry.bucket.max,
                })
                .await
                .map_err(|e| WriteError::Store(Box::new(e)))?;
        }

        insert
            .end()
            .await
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        Ok(())
    }
}

impl BucketReader for ClickHouseBucketStore {
    async fn read_buckets(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
        step_range: Range<Step>,
    ) -> Result<Vec<Bucket>, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let rows: Vec<BucketReadRow> = self
            .client
            .query(
                "SELECT ?fields FROM buckets \
                 WHERE run_id = ? AND key = ? AND tier = ? \
                 AND step_start < ? AND step_end >= ? \
                 ORDER BY step_start",
            )
            .bind(run_uuid)
            .bind(key.as_str())
            .bind(tier as u32)
            .bind(step_range.end.as_u64())
            .bind(step_range.start.as_u64())
            .fetch_all()
            .await
            .map_err(|e| ReadError::Store(Box::new(e)))?;

        Ok(rows
            .into_iter()
            .map(|r| Bucket {
                step_start: Step::new(r.step_start),
                step_end: Step::new(r.step_end),
                value: r.value,
                min: r.min,
                max: r.max,
            })
            .collect())
    }
}
