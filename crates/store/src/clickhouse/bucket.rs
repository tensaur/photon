use std::ops::Range;

use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::types::bucket::{Bucket, BucketEntry};
use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;

use super::ClickHouseStore;
use super::rows::BucketRow;
use crate::ports::bucket::{BucketReader, BucketWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Row, Serialize, Deserialize)]
struct BucketReadRow {
    step_start: u64,
    step_end: u64,
    value: f64,
    min: f64,
    max: f64,
}

impl BucketWriter for ClickHouseStore {
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
            .map_err(|e| WriteError::Unknown(e.into()))?;

        let run_uuid: uuid::Uuid = (*run_id).into();
        for entry in entries {
            insert
                .write(&BucketRow {
                    run_id: run_uuid,
                    key: entry.key.as_str().to_owned(),
                    tier: entry.tier as u32,
                    step_start: entry.bucket.step_start,
                    step_end: entry.bucket.step_end,
                    value: entry.bucket.value,
                    min: entry.bucket.min,
                    max: entry.bucket.max,
                })
                .await
                .map_err(|e| WriteError::Unknown(e.into()))?;
        }

        insert
            .end()
            .await
            .map_err(|e| WriteError::Unknown(e.into()))?;

        Ok(())
    }
}

impl BucketReader for ClickHouseStore {
    async fn read_buckets(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
        step_range: Range<u64>,
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
            .bind(step_range.end)
            .bind(step_range.start)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows
            .into_iter()
            .map(|r| Bucket {
                step_start: r.step_start,
                step_end: r.step_end,
                value: r.value,
                min: r.min,
                max: r.max,
            })
            .collect())
    }
}
