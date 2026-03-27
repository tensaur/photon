use std::ops::Range;

use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, Step};

use crate::ports::metric::{MetricReader, MetricWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MetricWriteRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub run_id: uuid::Uuid,
    pub key: String,
    pub step: u64,
    pub value: f64,
    pub timestamp_ms: u64,
}

#[derive(Row, Serialize, Deserialize)]
struct MetricReadRow {
    step: u64,
    value: f64,
}

#[derive(Row, Serialize, Deserialize)]
struct KeyRow {
    key: String,
}

#[derive(Row, Serialize, Deserialize)]
struct CountRow {
    count: u64,
}

#[derive(Clone)]
pub struct ClickHouseMetricStore {
    client: clickhouse::Client,
}

impl ClickHouseMetricStore {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }
}

impl MetricWriter for ClickHouseMetricStore {
    async fn write_batch(&self, batch: &MetricBatch) -> Result<(), WriteError> {
        self.write_batches(std::slice::from_ref(batch)).await
    }

    async fn write_batches(&self, batches: &[MetricBatch]) -> Result<(), WriteError> {
        if batches.iter().all(MetricBatch::is_empty) {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("metrics")
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        for batch in batches {
            let run_uuid: uuid::Uuid = batch.run_id.into();

            for point in &batch.points {
                insert
                    .write(&MetricWriteRow {
                        run_id: run_uuid,
                        key: batch.key(point).as_str().to_owned(),
                        step: point.step.as_u64(),
                        value: point.value,
                        timestamp_ms: point.timestamp_ms,
                    })
                    .await
                    .map_err(|e| WriteError::Store(Box::new(e)))?;
            }
        }

        insert
            .end()
            .await
            .map_err(|e| WriteError::Store(Box::new(e)))?;
        Ok(())
    }
}

impl MetricReader for ClickHouseMetricStore {
    async fn read_points(
        &self,
        run_id: &RunId,
        key: &Metric,
        step_range: Range<Step>,
    ) -> Result<Vec<(Step, f64)>, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let rows: Vec<MetricReadRow> = self
            .client
            .query(
                "SELECT ?fields FROM metrics \
                 WHERE run_id = ? AND key = ? AND step >= ? AND step < ? \
                 ORDER BY step",
            )
            .bind(run_uuid)
            .bind(key.as_str())
            .bind(step_range.start.as_u64())
            .bind(step_range.end.as_u64())
            .fetch_all()
            .await
            .map_err(|e| ReadError::Store(Box::new(e)))?;

        Ok(rows
            .into_iter()
            .map(|r| (Step::new(r.step), r.value))
            .collect())
    }

    async fn list_metrics(&self, run_id: &RunId) -> Result<Vec<Metric>, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let rows: Vec<KeyRow> = self
            .client
            .query("SELECT DISTINCT key FROM metrics WHERE run_id = ?")
            .bind(run_uuid)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Store(Box::new(e)))?;

        Ok(rows
            .into_iter()
            .map(|r| Metric::new_unchecked(r.key))
            .collect())
    }

    async fn count_points(
        &self,
        run_id: &RunId,
        key: &Metric,
        step_range: Range<Step>,
    ) -> Result<usize, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let rows: Vec<CountRow> = self
            .client
            .query(
                "SELECT count() as count FROM metrics \
                 WHERE run_id = ? AND key = ? AND step >= ? AND step < ?",
            )
            .bind(run_uuid)
            .bind(key.as_str())
            .bind(step_range.start.as_u64())
            .bind(step_range.end.as_u64())
            .fetch_all()
            .await
            .map_err(|e| ReadError::Store(Box::new(e)))?;

        Ok(rows.first().map_or(0, |r| r.count as usize))
    }
}
