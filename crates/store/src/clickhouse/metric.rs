use std::ops::Range;

use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch};

use super::rows::MetricRow;
use super::{BackgroundWriter, WriteOp};
use crate::ports::metric::{MetricReader, MetricWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Row, Serialize, Deserialize)]
struct StepValue {
    step: u64,
    value: f64,
}

#[derive(Row, Serialize, Deserialize)]
struct RunIdRow {
    #[serde(with = "clickhouse::serde::uuid")]
    run_id: uuid::Uuid,
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
    writer: BackgroundWriter,
}

impl ClickHouseMetricStore {
    pub fn new(client: clickhouse::Client, writer: BackgroundWriter) -> Self {
        Self { client, writer }
    }
}

impl MetricWriter for ClickHouseMetricStore {
    async fn write_batch(&self, batch: &MetricBatch) -> Result<(), WriteError> {
        if batch.points.is_empty() {
            return Ok(());
        }

        let run_uuid: uuid::Uuid = batch.run_id.into();
        let rows: Vec<MetricRow> = batch
            .points
            .iter()
            .map(|point| MetricRow {
                run_id: run_uuid,
                key: batch.key(point).as_str().to_owned(),
                step: point.step,
                value: point.value,
                timestamp_ms: point.timestamp_ms,
            })
            .collect();

        self.writer
            .write_tx
            .send(WriteOp::Metrics(rows))
            .map_err(|_| WriteError::Unknown(anyhow::anyhow!("background writer stopped")))?;

        Ok(())
    }
}

impl MetricReader for ClickHouseMetricStore {
    async fn list_runs(&self) -> Result<Vec<RunId>, ReadError> {
        let rows: Vec<RunIdRow> = self
            .client
            .query("SELECT DISTINCT run_id FROM metrics")
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows.into_iter().map(|r| RunId::from(r.run_id)).collect())
    }

    async fn read_points(
        &self,
        run_id: &RunId,
        key: &Metric,
        step_range: Range<u64>,
    ) -> Result<Vec<(u64, f64)>, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let rows: Vec<StepValue> = self
            .client
            .query(
                "SELECT ?fields FROM metrics \
                 WHERE run_id = ? AND key = ? AND step >= ? AND step < ? \
                 ORDER BY step",
            )
            .bind(run_uuid)
            .bind(key.as_str())
            .bind(step_range.start)
            .bind(step_range.end)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows.into_iter().map(|r| (r.step, r.value)).collect())
    }

    async fn list_metrics(&self, run_id: &RunId) -> Result<Vec<Metric>, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let rows: Vec<KeyRow> = self
            .client
            .query("SELECT DISTINCT key FROM metrics WHERE run_id = ?")
            .bind(run_uuid)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows
            .into_iter()
            .map(|r| Metric::new_unchecked(r.key))
            .collect())
    }

    async fn count_points(
        &self,
        run_id: &RunId,
        key: &Metric,
        step_range: Range<u64>,
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
            .bind(step_range.start)
            .bind(step_range.end)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows.first().map(|r| r.count as usize).unwrap_or(0))
    }
}
