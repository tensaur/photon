use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::types::id::RunId;

use crate::ports::finalised::FinalisedStore;
use crate::ports::{ReadError, WriteError};

#[derive(Row, Serialize, Deserialize)]
struct FinalisedRow {
    #[serde(with = "clickhouse::serde::uuid")]
    run_id: uuid::Uuid,
    finalised_at: i64,
}

#[derive(Clone)]
pub struct ClickHouseFinalisedStore {
    client: clickhouse::Client,
}

impl ClickHouseFinalisedStore {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }
}

impl FinalisedStore for ClickHouseFinalisedStore {
    async fn mark_finalised(&self, run_id: &RunId) -> Result<(), WriteError> {
        let run_uuid: uuid::Uuid = (*run_id).into();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut insert = self
            .client
            .insert("finalised")
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        insert
            .write(&FinalisedRow {
                run_id: run_uuid,
                finalised_at: now,
            })
            .await
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        insert
            .end()
            .await
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        Ok(())
    }

    async fn is_finalised(&self, run_id: &RunId) -> Result<bool, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let count: u64 = self
            .client
            .query("SELECT count() FROM finalised FINAL WHERE run_id = ?")
            .bind(run_uuid)
            .fetch_one()
            .await
            .map_err(|e| ReadError::Store(Box::new(e)))?;

        Ok(count > 0)
    }
}
