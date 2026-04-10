use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::types::id::RunId;

use crate::ports::finalized::FinalizedStore;
use crate::ports::{ReadError, WriteError};

#[derive(Row, Serialize, Deserialize)]
struct FinalizedRow {
    #[serde(with = "clickhouse::serde::uuid")]
    run_id: uuid::Uuid,
    finalized_at: i64,
}

#[derive(Clone)]
pub struct ClickHouseFinalizedStore {
    client: clickhouse::Client,
}

impl ClickHouseFinalizedStore {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }
}

impl FinalizedStore for ClickHouseFinalizedStore {
    async fn mark_finalized(&self, run_id: &RunId) -> Result<(), WriteError> {
        let run_uuid: uuid::Uuid = (*run_id).into();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut insert = self
            .client
            .insert("finalized")
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        insert
            .write(&FinalizedRow {
                run_id: run_uuid,
                finalized_at: now,
            })
            .await
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        insert
            .end()
            .await
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        Ok(())
    }

    async fn is_finalized(&self, run_id: &RunId) -> Result<bool, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let count: u64 = self
            .client
            .query("SELECT count() FROM finalized FINAL WHERE run_id = ?")
            .bind(run_uuid)
            .fetch_one()
            .await
            .map_err(|e| ReadError::Store(Box::new(e)))?;

        Ok(count > 0)
    }
}
