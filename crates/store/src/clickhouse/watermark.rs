use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::ports::watermark::WatermarkStore;
use crate::ports::{ReadError, WriteError};

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct WatermarkRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub run_id: uuid::Uuid,
    pub sequence: u64,
}

#[derive(Clone)]
pub struct ClickHouseWatermarkStore {
    client: clickhouse::Client,
}

impl ClickHouseWatermarkStore {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }
}

impl WatermarkStore for ClickHouseWatermarkStore {
    async fn get(&self, run_id: &RunId) -> Result<Option<SequenceNumber>, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let rows: Vec<WatermarkRow> = self
            .client
            .query("SELECT ?fields FROM watermarks FINAL WHERE run_id = ?")
            .bind(run_uuid)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows.first().map(|r| SequenceNumber::from(r.sequence)))
    }

    async fn advance(&self, run_id: &RunId, seq: SequenceNumber) -> Result<(), WriteError> {
        self.advance_many(&[(*run_id, seq)]).await
    }

    async fn advance_many(&self, entries: &[(RunId, SequenceNumber)]) -> Result<(), WriteError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut insert = self
            .client
            .insert("watermarks")
            .map_err(|e| WriteError::Unknown(e.into()))?;

        for (run_id, seq) in entries {
            insert
                .write(&WatermarkRow {
                    run_id: (*run_id).into(),
                    sequence: (*seq).into(),
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
