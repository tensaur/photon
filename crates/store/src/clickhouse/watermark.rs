use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::ports::watermark::WatermarkWriter;
use crate::ports::{ReadError, WriteError};

#[derive(Row, Serialize, Deserialize)]
struct WatermarkRow {
    #[serde(with = "clickhouse::serde::uuid")]
    run_id: uuid::Uuid,
    sequence: u64,
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

impl WatermarkWriter for ClickHouseWatermarkStore {
    async fn write_watermarks(
        &self,
        entries: &[(RunId, SequenceNumber)],
    ) -> Result<(), WriteError> {
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

    async fn read_all(&self) -> Result<Vec<(RunId, SequenceNumber)>, ReadError> {
        let rows: Vec<WatermarkRow> = self
            .client
            .query("SELECT ?fields FROM watermarks FINAL")
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows
            .into_iter()
            .map(|r| (RunId::from(r.run_id), SequenceNumber::from(r.sequence)))
            .collect())
    }
}
