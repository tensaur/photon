use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use super::{BackgroundWriter, WriteOp};
use crate::ports::watermark::WatermarkStore;
use crate::ports::{ReadError, WriteError};

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub(crate) struct WatermarkRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub run_id: uuid::Uuid,
    pub sequence: u64,
}

#[derive(Clone)]
pub struct ClickHouseWatermarkStore {
    client: clickhouse::Client,
    writer: BackgroundWriter,
}

impl ClickHouseWatermarkStore {
    pub fn new(client: clickhouse::Client, writer: BackgroundWriter) -> Self {
        Self { client, writer }
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
        self.writer
            .write_tx
            .send(WriteOp::Watermark(WatermarkRow {
                run_id: (*run_id).into(),
                sequence: seq.into(),
            }))
            .map_err(|_| WriteError::Unknown(anyhow::anyhow!("background writer stopped")))?;

        Ok(())
    }
}
