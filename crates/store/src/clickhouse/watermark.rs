use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use super::rows::WatermarkRow;
use super::{ClickHouseStore, WriteOp};
use crate::ports::watermark::WatermarkStore;
use crate::ports::{ReadError, WriteError};

impl WatermarkStore for ClickHouseStore {
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
        self.write_tx
            .send(WriteOp::Watermark(WatermarkRow {
                run_id: (*run_id).into(),
                sequence: seq.into(),
            }))
            .map_err(|_| WriteError::Unknown(anyhow::anyhow!("background writer stopped")))?;

        Ok(())
    }
}
