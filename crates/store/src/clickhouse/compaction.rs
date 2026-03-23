use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;

use super::ClickHouseStore;
use super::rows::CompactionCursorRow;
use crate::ports::compaction::CompactionCursor;
use crate::ports::{ReadError, WriteError};

impl CompactionCursor for ClickHouseStore {
    async fn get(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
    ) -> Result<Option<u64>, ReadError> {
        let run_uuid: uuid::Uuid = (*run_id).into();

        let rows: Vec<CompactionCursorRow> = self
            .client
            .query(
                "SELECT ?fields FROM compaction_cursors FINAL \
                 WHERE run_id = ? AND key = ? AND tier = ?",
            )
            .bind(run_uuid)
            .bind(key.as_str())
            .bind(tier as u32)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows.first().map(|r| r.offset))
    }

    async fn advance(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
        offset: u64,
    ) -> Result<(), WriteError> {
        let mut insert = self
            .client
            .insert("compaction_cursors")
            .map_err(|e| WriteError::Unknown(e.into()))?;

        insert
            .write(&CompactionCursorRow {
                run_id: (*run_id).into(),
                key: key.as_str().to_owned(),
                tier: tier as u32,
                offset,
            })
            .await
            .map_err(|e| WriteError::Unknown(e.into()))?;

        insert
            .end()
            .await
            .map_err(|e| WriteError::Unknown(e.into()))?;

        Ok(())
    }
}
