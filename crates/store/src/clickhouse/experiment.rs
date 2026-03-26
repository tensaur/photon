use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::domain::experiment::Experiment;
use photon_core::types::id::{ExperimentId, ProjectId};

use crate::ports::experiment::{ExperimentReader, ExperimentWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Row, Serialize, Deserialize)]
struct ExperimentRow {
    #[serde(with = "clickhouse::serde::uuid")]
    id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    project_id: uuid::Uuid,
    name: String,
    tags: Vec<String>,
    created_at: i64,
    updated_at: i64,
}

impl From<&Experiment> for ExperimentRow {
    fn from(e: &Experiment) -> Self {
        Self {
            id: e.id.into(),
            project_id: e.project_id.into(),
            name: e.name.clone(),
            tags: e.tags.clone(),
            created_at: e.created_at.timestamp_millis(),
            updated_at: e.updated_at.timestamp_millis(),
        }
    }
}

impl From<ExperimentRow> for Experiment {
    fn from(r: ExperimentRow) -> Self {
        Self {
            id: ExperimentId::from(r.id),
            project_id: ProjectId::from(r.project_id),
            name: r.name,
            tags: r.tags,
            created_at: chrono::DateTime::from_timestamp_millis(r.created_at)
                .unwrap_or_default(),
            updated_at: chrono::DateTime::from_timestamp_millis(r.updated_at)
                .unwrap_or_default(),
        }
    }
}

#[derive(Clone)]
pub struct ClickHouseExperimentStore {
    client: clickhouse::Client,
}

impl ClickHouseExperimentStore {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }
}

impl ExperimentWriter for ClickHouseExperimentStore {
    async fn upsert_experiment(&self, experiment: &Experiment) -> Result<(), WriteError> {
        let mut insert = self
            .client
            .insert("experiments")
            .map_err(|e| WriteError::Unknown(e.into()))?;

        insert
            .write(&ExperimentRow::from(experiment))
            .await
            .map_err(|e| WriteError::Unknown(e.into()))?;

        insert
            .end()
            .await
            .map_err(|e| WriteError::Unknown(e.into()))?;
        Ok(())
    }
}

impl ExperimentReader for ClickHouseExperimentStore {
    async fn list_experiments(&self) -> Result<Vec<Experiment>, ReadError> {
        let rows: Vec<ExperimentRow> = self
            .client
            .query("SELECT ?fields FROM experiments FINAL")
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows.into_iter().map(Experiment::from).collect())
    }

    async fn get_experiment(&self, id: &ExperimentId) -> Result<Option<Experiment>, ReadError> {
        let uuid: uuid::Uuid = (*id).into();

        let rows: Vec<ExperimentRow> = self
            .client
            .query("SELECT ?fields FROM experiments FINAL WHERE id = ?")
            .bind(uuid)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows.into_iter().next().map(Experiment::from))
    }
}
