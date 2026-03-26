use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::domain::project::Project;
use photon_core::types::id::{ProjectId, TenantId};

use crate::ports::project::{ProjectReader, ProjectWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Row, Serialize, Deserialize)]
struct ProjectRow {
    #[serde(with = "clickhouse::serde::uuid")]
    id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    tenant_id: uuid::Uuid,
    name: String,
    created_at: i64,
    updated_at: i64,
}

impl From<&Project> for ProjectRow {
    fn from(p: &Project) -> Self {
        Self {
            id: p.id.into(),
            tenant_id: p.tenant_id.into(),
            name: p.name.clone(),
            created_at: p.created_at.timestamp_millis(),
            updated_at: p.updated_at.timestamp_millis(),
        }
    }
}

impl From<ProjectRow> for Project {
    fn from(r: ProjectRow) -> Self {
        Self {
            id: ProjectId::from(r.id),
            tenant_id: TenantId::from(r.tenant_id),
            name: r.name,
            created_at: chrono::DateTime::from_timestamp_millis(r.created_at)
                .unwrap_or_default(),
            updated_at: chrono::DateTime::from_timestamp_millis(r.updated_at)
                .unwrap_or_default(),
        }
    }
}

#[derive(Clone)]
pub struct ClickHouseProjectStore {
    client: clickhouse::Client,
}

impl ClickHouseProjectStore {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }
}

impl ProjectWriter for ClickHouseProjectStore {
    async fn upsert_project(&self, project: &Project) -> Result<(), WriteError> {
        let mut insert = self
            .client
            .insert("projects")
            .map_err(|e| WriteError::Unknown(e.into()))?;

        insert
            .write(&ProjectRow::from(project))
            .await
            .map_err(|e| WriteError::Unknown(e.into()))?;

        insert
            .end()
            .await
            .map_err(|e| WriteError::Unknown(e.into()))?;
        Ok(())
    }
}

impl ProjectReader for ClickHouseProjectStore {
    async fn list_projects(&self) -> Result<Vec<Project>, ReadError> {
        let rows: Vec<ProjectRow> = self
            .client
            .query("SELECT ?fields FROM projects FINAL")
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows.into_iter().map(Project::from).collect())
    }

    async fn get_project(&self, id: &ProjectId) -> Result<Option<Project>, ReadError> {
        let uuid: uuid::Uuid = (*id).into();

        let rows: Vec<ProjectRow> = self
            .client
            .query("SELECT ?fields FROM projects FINAL WHERE id = ?")
            .bind(uuid)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Unknown(e.into()))?;

        Ok(rows.into_iter().next().map(Project::from))
    }
}
