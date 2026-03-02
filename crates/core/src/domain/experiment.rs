use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::id::{ExperimentId, ProjectId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Experiment {
    pub id: ExperimentId,
    pub project_id: ProjectId,
    pub name: String,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
