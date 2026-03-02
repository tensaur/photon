use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::id::ProjectId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub created_at: DateTime<Utc>,
}
