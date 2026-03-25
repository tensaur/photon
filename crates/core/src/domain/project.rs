use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::id::{ProjectId, TenantId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
