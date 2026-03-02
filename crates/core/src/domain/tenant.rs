use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::id::{TenantId, UserId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthIdentity {
    pub tenant_id: TenantId,
    pub user_id: UserId,
}

impl Default for AuthIdentity {
    fn default() -> Self {
        Self {
            tenant_id: Uuid::nil().into(),
            user_id: Uuid::nil().into(),
        }
    }
}
