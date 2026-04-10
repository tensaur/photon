use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

macro_rules! define_id {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl From<Uuid> for $name {
            fn from(uuid: Uuid) -> Self {
                Self(uuid)
            }
        }

        impl From<$name> for Uuid {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl AsRef<Uuid> for $name {
            fn as_ref(&self) -> &Uuid {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse::<Uuid>().map(Self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl $name {
            /// First 8 hex characters of the UUID for compact display.
            pub fn short(&self) -> String {
                self.0.simple().to_string()[..8].to_owned()
            }
        }
    };
}

define_id!(RunId);
define_id!(ExperimentId);
define_id!(ProjectId);
define_id!(UserId);
define_id!(TenantId);

impl ProjectId {
    pub fn from_name(tenant: &TenantId, name: &str) -> Self {
        let tenant_uuid: Uuid = (*tenant).into();
        let mut key = tenant_uuid.as_bytes().to_vec();
        key.extend_from_slice(name.as_bytes());
        Self(Uuid::new_v5(&Uuid::NAMESPACE_OID, &key))
    }
}

impl ExperimentId {
    pub fn from_name(project: &ProjectId, name: &str) -> Self {
        let project_uuid: Uuid = (*project).into();
        let mut key = project_uuid.as_bytes().to_vec();
        key.extend_from_slice(name.as_bytes());
        Self(Uuid::new_v5(&Uuid::NAMESPACE_OID, &key))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}
