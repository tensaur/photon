pub mod domain;
pub mod inbound;

pub use domain::ack::UplinkStats;
pub use domain::error::{RecoveryError, UplinkTransportError, UplinkError};
pub use domain::service::UplinkService;
pub use inbound::thread::run_uplink;

/// Orchestration-level error for the uplink thread.
#[derive(Debug, thiserror::Error)]
pub enum UplinkThreadError {
    #[error("failed to create uplink runtime")]
    Runtime(#[source] std::io::Error),
    #[error("uplink connection failed")]
    Connection(#[from] UplinkTransportError),
    #[error("WAL recovery failed")]
    Recovery(#[from] RecoveryError),
    #[error("uplink run loop failed")]
    Uplink(#[from] UplinkError),
}
