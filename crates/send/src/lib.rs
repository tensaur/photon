pub mod domain;
pub mod inbound;

pub use domain::ack::SenderStats;
pub use domain::error::{RecoveryError, SendError, TransportError};
pub use domain::service::SenderService;
pub use inbound::run::run_sender_thread;

/// Orchestration-level error for the sender thread.
#[derive(Debug, thiserror::Error)]
pub enum SenderThreadError {
    #[error("failed to create sender runtime")]
    Runtime(#[source] std::io::Error),
    #[error("sender connection failed")]
    Connection(#[from] TransportError),
    #[error("WAL recovery failed")]
    Recovery(#[from] RecoveryError),
    #[error("sender run loop failed")]
    Sender(#[from] SendError),
}
