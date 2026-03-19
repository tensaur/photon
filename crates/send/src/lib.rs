pub mod domain;
pub mod inbound;

pub use domain::error::TransportError;
pub use domain::service::{RecoveryError, SendError, SenderService, SenderStats};
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
