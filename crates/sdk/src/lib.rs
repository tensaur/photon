mod builder;
mod domain;
mod inbound;
mod outbound;

pub type Run = inbound::run::Run<domain::service::Service<outbound::wal::WalManagerChoice>>;

impl Run {
    pub fn builder() -> builder::RunBuilder {
        builder::RunBuilder::default()
    }
}

pub use builder::RunBuilder;
pub use domain::service::SdkService;
pub use inbound::error::SdkError as PhotonSdkError;
pub use inbound::run::RunStats;
pub use photon_core::types::id::RunId;
pub use photon_protocol::codec::CodecChoice;
pub use outbound::wal::WalChoice;
pub use photon_protocol::compressor::CompressorChoice;
