mod builder;
mod domain;
mod inbound;
mod outbound;

use domain::service::Service;
use inbound::run;
use outbound::wal::WalManagerChoice;

pub type Run = run::Run<Service<WalManagerChoice>>;

impl Run {
    pub fn builder() -> builder::RunBuilder {
        builder::RunBuilder::default()
    }
}

pub use builder::RunBuilder;
pub use domain::service::SdkService;
pub use inbound::error::SdkError as PhotonSdkError;
pub use inbound::run::RunStats;
pub use outbound::wal::WalChoice;
pub use photon_core::types::id::RunId;
pub use photon_protocol::codec::CodecChoice;
pub use photon_protocol::compressor::CompressorChoice;
