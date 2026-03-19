mod builder;
mod domain;
mod inbound;
mod outbound;

pub use builder::RunBuilder;
pub use inbound::error::SdkError as PhotonSdkError;
pub use inbound::run::{Run, RunStats};
pub use outbound::wal::WalChoice;
pub use photon_core::types::id::RunId;
pub use photon_protocol::codec::CodecChoice;
pub use photon_protocol::compressor::CompressorChoice;

impl Run {
    pub fn builder() -> RunBuilder {
        RunBuilder::default()
    }
}
