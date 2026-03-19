mod accumulator;
mod builder;
pub mod error;
mod run;

pub use builder::RunBuilder;
pub use error::{FinishError, LogError, StartError};
pub use photon_core::types::id::RunId;
pub use photon_protocol::codec::CodecChoice;
pub use photon_protocol::compressor::CompressorChoice;
pub use photon_wal::WalChoice;
pub use run::{Run, RunStats};

impl Run {
    pub fn builder() -> RunBuilder {
        RunBuilder::default()
    }
}
