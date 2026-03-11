mod domain;
mod inbound;
mod outbound;

pub type Run = inbound::run::Run<
    domain::service::Service<
        outbound::wal::SharedDiskWal,
        photon_protocol::codec::protobuf::codec::ProtobufCodec,
        photon_protocol::compressor::zstd::ZstdCompressor,
    >,
>;

pub use domain::service::PipelineService;
pub use inbound::error::SdkError as PhotonSdkError;
pub use inbound::run::{RunBuilder, RunStats};
pub use photon_core::types::id::RunId;
