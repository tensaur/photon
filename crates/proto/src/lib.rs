pub mod conversions;
pub mod codec;

pub mod ingest {
    pub mod v1 {
        tonic::include_proto!("photon.ingest.v1");
    }
}

pub use ingest::v1::{
    ingest_service_client::IngestServiceClient,
    ingest_service_server::{IngestService, IngestServiceServer},
    AckStatus as ProtoAckStatus,
    MetricBatchAck,
    MetricBatchContent,
    MetricBatchRequest,
    MetricPointProto,
    WatermarkRequest,
    WatermarkResponse,
};
