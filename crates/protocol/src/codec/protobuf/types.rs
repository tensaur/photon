pub mod ingest {
    pub mod v1 {
        tonic::include_proto!("photon.ingest.v1");
    }
}

pub mod query {
    pub mod v1 {
        tonic::include_proto!("photon.query.v1");
    }
}

pub use ingest::v1::{
    AckStatus as ProtoAckStatus, MetricBatchAck, MetricBatchContent, MetricBatchRequest,
    MetricPointCompact, WatermarkRequest, WatermarkResponse,
    ingest_service_client::IngestServiceClient,
    ingest_service_server::{IngestService, IngestServiceServer},
};

pub use query::v1::{
    AggregatedData as ProtoAggregatedData, DataPoint as ProtoDataPoint,
    MetricQuery as ProtoMetricQuery, MetricSeries as ProtoMetricSeries,
    QueryRequest as ProtoQueryRequest, QueryResponse as ProtoQueryResponse,
    RangePoint as ProtoRangePoint, RawData as ProtoRawData, metric_series::Data as ProtoSeriesData,
};
