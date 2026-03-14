use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_protocol::codec::protobuf::convert::ProtoConversionError;
use photon_protocol::codec::protobuf::types::{
    IngestService as GrpcIngestService, IngestServiceServer, MetricBatchAck, MetricBatchRequest,
    ProtoAckStatus, WatermarkRequest, WatermarkResponse,
};

use crate::domain::service::IngestService;

/// gRPC handler. Thin adapter between proto wire format and domain service.
pub struct Handler<S: IngestService> {
    service: Arc<S>,
}

impl<S: IngestService> Handler<S> {
    pub fn new(service: S) -> Self {
        Self {
            service: Arc::new(service),
        }
    }

    pub fn into_server(self) -> IngestServiceServer<Self>
    where
        S: Send + 'static,
    {
        IngestServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl<S: IngestService + Send + Sync + 'static> GrpcIngestService for Handler<S> {
    type LogMetricsStream = ReceiverStream<Result<MetricBatchAck, Status>>;

    async fn log_metrics(
        &self,
        request: Request<Streaming<MetricBatchRequest>>,
    ) -> Result<Response<Self::LogMetricsStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(64);
        let service = Arc::clone(&self.service);

        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                let proto = match result {
                    Ok(proto) => proto,
                    Err(e) => {
                        tracing::warn!("stream receive error: {e}");
                        break;
                    }
                };

                let seq = proto.sequence_number;

                let batch = match WireBatch::try_from(proto) {
                    Ok(batch) => batch,
                    Err(e) => {
                        tracing::warn!("invalid batch: {e}");
                        let ack = MetricBatchAck {
                            sequence_number: seq,
                            status: ProtoAckStatus::Rejected.into(),
                            message: e.to_string(),
                        };
                        let _ = tx.send(Ok(ack)).await;
                        continue;
                    }
                };

                let ack = match service.ingest(&batch).await {
                    Ok(result) => MetricBatchAck {
                        sequence_number: u64::from(result.sequence_number),
                        status: ProtoAckStatus::from(result.status).into(),
                        message: String::new(),
                    },
                    Err(e) => {
                        tracing::error!("ingest error: {e}");
                        MetricBatchAck {
                            sequence_number: u64::from(batch.sequence_number),
                            status: ProtoAckStatus::Rejected.into(),
                            message: e.to_string(),
                        }
                    }
                };

                if tx.send(Ok(ack)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_watermark(
        &self,
        request: Request<WatermarkRequest>,
    ) -> Result<Response<WatermarkResponse>, Status> {
        let proto = request.into_inner();

        let run_id = RunId::try_from(&proto)
            .map_err(|e: ProtoConversionError| Status::invalid_argument(e.to_string()))?;

        let seq = self
            .service
            .watermark(&run_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(WatermarkResponse {
            sequence_number: u64::from(seq),
        }))
    }
}
