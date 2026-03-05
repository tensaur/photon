use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};
use tonic::transport::Channel;

use photon_core::types::ack::AckResult;
use photon_core::types::batch::AssembledBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use photon_protocol::codec::protobuf::convert::ProtoConversionError;
use photon_protocol::codec::protobuf::types::{
    IngestServiceClient, MetricBatchAck, MetricBatchRequest, WatermarkRequest,
};

use crate::domain::ports::transport::{BatchTransport, TransportError};

#[derive(Clone)]
pub struct GrpcTransport {
    request_tx: mpsc::Sender<MetricBatchRequest>,
    ack_rx: Arc<Mutex<tonic::Streaming<MetricBatchAck>>>,
    client: Arc<Mutex<IngestServiceClient<Channel>>>,
}

impl GrpcTransport {
    pub async fn connect(endpoint: &str, request_buffer: usize) -> Result<Self, TransportError> {
        let channel = Channel::from_shared(endpoint.to_owned())
            .map_err(|e| TransportError::Unknown(e.into()))?
            .connect()
            .await
            .map_err(|e| TransportError::ConnectionLost {
                reason: e.to_string(),
            })?;

        let mut client = IngestServiceClient::new(channel);

        let (request_tx, request_rx) = mpsc::channel(request_buffer);
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(request_rx);

        let response = client.log_metrics(request_stream).await.map_err(|e| {
            TransportError::ConnectionLost {
                reason: e.to_string(),
            }
        })?;

        let ack_rx = response.into_inner();

        Ok(Self {
            request_tx,
            ack_rx: Arc::new(Mutex::new(ack_rx)),
            client: Arc::new(Mutex::new(client)),
        })
    }
}

impl BatchTransport for GrpcTransport {
    async fn send(&self, batch: &AssembledBatch) -> Result<(), TransportError> {
        let proto: MetricBatchRequest = batch.into();

        self.request_tx
            .send(proto)
            .await
            .map_err(|_| TransportError::ConnectionLost {
                reason: "outbound stream closed".to_owned(),
            })
    }

    async fn recv_ack(&self) -> Result<AckResult, TransportError> {
        let mut ack_rx = self.ack_rx.lock().await;

        let proto = ack_rx
            .message()
            .await
            .map_err(|e| TransportError::ConnectionLost {
                reason: e.to_string(),
            })?
            .ok_or_else(|| TransportError::ConnectionLost {
                reason: "ack stream ended".to_owned(),
            })?;

        AckResult::try_from(proto)
            .map_err(|e: ProtoConversionError| TransportError::Unknown(e.into()))
    }

    async fn get_watermark(&self, run_id: &RunId) -> Result<SequenceNumber, TransportError> {
        let request: WatermarkRequest = run_id.into();

        let mut client = self.client.lock().await;

        let response =
            client
                .get_watermark(request)
                .await
                .map_err(|e| TransportError::ConnectionLost {
                    reason: e.to_string(),
                })?;

        Ok(SequenceNumber::from(response.into_inner()))
    }
}
