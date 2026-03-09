use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use prost::Message;

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, MetricPoint};
use photon_core::types::query::{
    MetricQuery, MetricSeries, QueryRequest, QueryResponse,
};

use crate::codec::protobuf::types::{
    MetricBatchContent, MetricPointProto, ProtoMetricQuery, ProtoMetricSeries,
    ProtoQueryRequest, ProtoQueryResponse,
};
use crate::ports::codec::{Codec, CodecError};

#[derive(Clone)]
pub struct ProtobufCodec;

impl Codec<MetricBatch> for ProtobufCodec {
    fn encode(&self, batch: &MetricBatch, output: &mut BytesMut) -> Result<(), CodecError> {
        let proto = MetricBatchContent {
            run_id: batch.run_id.to_string(),
            points: batch
                .points
                .iter()
                .map(|p| MetricPointProto {
                    key: p.key.as_str().to_owned(),
                    value: p.value,
                    step: p.step,
                    timestamp_epoch_ms: system_time_to_epoch_ms(p.timestamp),
                })
                .collect(),
        };

        let len = proto.encoded_len();
        output.reserve(len);

        proto.encode(output).map_err(|e| CodecError::EncodeFailed {
            reason: e.to_string(),
        })?;

        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<MetricBatch, CodecError> {
        let proto = MetricBatchContent::decode(input).map_err(|e| CodecError::DecodeFailed {
            reason: e.to_string(),
        })?;

        let run_id: uuid::Uuid = proto.run_id.parse().map_err(|_| CodecError::DecodeFailed {
            reason: format!("invalid run_id: {}", proto.run_id),
        })?;

        let points = proto
            .points
            .into_iter()
            .map(|p| {
                let key = Metric::new(p.key).map_err(|e| CodecError::DecodeFailed {
                    reason: format!("invalid metric key: {e}"),
                })?;

                Ok(MetricPoint {
                    key,
                    value: p.value,
                    step: p.step,
                    timestamp: epoch_ms_to_system_time(p.timestamp_epoch_ms),
                })
            })
            .collect::<Result<Vec<_>, CodecError>>()?;

        Ok(MetricBatch {
            run_id: RunId::from(run_id),
            points,
        })
    }
}

impl Codec<QueryRequest> for ProtobufCodec {
    fn encode(&self, value: &QueryRequest, output: &mut BytesMut) -> Result<(), CodecError> {
        let proto = ProtoQueryRequest::from(value);
        let len = proto.encoded_len();
        output.reserve(len);

        proto.encode(output).map_err(|e| CodecError::EncodeFailed {
            reason: e.to_string(),
        })?;

        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<QueryRequest, CodecError> {
        let proto =
            ProtoQueryRequest::decode(input).map_err(|e| CodecError::DecodeFailed {
                reason: e.to_string(),
            })?;

        QueryRequest::try_from(proto).map_err(|e| CodecError::DecodeFailed {
            reason: e.to_string(),
        })
    }
}

impl Codec<QueryResponse> for ProtobufCodec {
    fn encode(&self, value: &QueryResponse, output: &mut BytesMut) -> Result<(), CodecError> {
        let proto = ProtoQueryResponse::from(value);
        let len = proto.encoded_len();
        output.reserve(len);

        proto.encode(output).map_err(|e| CodecError::EncodeFailed {
            reason: e.to_string(),
        })?;

        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<QueryResponse, CodecError> {
        let proto =
            ProtoQueryResponse::decode(input).map_err(|e| CodecError::DecodeFailed {
                reason: e.to_string(),
            })?;

        QueryResponse::try_from(proto).map_err(|e| CodecError::DecodeFailed {
            reason: e.to_string(),
        })
    }
}

impl Codec<MetricQuery> for ProtobufCodec {
    fn encode(&self, value: &MetricQuery, output: &mut BytesMut) -> Result<(), CodecError> {
        let proto = ProtoMetricQuery::from(value);
        let len = proto.encoded_len();
        output.reserve(len);

        proto.encode(output).map_err(|e| CodecError::EncodeFailed {
            reason: e.to_string(),
        })?;

        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<MetricQuery, CodecError> {
        let proto =
            ProtoMetricQuery::decode(input).map_err(|e| CodecError::DecodeFailed {
                reason: e.to_string(),
            })?;

        MetricQuery::try_from(proto).map_err(|e| CodecError::DecodeFailed {
            reason: e.to_string(),
        })
    }
}

impl Codec<MetricSeries> for ProtobufCodec {
    fn encode(&self, value: &MetricSeries, output: &mut BytesMut) -> Result<(), CodecError> {
        let proto = ProtoMetricSeries::from(value);
        let len = proto.encoded_len();
        output.reserve(len);

        proto.encode(output).map_err(|e| CodecError::EncodeFailed {
            reason: e.to_string(),
        })?;

        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<MetricSeries, CodecError> {
        let proto =
            ProtoMetricSeries::decode(input).map_err(|e| CodecError::DecodeFailed {
                reason: e.to_string(),
            })?;

        MetricSeries::try_from(proto).map_err(|e| CodecError::DecodeFailed {
            reason: e.to_string(),
        })
    }
}

fn system_time_to_epoch_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

fn epoch_ms_to_system_time(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}
