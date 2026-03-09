use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use prost::Message;

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, MetricPoint};
use photon_core::types::query::{MetricQuery, MetricSeries, QueryRequest, QueryResponse};

use crate::codec::protobuf::types::{
    MetricBatchContent, MetricPointCompact, ProtoMetricQuery, ProtoMetricSeries,
    ProtoQueryRequest, ProtoQueryResponse,
};
use crate::ports::codec::{Codec, CodecError};

#[derive(Clone)]
pub struct ProtobufCodec;

impl Codec<MetricBatch> for ProtobufCodec {
    fn encode(&self, batch: &MetricBatch, output: &mut BytesMut) -> Result<(), CodecError> {
        let mut key_to_index: HashMap<&str, u32> = HashMap::new();
        let mut keys: Vec<String> = Vec::new();

        for p in &batch.points {
            let key_str = p.key.as_str();
            if !key_to_index.contains_key(key_str) {
                let idx = keys.len() as u32;
                key_to_index.insert(key_str, idx);
                keys.push(key_str.to_owned());
            }
        }

        let proto = MetricBatchContent {
            run_id: batch.run_id.to_string(),
            keys,
            points: batch
                .points
                .iter()
                .map(|p| MetricPointCompact {
                    key_index: key_to_index[p.key.as_str()],
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

        let metrics: Vec<Metric> = proto
            .keys
            .iter()
            .map(|k| {
                Metric::new(k).map_err(|e| CodecError::DecodeFailed {
                    reason: format!("invalid metric key: {e}"),
                })
            })
            .collect::<Result<Vec<_>, CodecError>>()?;

        let points = proto
            .points
            .into_iter()
            .map(|p| {
                let key = metrics
                    .get(p.key_index as usize)
                    .ok_or_else(|| CodecError::DecodeFailed {
                        reason: format!("key_index {} out of range (have {} keys)", p.key_index, metrics.len()),
                    })?
                    .clone();

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
        let proto = ProtoQueryRequest::decode(input).map_err(|e| CodecError::DecodeFailed {
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
        let proto = ProtoQueryResponse::decode(input).map_err(|e| CodecError::DecodeFailed {
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
        let proto = ProtoMetricQuery::decode(input).map_err(|e| CodecError::DecodeFailed {
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
        let proto = ProtoMetricSeries::decode(input).map_err(|e| CodecError::DecodeFailed {
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
