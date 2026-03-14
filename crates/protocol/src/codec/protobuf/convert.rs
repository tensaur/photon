use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use photon_core::types::ack::{AckResult, AckStatus};
use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{
    DataPoint, MetricQuery, MetricSeries, QueryRequest, QueryResponse, RangePoint, SeriesData,
};
use photon_core::types::sequence::SequenceNumber;

use crate::codec::protobuf::types::{
    MetricBatchAck, MetricBatchRequest, ProtoAckStatus, ProtoAggregatedData, ProtoDataPoint,
    ProtoMetricQuery, ProtoMetricSeries, ProtoQueryRequest, ProtoQueryResponse, ProtoRangePoint,
    ProtoRawData, ProtoSeriesData, WatermarkRequest, WatermarkResponse,
};

#[derive(Debug, thiserror::Error)]
pub enum ProtoConversionError {
    #[error("invalid run_id: {0}")]
    InvalidRunId(String),

    #[error("invalid metric key: {0}")]
    InvalidMetricKey(String),

    #[error("unrecognised ack status: {0}")]
    UnknownAckStatus(i32),

    #[error("missing required field: {0}")]
    MissingField(&'static str),
}

impl From<&WireBatch> for MetricBatchRequest {
    fn from(batch: &WireBatch) -> Self {
        Self {
            run_id: batch.run_id.to_string(),
            sequence_number: u64::from(batch.sequence_number),
            compressed_payload: batch.compressed_payload.to_vec(),
            crc32: batch.crc32,
            compressor_name: String::new(),
            created_at_epoch_ms: system_time_to_epoch_ms(batch.created_at),
            point_count: batch.point_count as u32,
            uncompressed_size: batch.uncompressed_size as u32,
        }
    }
}

impl TryFrom<MetricBatchRequest> for WireBatch {
    type Error = ProtoConversionError;

    fn try_from(proto: MetricBatchRequest) -> Result<Self, Self::Error> {
        let run_id: uuid::Uuid = proto
            .run_id
            .parse()
            .map_err(|_| ProtoConversionError::InvalidRunId(proto.run_id.clone()))?;

        Ok(Self {
            run_id: RunId::from(run_id),
            sequence_number: SequenceNumber::from(proto.sequence_number),
            compressed_payload: Bytes::from(proto.compressed_payload),
            crc32: proto.crc32,
            created_at: epoch_ms_to_system_time(proto.created_at_epoch_ms),
            point_count: proto.point_count as usize,
            uncompressed_size: proto.uncompressed_size as usize,
        })
    }
}

impl From<&AckResult> for MetricBatchAck {
    fn from(ack: &AckResult) -> Self {
        let status = match ack.status {
            AckStatus::Ok => ProtoAckStatus::Ok,
            AckStatus::Duplicate => ProtoAckStatus::Duplicate,
            AckStatus::Rejected => ProtoAckStatus::Rejected,
        };

        Self {
            sequence_number: u64::from(ack.sequence_number),
            status: status.into(),
            message: String::new(),
        }
    }
}

impl TryFrom<MetricBatchAck> for AckResult {
    type Error = ProtoConversionError;

    fn try_from(proto: MetricBatchAck) -> Result<Self, Self::Error> {
        let status = match ProtoAckStatus::try_from(proto.status) {
            Ok(ProtoAckStatus::Ok) => AckStatus::Ok,
            Ok(ProtoAckStatus::Duplicate) => AckStatus::Duplicate,
            Ok(ProtoAckStatus::Rejected) => AckStatus::Rejected,
            Ok(ProtoAckStatus::Unspecified) => {
                return Err(ProtoConversionError::UnknownAckStatus(proto.status));
            }
            Err(_) => {
                return Err(ProtoConversionError::UnknownAckStatus(proto.status));
            }
        };

        Ok(Self {
            sequence_number: SequenceNumber::from(proto.sequence_number),
            status,
        })
    }
}

impl From<AckStatus> for ProtoAckStatus {
    fn from(status: AckStatus) -> Self {
        match status {
            AckStatus::Ok => ProtoAckStatus::Ok,
            AckStatus::Duplicate => ProtoAckStatus::Duplicate,
            AckStatus::Rejected => ProtoAckStatus::Rejected,
        }
    }
}

impl From<&RunId> for WatermarkRequest {
    fn from(run_id: &RunId) -> Self {
        Self {
            run_id: run_id.to_string(),
        }
    }
}

impl TryFrom<&WatermarkRequest> for RunId {
    type Error = ProtoConversionError;

    fn try_from(proto: &WatermarkRequest) -> Result<Self, Self::Error> {
        let uuid: uuid::Uuid = proto
            .run_id
            .parse()
            .map_err(|_| ProtoConversionError::InvalidRunId(proto.run_id.clone()))?;
        Ok(RunId::from(uuid))
    }
}

impl From<SequenceNumber> for WatermarkResponse {
    fn from(seq: SequenceNumber) -> Self {
        Self {
            sequence_number: u64::from(seq),
        }
    }
}

impl From<WatermarkResponse> for SequenceNumber {
    fn from(proto: WatermarkResponse) -> Self {
        SequenceNumber::from(proto.sequence_number)
    }
}

impl From<&QueryRequest> for ProtoQueryRequest {
    fn from(request: &QueryRequest) -> Self {
        Self {
            queries: request.queries.iter().map(ProtoMetricQuery::from).collect(),
        }
    }
}

impl TryFrom<ProtoQueryRequest> for QueryRequest {
    type Error = ProtoConversionError;

    fn try_from(proto: ProtoQueryRequest) -> Result<Self, Self::Error> {
        let queries = proto
            .queries
            .into_iter()
            .map(MetricQuery::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { queries })
    }
}

impl From<&MetricQuery> for ProtoMetricQuery {
    fn from(query: &MetricQuery) -> Self {
        Self {
            run_id: query.run_id.to_string(),
            key: query.key.as_str().to_owned(),
            step_start: query.step_range.start,
            step_end: query.step_range.end,
            target_points: query.target_points as u32,
        }
    }
}

impl TryFrom<ProtoMetricQuery> for MetricQuery {
    type Error = ProtoConversionError;

    fn try_from(proto: ProtoMetricQuery) -> Result<Self, Self::Error> {
        let run_id: uuid::Uuid = proto
            .run_id
            .parse()
            .map_err(|_| ProtoConversionError::InvalidRunId(proto.run_id.clone()))?;
        let key = Metric::new(&proto.key)
            .map_err(|_| ProtoConversionError::InvalidMetricKey(proto.key.clone()))?;

        Ok(Self {
            run_id: RunId::from(run_id),
            key,
            step_range: proto.step_start..proto.step_end,
            target_points: proto.target_points as usize,
        })
    }
}

impl From<&QueryResponse> for ProtoQueryResponse {
    fn from(response: &QueryResponse) -> Self {
        Self {
            series: response
                .series
                .iter()
                .map(ProtoMetricSeries::from)
                .collect(),
        }
    }
}

impl TryFrom<ProtoQueryResponse> for QueryResponse {
    type Error = ProtoConversionError;

    fn try_from(proto: ProtoQueryResponse) -> Result<Self, Self::Error> {
        let series = proto
            .series
            .into_iter()
            .map(MetricSeries::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { series })
    }
}

impl From<&MetricSeries> for ProtoMetricSeries {
    fn from(series: &MetricSeries) -> Self {
        let data = match &series.data {
            SeriesData::Raw { points } => ProtoSeriesData::Raw(ProtoRawData {
                points: points.iter().map(ProtoDataPoint::from).collect(),
            }),
            SeriesData::Aggregated { points, envelope } => {
                ProtoSeriesData::Aggregated(ProtoAggregatedData {
                    points: points.iter().map(ProtoDataPoint::from).collect(),
                    envelope: envelope.iter().map(ProtoRangePoint::from).collect(),
                })
            }
        };

        Self {
            run_id: series.run_id.to_string(),
            key: series.key.as_str().to_owned(),
            data: Some(data),
        }
    }
}

impl TryFrom<ProtoMetricSeries> for MetricSeries {
    type Error = ProtoConversionError;

    fn try_from(proto: ProtoMetricSeries) -> Result<Self, Self::Error> {
        let run_id: uuid::Uuid = proto
            .run_id
            .parse()
            .map_err(|_| ProtoConversionError::InvalidRunId(proto.run_id.clone()))?;
        let key = Metric::new(&proto.key)
            .map_err(|_| ProtoConversionError::InvalidMetricKey(proto.key.clone()))?;

        let data = match proto
            .data
            .ok_or(ProtoConversionError::MissingField("data"))?
        {
            ProtoSeriesData::Raw(raw) => SeriesData::Raw {
                points: raw.points.into_iter().map(DataPoint::from).collect(),
            },
            ProtoSeriesData::Aggregated(agg) => SeriesData::Aggregated {
                points: agg.points.into_iter().map(DataPoint::from).collect(),
                envelope: agg.envelope.into_iter().map(RangePoint::from).collect(),
            },
        };

        Ok(Self {
            run_id: RunId::from(run_id),
            key,
            data,
        })
    }
}

impl From<&DataPoint> for ProtoDataPoint {
    fn from(p: &DataPoint) -> Self {
        Self {
            step: p.step,
            value: p.value,
        }
    }
}

impl From<ProtoDataPoint> for DataPoint {
    fn from(proto: ProtoDataPoint) -> Self {
        Self {
            step: proto.step,
            value: proto.value,
        }
    }
}

impl From<&RangePoint> for ProtoRangePoint {
    fn from(p: &RangePoint) -> Self {
        Self {
            step_start: p.step_start,
            step_end: p.step_end,
            min: p.min,
            max: p.max,
        }
    }
}

impl From<ProtoRangePoint> for RangePoint {
    fn from(proto: ProtoRangePoint) -> Self {
        Self {
            step_start: proto.step_start,
            step_end: proto.step_end,
            min: proto.min,
            max: proto.max,
        }
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
