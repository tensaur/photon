use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use photon_core::types::ack::{AckResult, AckStatus};
use photon_core::types::batch::AssembledBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::codec::protobuf::types::{
    MetricBatchAck, MetricBatchRequest, ProtoAckStatus, WatermarkRequest, WatermarkResponse,
};

#[derive(Debug, thiserror::Error)]
pub enum ProtoConversionError {
    #[error("invalid run_id: {0}")]
    InvalidRunId(String),

    #[error("unrecognised ack status: {0}")]
    UnknownAckStatus(i32),

    #[error("missing required field: {0}")]
    MissingField(&'static str),
}

// SDK to wire
impl From<&AssembledBatch> for MetricBatchRequest {
    fn from(batch: &AssembledBatch) -> Self {
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

impl From<AckStatus> for ProtoAckStatus {
    fn from(status: AckStatus) -> Self {
        match status {
            AckStatus::Ok => ProtoAckStatus::Ok,
            AckStatus::Duplicate => ProtoAckStatus::Duplicate,
            AckStatus::Rejected => ProtoAckStatus::Rejected,
        }
    }
}

// wire to server, TryFrom as network data can be malformed
impl TryFrom<MetricBatchRequest> for AssembledBatch {
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

// wire to SDK, TryFrom as ack status enum could have an invalid value
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

// server to wire
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

fn system_time_to_epoch_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

fn epoch_ms_to_system_time(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}
