pub mod flatbuffers;
pub mod json;
pub mod protobuf;

use bytes::BytesMut;
use photon_core::types::metric::MetricBatch;
use crate::ports::codec::{BatchCodec, CodecError};
use self::protobuf::codec::ProtobufCodec;

#[derive(Clone, Debug, Default)]
pub enum CodecChoice {
    #[default]
    Protobuf,
}

impl CodecChoice {
    pub fn protobuf() -> Self {
        Self::Protobuf
    }
}

impl BatchCodec for CodecChoice {
    fn encode(&self, batch: &MetricBatch, output: &mut BytesMut) -> Result<(), CodecError> {
        match self {
            Self::Protobuf => ProtobufCodec.encode(batch, output),
        }
    }

    fn decode(&self, input: &[u8]) -> Result<MetricBatch, CodecError> {
        match self {
            Self::Protobuf => ProtobufCodec.decode(input),
        }
    }
}
