use bytes::BytesMut;
use serde::{Serialize, de::DeserializeOwned};

use crate::ports::codec::{Codec, CodecError};

/// Serde-based codec using postcard's compact binary format.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostcardCodec;

impl<T> Codec<T> for PostcardCodec
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    fn encode(&self, value: &T, output: &mut BytesMut) -> Result<(), CodecError> {
        let bytes = postcard::to_allocvec(value).map_err(|e| CodecError::EncodeFailed {
            reason: e.to_string(),
        })?;

        output.extend_from_slice(&bytes);
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<T, CodecError> {
        postcard::from_bytes(input).map_err(|e| CodecError::DecodeFailed {
            reason: e.to_string(),
        })
    }
}
