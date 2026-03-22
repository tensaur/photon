pub mod postcard;

pub use self::postcard::PostcardCodec;

use bytes::BytesMut;
use serde::{Serialize, de::DeserializeOwned};

use crate::ports::codec::{Codec, CodecError};

#[derive(Clone, Copy, Debug, Default)]
pub enum CodecKind {
    #[default]
    Postcard,
}

impl<T> Codec<T> for CodecKind
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    fn encode(&self, value: &T, output: &mut BytesMut) -> Result<(), CodecError> {
        match self {
            Self::Postcard => PostcardCodec.encode(value, output),
        }
    }

    fn decode(&self, input: &[u8]) -> Result<T, CodecError> {
        match self {
            Self::Postcard => PostcardCodec.decode(input),
        }
    }
}
