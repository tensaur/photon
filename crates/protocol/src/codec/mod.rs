pub mod postcard;

use bytes::BytesMut;
use serde::{Serialize, de::DeserializeOwned};

use crate::ports::codec::{Codec, CodecError};

use self::postcard::PostcardCodec;

#[derive(Clone, Debug, Default)]
pub enum CodecChoice {
    #[default]
    Postcard,
}

impl CodecChoice {
    pub fn postcard() -> Self {
        Self::Postcard
    }
}

impl<T> Codec<T> for CodecChoice
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
