use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use photon_protocol::ports::codec::Codec;

use crate::ports::{ByteTransport, Transport, TransportError};

/// Codec transport adapter — encodes/decodes once, delegates framing to a `ByteTransport`.
pub struct CodecTransport<C, T: ?Sized> {
    codec: C,
    inner: Arc<T>,
}

impl<C: Clone, T: ?Sized> Clone for CodecTransport<C, T> {
    fn clone(&self) -> Self {
        Self {
            codec: self.codec.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C, T> CodecTransport<C, T> {
    pub fn new(codec: C, inner: T) -> Self {
        Self {
            codec,
            inner: Arc::new(inner),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, R, C, T> Transport<S, R> for CodecTransport<C, T>
where
    S: Send + Sync + 'static,
    R: Send + 'static,
    C: Codec<S> + Codec<R> + Clone + Send + Sync + 'static,
    T: ByteTransport + ?Sized,
{
    async fn send(&self, msg: &S) -> Result<(), TransportError> {
        let mut buf = BytesMut::new();

        self.codec
            .encode(msg, &mut buf)
            .map_err(|e| TransportError::Request(e.to_string()))?;

        self.inner.send_bytes(&buf).await
    }

    async fn recv(&self) -> Result<R, TransportError> {
        let bytes = self.inner.recv_bytes().await?;

        self.codec
            .decode(&bytes)
            .map_err(|e| TransportError::Request(e.to_string()))
    }
}
