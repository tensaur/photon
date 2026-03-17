use std::sync::Arc;

use bytes::BytesMut;
use photon_protocol::ports::codec::Codec;
use reqwest::Client;
use tokio::sync::Mutex;

use crate::ports::{Transport, TransportError};

/// HTTP transport adapter for request-response communication.
pub struct HttpTransport<C> {
    codec: C,
    client: Client,
    url: String,
    response: Arc<Mutex<Option<Vec<u8>>>>,
}

impl<C: Clone> Clone for HttpTransport<C> {
    fn clone(&self) -> Self {
        Self {
            codec: self.codec.clone(),
            client: self.client.clone(),
            url: self.url.clone(),
            response: Arc::clone(&self.response),
        }
    }
}

impl<C> HttpTransport<C> {
    pub fn new(url: impl Into<String>, codec: C) -> Self {
        Self {
            codec,
            client: Client::new(),
            url: url.into(),
            response: Arc::new(Mutex::new(None)),
        }
    }
}

impl<S, R, C> Transport<S, R> for HttpTransport<C>
where
    S: Send + Sync + 'static,
    R: Send + 'static,
    C: Codec<S> + Codec<R> + Send + Sync + Clone + 'static,
{
    async fn send(&self, msg: &S) -> Result<(), TransportError> {
        let mut buf = BytesMut::new();
        Codec::<S>::encode(&self.codec, msg, &mut buf)
            .map_err(|e| TransportError::Request(e.to_string()))?;

        let resp = self
            .client
            .post(&self.url)
            .body(buf.freeze())
            .send()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TransportError::Request(format!("HTTP {}", resp.status())));
        }

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| TransportError::Request(e.to_string()))?;

        *self.response.lock().await = Some(bytes.to_vec());
        Ok(())
    }

    async fn recv(&self) -> Result<R, TransportError> {
        let bytes = self
            .response
            .lock()
            .await
            .take()
            .ok_or_else(|| TransportError::Request("no pending response".into()))?;

        Codec::<R>::decode(&self.codec, &bytes)
            .map_err(|e| TransportError::Request(e.to_string()))
    }
}
