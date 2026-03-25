use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use reqwest::Client;

use crate::ports::{ByteTransport, TransportError};

#[derive(Clone)]
pub struct HttpTransport {
    client: Client,
    url: String,
    pending: Arc<Mutex<Option<Vec<u8>>>>,
}

impl HttpTransport {
    pub fn connect(url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            url: url.into(),
            pending: Arc::new(Mutex::new(None)),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ByteTransport for HttpTransport {
    async fn send_bytes(&self, bytes: &[u8]) -> Result<(), TransportError> {
        let resp = self
            .client
            .post(&self.url)
            .body(bytes.to_vec())
            .send()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TransportError::Request(format!("HTTP {}", resp.status())));
        }

        let resp_bytes = resp
            .bytes()
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        *self.pending.lock().unwrap() = Some(resp_bytes.to_vec());
        Ok(())
    }

    async fn recv_bytes(&self) -> Result<Vec<u8>, TransportError> {
        self.pending
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| TransportError::StreamClosed("no pending data".into()))
    }
}
