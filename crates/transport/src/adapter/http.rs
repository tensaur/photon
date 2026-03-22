use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use reqwest::Client;

#[cfg(not(target_arch = "wasm32"))]
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
#[cfg(not(target_arch = "wasm32"))]
use tokio::net::{
    TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::ports::{ByteTransport, TransportError};

#[derive(Clone)]
pub struct HttpTransport {
    inner: HttpInner,
}

#[derive(Clone)]
enum HttpInner {
    Client {
        client: Client,
        url: String,
        pending: Arc<Mutex<Option<Vec<u8>>>>,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Server {
        pending: Arc<Mutex<Option<Vec<u8>>>>,
        writer: Arc<Mutex<Option<BufWriter<OwnedWriteHalf>>>>,
    },
}

impl HttpTransport {
    /// Create a client-mode HTTP transport.
    pub fn connect(url: impl Into<String>) -> Self {
        Self {
            inner: HttpInner::Client {
                client: Client::new(),
                url: url.into(),
                pending: Arc::new(Mutex::new(None)),
            },
        }
    }

    /// Accept an HTTP request from a raw TCP stream.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn accept(stream: TcpStream) -> Result<Self, TransportError> {
        let (mut reader, writer) = stream.into_split();
        let body = Self::read_http_body(&mut reader, 16 * 1024 * 1024).await?;

        Ok(Self {
            inner: HttpInner::Server {
                pending: Arc::new(Mutex::new(Some(body))),
                writer: Arc::new(Mutex::new(Some(BufWriter::new(writer)))),
            },
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_http_body(
        reader: &mut OwnedReadHalf,
        max_body_size: usize,
    ) -> Result<Vec<u8>, TransportError> {
        let mut buf = Vec::with_capacity(8192);
        loop {
            let mut chunk = [0u8; 4096];
            let n = reader
                .read(&mut chunk)
                .await
                .map_err(TransportError::from_io)?;
            if n == 0 {
                return Err(TransportError::StreamClosed("connection closed".into()));
            }
            buf.extend_from_slice(&chunk[..n]);

            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut req = httparse::Request::new(&mut headers);
            match req.parse(&buf) {
                Ok(httparse::Status::Complete(header_len)) => {
                    let content_length = req
                        .headers
                        .iter()
                        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
                        .and_then(|h| std::str::from_utf8(h.value).ok())
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0);

                    if content_length > max_body_size {
                        return Err(TransportError::Request(format!(
                            "body too large: {content_length} bytes (max {max_body_size})"
                        )));
                    }

                    let body_so_far = buf.len() - header_len;
                    if content_length > body_so_far {
                        buf.resize(header_len + content_length, 0);
                        reader
                            .read_exact(&mut buf[header_len + body_so_far..])
                            .await
                            .map_err(TransportError::from_io)?;
                    }

                    return Ok(buf[header_len..header_len + content_length].to_vec());
                }
                Ok(httparse::Status::Partial) => continue,
                Err(e) => {
                    return Err(TransportError::Request(format!("HTTP parse error: {e}")));
                }
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ByteTransport for HttpTransport {
    async fn send_bytes(&self, bytes: &[u8]) -> Result<(), TransportError> {
        match &self.inner {
            HttpInner::Client {
                client,
                url,
                pending,
            } => {
                let resp = client
                    .post(url)
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

                *pending.lock().unwrap() = Some(resp_bytes.to_vec());
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            HttpInner::Server { writer, .. } => {
                let mut w = writer
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| TransportError::StreamClosed("already sent".into()))?;

                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
                    bytes.len()
                );
                w.write_all(header.as_bytes())
                    .await
                    .map_err(TransportError::from_io)?;
                w.write_all(bytes).await.map_err(TransportError::from_io)?;
                w.flush().await.map_err(TransportError::from_io)?;
                Ok(())
            }
        }
    }

    async fn recv_bytes(&self) -> Result<Vec<u8>, TransportError> {
        let pending = match &self.inner {
            HttpInner::Client { pending, .. } => pending,
            #[cfg(not(target_arch = "wasm32"))]
            HttpInner::Server { pending, .. } => pending,
        };

        pending
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| TransportError::StreamClosed("no pending data".into()))
    }
}
