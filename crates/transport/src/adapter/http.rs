use std::sync::Mutex;

use bytes::BytesMut;
use photon_protocol::ports::codec::Codec;
use reqwest::Client;

use crate::ports::{MaybeSend, MaybeSync, Transport, TransportError};

pub struct HttpTransport<C> {
    pub(crate) codec: C,
    client: Option<Client>,
    url: Option<String>,
    pending: Mutex<Option<Vec<u8>>>,
    #[cfg(not(target_arch = "wasm32"))]
    writer: Mutex<Option<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>,
}

impl<C: Clone> Clone for HttpTransport<C> {
    fn clone(&self) -> Self {
        Self {
            codec: self.codec.clone(),
            client: self.client.clone(),
            url: self.url.clone(),
            pending: Mutex::new(None),
            #[cfg(not(target_arch = "wasm32"))]
            writer: Mutex::new(None),
        }
    }
}

impl<C> HttpTransport<C> {
    /// Create an unconnected HTTP transport
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            client: None,
            url: None,
            pending: Mutex::new(None),
            #[cfg(not(target_arch = "wasm32"))]
            writer: Mutex::new(None),
        }
    }

    /// Set the target URL and create the HTTP client
    pub fn connect(&mut self, url: impl Into<String>) {
        self.url = Some(url.into());
        self.client = Some(Client::new());
    }

    /// Accept an HTTP request from a raw TCP stream
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn accept(stream: tokio::net::TcpStream, codec: C) -> Result<Self, TransportError> {
        use tokio::io::{AsyncReadExt, BufWriter};

        let (mut reader, writer) = stream.into_split();

        let mut buf = Vec::with_capacity(8192);
        loop {
            let mut chunk = [0u8; 4096];
            let n = reader
                .read(&mut chunk)
                .await
                .map_err(|e| TransportError::Connection(e.to_string()))?;
            if n == 0 {
                return Err(TransportError::StreamClosed("connection closed".into()));
            }
            buf.extend_from_slice(&chunk[..n]);

            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut req = httparse::Request::new(&mut headers);
            match req.parse(&buf) {
                Ok(httparse::Status::Complete(header_len)) => {
                    const MAX_BODY_SIZE: usize = 16 * 1024 * 1024;

                    let content_length = req
                        .headers
                        .iter()
                        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
                        .and_then(|h| std::str::from_utf8(h.value).ok())
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0);

                    if content_length > MAX_BODY_SIZE {
                        return Err(TransportError::Request(format!(
                            "body too large: {content_length} bytes (max {MAX_BODY_SIZE})"
                        )));
                    }

                    let body_so_far = buf.len() - header_len;
                    if content_length > body_so_far {
                        buf.resize(header_len + content_length, 0);
                        reader
                            .read_exact(&mut buf[header_len + body_so_far..])
                            .await
                            .map_err(|e| TransportError::Connection(e.to_string()))?;
                    }

                    let body = buf[header_len..header_len + content_length].to_vec();

                    return Ok(Self {
                        codec,
                        client: None,
                        url: None,
                        pending: Mutex::new(Some(body)),
                        writer: Mutex::new(Some(BufWriter::new(writer))),
                    });
                }
                Ok(httparse::Status::Partial) => continue,
                Err(e) => {
                    return Err(TransportError::Request(format!("HTTP parse error: {e}")));
                }
            }
        }
    }
}

impl<S, R, C> Transport<S, R> for HttpTransport<C>
where
    S: MaybeSend + MaybeSync + 'static,
    R: MaybeSend + 'static,
    C: Codec<S> + Codec<R> + MaybeSend + MaybeSync + Clone + 'static,
{
    async fn send(&self, msg: &S) -> Result<(), TransportError> {
        let mut buf = BytesMut::new();
        Codec::<S>::encode(&self.codec, msg, &mut buf)
            .map_err(|e| TransportError::Request(e.to_string()))?;

        // Client: POST and stash response for next recv
        if let (Some(client), Some(url)) = (&self.client, &self.url) {
            let resp = client
                .post(url)
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

            *self.pending.lock().unwrap() = Some(bytes.to_vec());
            return Ok(());
        }

        // Server: write HTTP response to stream
        #[cfg(not(target_arch = "wasm32"))]
        {
            use tokio::io::AsyncWriteExt;

            let mut writer = self
                .writer
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| TransportError::StreamClosed("already sent".into()))?;

            let body = buf.to_vec();
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
                body.len()
            );
            writer
                .write_all(header.as_bytes())
                .await
                .map_err(|e| TransportError::Connection(e.to_string()))?;
            writer
                .write_all(&body)
                .await
                .map_err(|e| TransportError::Connection(e.to_string()))?;
            writer
                .flush()
                .await
                .map_err(|e| TransportError::Connection(e.to_string()))?;
            return Ok(());
        }

        #[cfg(target_arch = "wasm32")]
        Err(TransportError::Connection("not connected".into()))
    }

    async fn recv(&self) -> Result<R, TransportError> {
        let bytes = self
            .pending
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| TransportError::StreamClosed("no pending data".into()))?;

        Codec::<R>::decode(&self.codec, &bytes).map_err(|e| TransportError::Request(e.to_string()))
    }
}
