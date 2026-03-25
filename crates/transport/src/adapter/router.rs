use std::future::Future;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;

use photon_protocol::codec::CodecKind;

use super::websocket::WebSocketTransport;
use crate::codec::CodecTransport;
use crate::ports::{ByteTransport, TransportError};

pub struct Router {
    codec: CodecKind,
    router: axum::Router,
}

impl Router {
    pub fn new(codec: CodecKind) -> Self {
        Self {
            codec,
            router: axum::Router::new(),
        }
    }

    pub fn request_response<F, Fut>(mut self, path: &str, handler: F) -> Self
    where
        F: Fn(CodecTransport<CodecKind, BodyTransport>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let codec = self.codec.clone();
        self.router = self.router.route(
            path,
            axum::routing::post(move |body: axum::body::Bytes| {
                let handler = handler.clone();
                let codec = codec.clone();
                async move {
                    let transport = BodyTransport::new(body.to_vec());
                    let response = transport.clone();
                    handler(CodecTransport::new(codec, transport)).await;
                    match response.take_response() {
                        Some(bytes) => bytes.into_response(),
                        None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                    }
                }
            }),
        );
        self
    }

    pub fn websocket<F, Fut>(mut self, path: &str, handler: F) -> Self
    where
        F: Fn(CodecTransport<CodecKind, WebSocketTransport>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let codec = self.codec.clone();
        self.router = self.router.route(
            path,
            axum::routing::get(move |ws: WebSocketUpgrade| {
                let handler = handler.clone();
                let codec = codec.clone();
                async move {
                    ws.on_upgrade(move |socket| async move {
                        let transport = ws_from_axum(socket);
                        handler(CodecTransport::new(codec, transport)).await;
                    })
                }
            }),
        );
        self
    }

    pub fn fallback<F>(mut self, handler: F) -> Self
    where
        F: Fn(&str) -> Option<(Vec<u8>, String)> + Clone + Send + Sync + 'static,
    {
        self.router = self.router.fallback(move |uri: axum::http::Uri| {
            let handler = handler.clone();
            async move {
                let path = uri.path().trim_start_matches('/');
                let path = if path.is_empty() { "index.html" } else { path };
                match handler(path) {
                    Some((body, content_type)) => (
                        StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, content_type)],
                        body,
                    )
                        .into_response(),
                    None => StatusCode::NOT_FOUND.into_response(),
                }
            }
        });
        self
    }

    pub async fn serve(self, listener: TcpListener) {
        axum::serve(listener, self.router)
            .await
            .expect("http server failed");
    }
}

#[derive(Clone)]
pub struct BodyTransport {
    request_body: Arc<Mutex<Option<Vec<u8>>>>,
    response_body: Arc<Mutex<Option<Vec<u8>>>>,
}

impl BodyTransport {
    fn new(body: Vec<u8>) -> Self {
        Self {
            request_body: Arc::new(Mutex::new(Some(body))),
            response_body: Arc::new(Mutex::new(None)),
        }
    }

    fn take_response(&self) -> Option<Vec<u8>> {
        self.response_body.lock().unwrap().take()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ByteTransport for BodyTransport {
    async fn send_bytes(&self, bytes: &[u8]) -> Result<(), TransportError> {
        *self.response_body.lock().unwrap() = Some(bytes.to_vec());
        Ok(())
    }

    async fn recv_bytes(&self) -> Result<Vec<u8>, TransportError> {
        self.request_body
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| TransportError::StreamClosed("no pending data".into()))
    }
}

fn ws_from_axum(socket: WebSocket) -> WebSocketTransport {
    let (out_tx, out_rx) = async_channel::bounded::<Vec<u8>>(64);
    let (in_tx, in_rx) = async_channel::bounded::<Result<Vec<u8>, String>>(64);

    let (mut sink, mut stream) = socket.split();

    tokio::spawn(async move {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if in_tx.send(Ok(data.to_vec())).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    tokio::spawn(async move {
        while let Ok(data) = out_rx.recv().await {
            if sink.send(Message::Binary(data.into())).await.is_err() {
                break;
            }
        }
    });

    WebSocketTransport::from_channels(out_tx, in_rx)
}
