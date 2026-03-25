use async_channel::{Receiver, Sender};
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::{AsyncRead, AsyncWrite};

use crate::ports::{ByteTransport, TransportError};

const CHANNEL_CAPACITY: usize = 64;

/// WebSocket transport backed by async channels.
#[derive(Clone)]
pub struct WebSocketTransport {
    outgoing: Sender<Vec<u8>>,
    incoming: Receiver<Result<Vec<u8>, String>>,
}

impl WebSocketTransport {
    /// Create a transport from pre-built channels.
    pub fn from_channels(
        outgoing: Sender<Vec<u8>>,
        incoming: Receiver<Result<Vec<u8>, String>>,
    ) -> Self {
        Self { outgoing, incoming }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn connect(url: &str) -> Result<Self, TransportError> {
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        let (outgoing, incoming) = spawn_native_bridge(ws);
        Ok(Self { outgoing, incoming })
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn connect(url: &str) -> Result<Self, TransportError> {
        use gloo_net::websocket::futures::WebSocket;
        let ws = WebSocket::open(url).map_err(|e| TransportError::Connection(e.to_string()))?;

        let (outgoing, incoming) = spawn_wasm_bridge(ws);
        Ok(Self { outgoing, incoming })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ByteTransport for WebSocketTransport {
    async fn send_bytes(&self, bytes: &[u8]) -> Result<(), TransportError> {
        self.outgoing
            .send(bytes.to_vec())
            .await
            .map_err(|_| TransportError::StreamClosed("outgoing channel closed".into()))
    }

    async fn recv_bytes(&self) -> Result<Vec<u8>, TransportError> {
        self.incoming
            .recv()
            .await
            .map_err(|_| TransportError::StreamClosed("incoming channel closed".into()))?
            .map_err(TransportError::StreamClosed)
    }
}

fn bridge_channels() -> (
    Sender<Vec<u8>>,
    Receiver<Vec<u8>>,
    Sender<Result<Vec<u8>, String>>,
    Receiver<Result<Vec<u8>, String>>,
) {
    let (out_tx, out_rx) = async_channel::bounded(CHANNEL_CAPACITY);
    let (in_tx, in_rx) = async_channel::bounded(CHANNEL_CAPACITY);
    (out_tx, out_rx, in_tx, in_rx)
}

/// Bridge a native (tokio) WebSocket to async channels.
#[cfg(not(target_arch = "wasm32"))]
fn spawn_native_bridge<S>(
    ws: tokio_tungstenite::WebSocketStream<S>,
) -> (Sender<Vec<u8>>, Receiver<Result<Vec<u8>, String>>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (out_tx, out_rx, in_tx, in_rx) = bridge_channels();
    let (mut sink, mut stream) = ws.split();

    tokio::spawn(async move {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if in_tx.send(Ok(data.into())).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    in_tx.send(Err(e.to_string())).await.ok();
                    break;
                }
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
        sink.send(Message::Close(None)).await.ok();
    });

    (out_tx, in_rx)
}

/// Bridge a WASM (gloo-net) WebSocket to async channels.
#[cfg(target_arch = "wasm32")]
fn spawn_wasm_bridge(
    ws: gloo_net::websocket::futures::WebSocket,
) -> (Sender<Vec<u8>>, Receiver<Result<Vec<u8>, String>>) {
    use futures_util::{SinkExt, StreamExt};
    use gloo_net::websocket::Message;

    let (out_tx, out_rx, in_tx, in_rx) = bridge_channels();
    let (mut sink, mut stream) = ws.split();

    wasm_bindgen_futures::spawn_local(async move {
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(Message::Bytes(data)) => {
                    if in_tx.send(Ok(data)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    in_tx.send(Err(e.to_string())).await.ok();
                    break;
                }
                _ => {}
            }
        }
    });

    wasm_bindgen_futures::spawn_local(async move {
        while let Ok(data) = out_rx.recv().await {
            if sink.send(Message::Bytes(data)).await.is_err() {
                break;
            }
        }
        sink.close().await.ok();
    });

    (out_tx, in_rx)
}
