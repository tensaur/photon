use std::sync::Mutex;

use async_channel::{Receiver, Sender};
use bytes::BytesMut;
use photon_protocol::ports::codec::Codec;

use crate::ports::{MaybeSend, MaybeSync, Transport, TransportError};

const CHANNEL_CAPACITY: usize = 64;

struct WsConnection {
    outgoing: Sender<Vec<u8>>,
    incoming: Receiver<Result<Vec<u8>, String>>,
}

/// WebSocket transport backed by async channels.
pub struct WebSocketTransport<C> {
    pub(crate) codec: C,
    conn: Mutex<Option<WsConnection>>,
}

impl<C: Clone> Clone for WebSocketTransport<C> {
    fn clone(&self) -> Self {
        let conn = self.conn.lock().unwrap();
        Self {
            codec: self.codec.clone(),
            conn: Mutex::new(conn.as_ref().map(|c| WsConnection {
                outgoing: c.outgoing.clone(),
                incoming: c.incoming.clone(),
            })),
        }
    }
}

impl<C> WebSocketTransport<C> {
    /// Create an unconnected WebSocket transport.
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            conn: Mutex::new(None),
        }
    }

    fn set_conn(&self, outgoing: Sender<Vec<u8>>, incoming: Receiver<Result<Vec<u8>, String>>) {
        *self.conn.lock().unwrap() = Some(WsConnection { outgoing, incoming });
    }

    /// Create a transport from pre-built channels.
    pub fn from_channels(
        codec: C,
        outgoing: Sender<Vec<u8>>,
        incoming: Receiver<Result<Vec<u8>, String>>,
    ) -> Self {
        Self {
            codec,
            conn: Mutex::new(Some(WsConnection { outgoing, incoming })),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn connect(&self, url: &str) -> Result<(), TransportError> {
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        let (outgoing, incoming) = spawn_native_bridge(ws);
        self.set_conn(outgoing, incoming);
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn connect(&self, url: &str) -> Result<(), TransportError> {
        use gloo_net::websocket::futures::WebSocket;

        let ws = WebSocket::open(url).map_err(|e| TransportError::Connection(e.to_string()))?;

        let (outgoing, incoming) = spawn_wasm_bridge(ws);
        self.set_conn(outgoing, incoming);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn accept(stream: tokio::net::TcpStream, codec: C) -> Result<Self, TransportError> {
        let ws = tokio_tungstenite::accept_async(stream)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        let (outgoing, incoming) = spawn_native_bridge(ws);
        Ok(Self {
            codec,
            conn: Mutex::new(Some(WsConnection { outgoing, incoming })),
        })
    }
}

impl<S, R, C> Transport<S, R> for WebSocketTransport<C>
where
    S: MaybeSend + MaybeSync + 'static,
    R: MaybeSend + 'static,
    C: Codec<S> + Codec<R> + MaybeSend + MaybeSync + Clone + 'static,
{
    async fn send(&self, msg: &S) -> Result<(), TransportError> {
        let mut buf = BytesMut::new();
        Codec::<S>::encode(&self.codec, msg, &mut buf)
            .map_err(|e| TransportError::Request(e.to_string()))?;

        let outgoing = {
            let guard = self.conn.lock().unwrap();
            guard
                .as_ref()
                .ok_or_else(|| TransportError::Connection("not connected".into()))?
                .outgoing
                .clone()
        };

        outgoing
            .send(buf.to_vec())
            .await
            .map_err(|_| TransportError::StreamClosed("outgoing channel closed".into()))
    }

    async fn recv(&self) -> Result<R, TransportError> {
        let incoming = {
            let guard = self.conn.lock().unwrap();
            guard
                .as_ref()
                .ok_or_else(|| TransportError::Connection("not connected".into()))?
                .incoming
                .clone()
        };

        let data = incoming
            .recv()
            .await
            .map_err(|_| TransportError::StreamClosed("incoming channel closed".into()))?
            .map_err(TransportError::StreamClosed)?;

        Codec::<R>::decode(&self.codec, &data).map_err(|e| TransportError::Request(e.to_string()))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_native_bridge<S>(
    ws: tokio_tungstenite::WebSocketStream<S>,
) -> (Sender<Vec<u8>>, Receiver<Result<Vec<u8>, String>>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let (out_tx, out_rx) = async_channel::bounded::<Vec<u8>>(CHANNEL_CAPACITY);
    let (in_tx, in_rx) = async_channel::bounded::<Result<Vec<u8>, String>>(CHANNEL_CAPACITY);

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

#[cfg(target_arch = "wasm32")]
fn spawn_wasm_bridge(
    ws: gloo_net::websocket::futures::WebSocket,
) -> (Sender<Vec<u8>>, Receiver<Result<Vec<u8>, String>>) {
    use futures_util::{SinkExt, StreamExt};
    use gloo_net::websocket::Message;

    let (out_tx, out_rx) = async_channel::bounded::<Vec<u8>>(CHANNEL_CAPACITY);
    let (in_tx, in_rx) = async_channel::bounded::<Result<Vec<u8>, String>>(CHANNEL_CAPACITY);

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
