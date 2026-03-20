pub mod http;
pub mod websocket;

#[cfg(not(target_arch = "wasm32"))]
pub mod tcp;

use photon_protocol::codec::CodecChoice;
use photon_protocol::ports::codec::Codec;

use self::http::HttpTransport;
use self::websocket::WebSocketTransport;
use crate::ports::{MaybeSend, MaybeSync, Transport, TransportError};

/// Runtime transport selection for both client and server sides.
#[derive(Clone)]
pub enum TransportChoice {
    #[cfg(not(target_arch = "wasm32"))]
    Tcp(tcp::TcpTransport<CodecChoice>),
    Http(HttpTransport<CodecChoice>),
    WebSocket(WebSocketTransport<CodecChoice>),
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for TransportChoice {
    fn default() -> Self {
        Self::tcp(CodecChoice::default())
    }
}

impl TransportChoice {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn tcp(codec: CodecChoice) -> Self {
        Self::Tcp(tcp::TcpTransport::new(codec))
    }

    pub fn http(codec: CodecChoice) -> Self {
        Self::Http(HttpTransport::new(codec))
    }

    pub fn websocket(codec: CodecChoice) -> Self {
        Self::WebSocket(WebSocketTransport::new(codec))
    }

    pub async fn connect(&mut self, endpoint: &str) -> Result<(), TransportError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Tcp(t) => t.connect(endpoint).await,
            Self::Http(t) => {
                t.connect(endpoint);
                Ok(())
            }
            Self::WebSocket(t) => t.connect(endpoint).await,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn accept(&self, stream: tokio::net::TcpStream) -> Result<Self, TransportError> {
        match self {
            Self::Tcp(t) => Ok(Self::Tcp(tcp::TcpTransport::accept(
                stream,
                t.codec.clone(),
            ))),
            Self::Http(t) => Ok(Self::Http(
                HttpTransport::accept(stream, t.codec.clone()).await?,
            )),
            Self::WebSocket(t) => Ok(Self::WebSocket(
                WebSocketTransport::accept(stream, t.codec.clone()).await?,
            )),
        }
    }
}

impl<S, R> Transport<S, R> for TransportChoice
where
    S: MaybeSend + MaybeSync + 'static,
    R: MaybeSend + 'static,
    CodecChoice: Codec<S> + Codec<R>,
{
    async fn send(&self, msg: &S) -> Result<(), TransportError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Tcp(t) => Transport::<S, R>::send(t, msg).await,
            Self::Http(t) => Transport::<S, R>::send(t, msg).await,
            Self::WebSocket(t) => Transport::<S, R>::send(t, msg).await,
        }
    }

    async fn recv(&self) -> Result<R, TransportError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Tcp(t) => Transport::<S, R>::recv(t).await,
            Self::Http(t) => Transport::<S, R>::recv(t).await,
            Self::WebSocket(t) => Transport::<S, R>::recv(t).await,
        }
    }
}
