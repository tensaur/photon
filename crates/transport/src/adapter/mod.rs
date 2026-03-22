pub mod http;
pub mod websocket;

#[cfg(not(target_arch = "wasm32"))]
pub mod tcp;

#[cfg(not(target_arch = "wasm32"))]
use self::tcp::TcpTransport;
use self::http::HttpTransport;
use self::websocket::WebSocketTransport;
use crate::ports::{ByteTransport, TransportError};

/// Transport protocol selection. Call [`connect`](Self::connect) to create a connected transport.
#[derive(Clone, Copy, Debug)]
pub enum TransportKind {
    #[cfg(not(target_arch = "wasm32"))]
    Tcp,
    Http,
    WebSocket,
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for TransportKind {
    fn default() -> Self {
        Self::Tcp
    }
}

#[cfg(target_arch = "wasm32")]
impl Default for TransportKind {
    fn default() -> Self {
        Self::Http
    }
}

impl TransportKind {
    pub async fn connect(self, endpoint: &str) -> Result<Box<dyn ByteTransport>, TransportError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Tcp => Ok(Box::new(TcpTransport::connect(endpoint).await?)),
            Self::Http => Ok(Box::new(HttpTransport::connect(endpoint))),
            Self::WebSocket => Ok(Box::new(WebSocketTransport::connect(endpoint).await?)),
        }
    }
}
