pub mod http;
pub mod ports;
pub mod tcp;

use photon_protocol::codec::CodecChoice;
use photon_protocol::ports::codec::Codec;

use self::http::HttpTransport;
use self::ports::{Transport, TransportError};
use self::tcp::TcpTransport;

/// Runtime transport selection.
#[derive(Clone)]
pub enum TransportChoice {
    Tcp(TcpTransport<CodecChoice>),
    Http(HttpTransport<CodecChoice>),
}

impl Default for TransportChoice {
    fn default() -> Self {
        Self::Tcp(TcpTransport::new(CodecChoice::default()))
    }
}

impl TransportChoice {
    pub fn tcp(codec: CodecChoice) -> Self {
        Self::Tcp(TcpTransport::new(codec))
    }

    pub fn http(url: impl Into<String>, codec: CodecChoice) -> Self {
        Self::Http(HttpTransport::new(url, codec))
    }

    /// Establish the connection. Call before `send()`/`recv()`.
    pub async fn connect(&self, endpoint: &str) -> Result<(), TransportError> {
        match self {
            Self::Tcp(t) => t.connect(endpoint).await,
            Self::Http(_) => Ok(()),
        }
    }
}

impl<S, R> Transport<S, R> for TransportChoice
where
    S: Send + Sync + 'static,
    R: Send + 'static,
    CodecChoice: Codec<S> + Codec<R>,
{
    async fn send(&self, msg: &S) -> Result<(), TransportError> {
        match self {
            Self::Tcp(t) => Transport::<S, R>::send(t, msg).await,
            Self::Http(t) => Transport::<S, R>::send(t, msg).await,
        }
    }

    async fn recv(&self) -> Result<R, TransportError> {
        match self {
            Self::Tcp(t) => Transport::<S, R>::recv(t).await,
            Self::Http(t) => Transport::<S, R>::recv(t).await,
        }
    }
}
