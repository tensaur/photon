pub mod http;
pub mod ports;
pub mod tcp;

use photon_protocol::codec::CodecChoice;
use photon_protocol::ports::codec::Codec;

use self::http::HttpTransport;
use self::ports::{Transport, TransportError};
use self::tcp::TcpTransport;

/// Transport selection for builders. Analogous to `CodecChoice` and `CompressorChoice`.
#[derive(Clone, Debug, Default)]
pub enum TransportChoice {
    #[default]
    Tcp,
    Http { url: String },
}

impl TransportChoice {
    pub fn tcp() -> Self {
        Self::Tcp
    }

    pub fn http(url: impl Into<String>) -> Self {
        Self::Http { url: url.into() }
    }

    /// Connect using the selected transport and codec.
    pub async fn connect(
        &self,
        endpoint: &str,
        codec: CodecChoice,
    ) -> Result<ConnectedTransport, TransportError> {
        match self {
            Self::Tcp => {
                let t = TcpTransport::connect(endpoint, codec).await?;
                Ok(ConnectedTransport::Tcp(t))
            }
            Self::Http { url } => {
                let t = HttpTransport::new(url, codec);
                Ok(ConnectedTransport::Http(t))
            }
        }
    }
}

/// A connected transport. Implements `Transport<S, R>` by dispatching
/// to the concrete adapter inside.
#[derive(Clone)]
pub enum ConnectedTransport {
    Tcp(TcpTransport<CodecChoice>),
    Http(HttpTransport<CodecChoice>),
}

impl<S, R> Transport<S, R> for ConnectedTransport
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
