pub mod adapter;
pub mod codec;
pub mod ports;

pub use adapter::TransportKind;
pub use adapter::http;
#[cfg(not(target_arch = "wasm32"))]
pub use adapter::router;
#[cfg(not(target_arch = "wasm32"))]
pub use adapter::tcp;
pub use adapter::websocket;
pub use ports::{ByteTransport, Transport};

#[cfg(not(target_arch = "wasm32"))]
use std::future::Future;

#[cfg(not(target_arch = "wasm32"))]
use photon_protocol::codec::CodecKind;
#[cfg(not(target_arch = "wasm32"))]
use tokio::net::TcpListener;
#[cfg(not(target_arch = "wasm32"))]
use tokio_util::sync::CancellationToken;

#[cfg(not(target_arch = "wasm32"))]
use crate::adapter::tcp::TcpTransport;
#[cfg(not(target_arch = "wasm32"))]
use crate::codec::CodecTransport;

/// Accept TCP connections, wrap each in a `CodecTransport`, and dispatch to a handler.
#[cfg(not(target_arch = "wasm32"))]
pub async fn serve<F, Fut>(
    listener: TcpListener,
    codec: CodecKind,
    cancel: CancellationToken,
    handler: F,
) where
    F: Fn(CodecTransport<CodecKind, TcpTransport>) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = ()> + Send,
{
    loop {
        let conn = tokio::select! {
            _ = cancel.cancelled() => break,
            res = listener.accept() => res,
        };

        let (stream, peer) = match conn {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!("accept error: {e}");
                continue;
            }
        };

        let handler = handler.clone();

        tokio::spawn(async move {
            tracing::trace!("accepted connection from {peer}");
            let transport = CodecTransport::new(codec, TcpTransport::accept(stream));
            handler(transport).await;
        });
    }
}
