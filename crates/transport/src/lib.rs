pub mod adapter;
pub mod codec;
pub mod ports;

pub use adapter::TransportKind;
pub use adapter::http;
#[cfg(not(target_arch = "wasm32"))]
pub use adapter::tcp;
pub use adapter::websocket;
pub use ports::{ByteTransport, Transport};

#[cfg(not(target_arch = "wasm32"))]
use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

/// Accept connections and dispatch each to a handler.
#[cfg(not(target_arch = "wasm32"))]
pub async fn serve<S, F, Fut>(listener: tokio::net::TcpListener, service: Arc<S>, on_connection: F)
where
    S: Send + Sync + 'static,
    F: Fn(Arc<S>, tokio::net::TcpStream) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = ()> + Send,
{
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!("accept error: {e}");
                continue;
            }
        };

        let service = service.clone();
        let on_connection = on_connection.clone();

        tokio::spawn(async move {
            tracing::trace!("accepted connection from {peer}");
            on_connection(service, stream).await;
        });
    }
}
