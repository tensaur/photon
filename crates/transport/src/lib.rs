pub mod adapter;
pub mod ports;

pub use adapter::TransportChoice;
pub use adapter::http;
#[cfg(not(target_arch = "wasm32"))]
pub use adapter::tcp;
pub use adapter::websocket;
pub use ports::{MaybeSend, MaybeSync};

/// Accept connections and dispatch each to a handler.
#[cfg(not(target_arch = "wasm32"))]
pub async fn serve<S, H, Fut>(
    listener: tokio::net::TcpListener,
    transport: TransportChoice,
    service: std::sync::Arc<S>,
    handler: H,
) where
    S: Send + Sync + 'static,
    H: Fn(std::sync::Arc<S>, TransportChoice) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!("accept error: {e}");
                continue;
            }
        };

        let transport = transport.clone();
        let service = service.clone();
        let handler = handler.clone();

        tokio::spawn(async move {
            match transport.accept(stream).await {
                Ok(t) => handler(service, t).await,
                Err(e) => tracing::warn!("transport error from {peer}: {e}"),
            }
        });
    }
}
