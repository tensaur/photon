use std::future::Future;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("request failed: {0}")]
    Request(String),

    #[error("stream closed: {0}")]
    StreamClosed(String),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync> MaybeSync for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSync for T {}

/// Port for network communication, generic over message types.
pub trait Transport<S, R>: MaybeSend + MaybeSync + 'static
where
    S: MaybeSend + MaybeSync,
    R: MaybeSend,
{
    fn send(&self, msg: &S) -> impl Future<Output = Result<(), TransportError>> + MaybeSend;
    fn recv(&self) -> impl Future<Output = Result<R, TransportError>> + MaybeSend;
}
