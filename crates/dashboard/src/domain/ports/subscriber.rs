use std::future::Future;

use photon_core::types::id::RunId;

use crate::domain::error::{SubscribeError, UnsubscribeError};

#[cfg(not(target_arch = "wasm32"))]
pub trait MetricSubscriber: Clone + Send + Sync + 'static {
    fn subscribe(&self, run_id: &RunId) -> impl Future<Output = Result<(), SubscribeError>> + Send;
    fn unsubscribe(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<(), UnsubscribeError>> + Send;
}

#[cfg(target_arch = "wasm32")]
pub trait MetricSubscriber: Clone + 'static {
    fn subscribe(&self, run_id: &RunId) -> impl Future<Output = Result<(), SubscribeError>>;
    fn unsubscribe(&self, run_id: &RunId) -> impl Future<Output = Result<(), UnsubscribeError>>;
}
