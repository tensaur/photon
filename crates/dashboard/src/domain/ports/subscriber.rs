use std::future::Future;

use photon_core::types::id::SubscriptionId;
use photon_core::types::query::MetricQuery;

use crate::domain::error::{SubscribeError, UnsubscribeError};

#[cfg(not(target_arch = "wasm32"))]
pub trait MetricSubscriber: Clone + Send + Sync + 'static {
    fn subscribe(
        &self,
        query: &MetricQuery,
    ) -> impl Future<Output = Result<(), SubscribeError>> + Send;
    fn unsubscribe(
        &self,
        sub_id: SubscriptionId,
    ) -> impl Future<Output = Result<(), UnsubscribeError>> + Send;
}

#[cfg(target_arch = "wasm32")]
pub trait MetricSubscriber: Clone + 'static {
    fn subscribe(
        &self,
        query: &MetricQuery,
    ) -> impl Future<Output = Result<(), SubscribeError>>;
    fn unsubscribe(
        &self,
        sub_id: SubscriptionId,
    ) -> impl Future<Output = Result<(), UnsubscribeError>>;
}
