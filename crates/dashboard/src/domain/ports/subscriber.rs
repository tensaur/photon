use std::future::Future;

use photon_core::types::query::{MetricQuery, SubscriptionId};

use crate::domain::error::{SubscribeError, UnsubscribeError};

#[cfg(not(target_arch = "wasm32"))]
pub trait MetricSubscriber: Clone + Send + Sync + 'static {
    fn subscribe_metric(
        &self,
        query: &MetricQuery,
    ) -> impl Future<Output = Result<(), SubscribeError>> + Send;
    fn unsubscribe_metric(
        &self,
        sub_id: SubscriptionId,
    ) -> impl Future<Output = Result<(), UnsubscribeError>> + Send;
}

#[cfg(target_arch = "wasm32")]
pub trait MetricSubscriber: Clone + 'static {
    fn subscribe_metric(
        &self,
        query: &MetricQuery,
    ) -> impl Future<Output = Result<(), SubscribeError>>;
    fn unsubscribe_metric(
        &self,
        sub_id: SubscriptionId,
    ) -> impl Future<Output = Result<(), UnsubscribeError>>;
}
