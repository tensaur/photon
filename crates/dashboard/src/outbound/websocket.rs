use photon_core::types::id::SubscriptionId;
use photon_core::types::query::MetricQuery;
use photon_core::types::stream::{StreamFrame, SubscriptionCommand};
use photon_transport::Transport;

use crate::domain::error::{SubscribeError, UnsubscribeError};
use crate::domain::ports::MetricSubscriber;

#[derive(Clone)]
pub struct WsSubscriber<T> {
    transport: T,
}

impl<T> WsSubscriber<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }
}

impl<T> MetricSubscriber for WsSubscriber<T>
where
    T: Transport<SubscriptionCommand, StreamFrame>,
{
    async fn subscribe(&self, query: &MetricQuery) -> Result<(), SubscribeError> {
        self.transport
            .send(&SubscriptionCommand::Subscribe(query.clone()))
            .await
            .map_err(|e| SubscribeError::Unknown(Box::new(e)))
    }

    async fn unsubscribe(&self, sub_id: SubscriptionId) -> Result<(), UnsubscribeError> {
        self.transport
            .send(&SubscriptionCommand::Unsubscribe(sub_id))
            .await
            .map_err(|e| UnsubscribeError::Unknown(Box::new(e)))
    }
}
