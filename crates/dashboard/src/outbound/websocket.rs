use photon_core::types::id::SubscriptionId;
use photon_core::types::query::{MetricQuery, QueryMessage};
use photon_core::types::stream::StreamFrame;
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
    T: Transport<QueryMessage, StreamFrame>,
{
    async fn subscribe_metric(&self, query: &MetricQuery) -> Result<(), SubscribeError> {
        let mut q = query.clone();
        q.subscribe = true;
        self.transport
            .send(&QueryMessage::Query(q))
            .await
            .map_err(|e| SubscribeError::Unknown(Box::new(e)))
    }

    async fn unsubscribe_metric(&self, sub_id: SubscriptionId) -> Result<(), UnsubscribeError> {
        self.transport
            .send(&QueryMessage::Unsubscribe(sub_id))
            .await
            .map_err(|e| UnsubscribeError::Unknown(Box::new(e)))
    }
}
