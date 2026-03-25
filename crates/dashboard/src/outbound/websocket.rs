use photon_core::types::id::RunId;
use photon_core::types::subscription::{SubscriptionEvent, SubscriptionMessage};
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
    T: Transport<SubscriptionMessage, SubscriptionEvent>,
{
    async fn subscribe(&self, run_id: &RunId) -> Result<(), SubscribeError> {
        self.transport
            .send(&SubscriptionMessage::Subscribe(*run_id))
            .await
            .map_err(|e| SubscribeError::Unknown(Box::new(e)))
    }

    async fn unsubscribe(&self, run_id: &RunId) -> Result<(), UnsubscribeError> {
        self.transport
            .send(&SubscriptionMessage::Unsubscribe(*run_id))
            .await
            .map_err(|e| UnsubscribeError::Unknown(Box::new(e)))
    }
}
