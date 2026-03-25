use photon_core::types::id::RunId;

use crate::domain::error::{SubscribeError, UnsubscribeError};
use crate::domain::ports::MetricSubscriber;

#[derive(Clone)]
pub struct WsSubscriber;

impl WsSubscriber {
    pub fn new() -> Self {
        Self
    }
}

impl MetricSubscriber for WsSubscriber {
    async fn subscribe(&self, _run_id: &RunId) -> Result<(), SubscribeError> {
        Ok(())
    }

    async fn unsubscribe(&self, _run_id: &RunId) -> Result<(), UnsubscribeError> {
        Ok(())
    }
}
