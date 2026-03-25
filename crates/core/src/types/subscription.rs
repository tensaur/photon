use serde::{Deserialize, Serialize};

use crate::types::id::RunId;
use crate::types::metric::Metric;
use crate::types::query::DataPoint;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscriptionMessage {
    Subscribe(RunId),
    Unsubscribe(RunId),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscriptionEvent {
    LivePoints {
        run_id: RunId,
        metric: Metric,
        points: Vec<DataPoint>,
    },
    RunsChanged,
}
