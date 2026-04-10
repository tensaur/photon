use serde::{Deserialize, Serialize};

use crate::types::bucket::Bucket;
use crate::types::id::RunId;
use crate::types::id::SubscriptionId;
use crate::types::query::{DataPoint, MetricQuery, MetricSeries};

/// Updates that the server pushes for a ubscription.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscriptionUpdate {
    /// Full series snapshot. 
    Snapshot { series: MetricSeries },
    /// Incremental update appended to the existing series.
    Delta(DeltaData),
    /// Server has dropped the subscription.
    Unsubscribed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DeltaData {
    RawPoints(Vec<DataPoint>),
    Buckets(Vec<Bucket>),
}

/// Commands a client sends over the WS to manage subscriptions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscriptionCommand {
    Subscribe(MetricQuery),
    Unsubscribe(SubscriptionId),
}

/// One frame in the live stream of messages the server pushes to a client.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StreamFrame {
    Subscription {
        id: SubscriptionId,
        update: SubscriptionUpdate,
    },
    RunFinalised {
        run_id: RunId,
    },
    RunsChanged,
}
