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
    /// Incremental raw data points appended to the existing series.
    DeltaPoints(Vec<DataPoint>),
    /// Incremental aggregated buckets appended to the existing series.
    DeltaBuckets(Vec<Bucket>),
    /// Server has dropped the subscription.
    Unsubscribed,
}

/// Commands a client sends over the WS to manage subscriptions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscriptionMessage {
    Subscribe(MetricQuery),
    Unsubscribe(SubscriptionId),
}

/// One frame in the live stream of messages the server pushes to a client.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StreamMessage {
    Subscription {
        id: SubscriptionId,
        update: SubscriptionUpdate,
    },
    RunFinalised {
        run_id: RunId,
    },
    RunsChanged,
}
