use serde::{Deserialize, Serialize};

use crate::types::bucket::Bucket;
use crate::types::id::RunId;
use crate::types::id::SubscriptionId;
use crate::types::query::{DataPoint, MetricSeries};

/// Updates that the server pushes for a *specific* subscription. The owning
/// `SubscriptionId` is carried by the `StreamFrame::Subscription` wrapper,
/// not by individual variants — the relationship is structural, not by
/// convention.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SubscriptionUpdate {
    /// Initial full series sent immediately after the subscription is opened.
    Snapshot { series: MetricSeries },
    /// Incremental update appended to the existing series.
    Delta(DeltaData),
    /// LOD switched mid-stream — replace the cached series with this one.
    Resnapshot { series: MetricSeries },
    /// Server has dropped the subscription.
    Unsubscribed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DeltaData {
    RawPoints(Vec<DataPoint>),
    Buckets(Vec<Bucket>),
}

/// One frame in the live stream of messages the server pushes to a client.
///
/// Two scopes:
///   * `Subscription { id, update }` — update for a specific subscription.
///   * Top-level events (`RunFinalized`, `RunsChanged`) — connection-broadcast
///     notifications not tied to any subscription.
///
/// One-shot query results are not part of this stream — they travel over the
/// `/api/query` HTTP endpoint and are represented by `QueryResult` in
/// `query.rs`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StreamFrame {
    Subscription {
        id: SubscriptionId,
        update: SubscriptionUpdate,
    },
    RunFinalized {
        run_id: RunId,
    },
    RunsChanged,
}
