use std::sync::Arc;

use crate::domain::run::RunStatus;
use crate::types::bucket::Bucket;
use crate::types::id::RunId;
use crate::types::metric::{Metric, MetricBatch};

/// Events emitted by Photon.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PhotonEvent {
    /// A batch of metric points was decoded and will be persisted.
    BatchDecoded {
        run_id: RunId,
        batch: Arc<MetricBatch>,
    },

    /// A downsample bucket was closed during reduction.
    BucketsReduced {
        run_id: RunId,
        key: Metric,
        tier: usize,
        bucket: Bucket,
    },

    /// A run transitioned between states.
    RunStatusChanged {
        run_id: RunId,
        old: RunStatus,
        new: RunStatus,
    },

    /// All data for the run has been persisted and derived state is caught up.
    /// The run is safe to query at full fidelity and will receive no further writes.
    Finalized { run_id: RunId },
}
