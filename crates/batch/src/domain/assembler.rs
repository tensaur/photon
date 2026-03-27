use std::collections::HashMap;
use std::sync::Arc;

use lasso::ThreadedRodeo;

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, MetricKey, MetricPoint, RawPoint};

pub(crate) struct BatchAssembler {
    interner: Arc<ThreadedRodeo>,
    key_map: HashMap<MetricKey, u32>,
    keys: Vec<Metric>,
    points: Vec<MetricPoint>,
}

impl BatchAssembler {
    pub(crate) fn new(interner: Arc<ThreadedRodeo>) -> Self {
        Self {
            interner,
            key_map: HashMap::new(),
            keys: Vec::new(),
            points: Vec::new(),
        }
    }

    pub(crate) fn assemble(&mut self, run_id: RunId, pending: &[RawPoint]) -> MetricBatch {
        self.key_map.clear();
        self.keys.clear();
        self.points.clear();

        for p in pending {
            let key_index = *self.key_map.entry(p.key).or_insert_with(|| {
                let idx = self.keys.len() as u32;
                let spur = lasso::Key::try_from_usize(p.key.index()).unwrap();
                let s = self.interner.resolve(&spur);
                // Key was validated on intern
                self.keys.push(Metric::new_unchecked(s));
                idx
            });
            self.points.push(MetricPoint {
                key_index,
                value: p.value,
                step: p.step,
                timestamp_ms: p.timestamp_ns / 1_000_000,
            });
        }

        MetricBatch {
            run_id,
            keys: std::mem::take(&mut self.keys),
            points: std::mem::take(&mut self.points),
        }
    }

    pub(crate) fn reclaim(&mut self, batch: MetricBatch) {
        self.keys = batch.keys;
        self.points = batch.points;
    }
}
