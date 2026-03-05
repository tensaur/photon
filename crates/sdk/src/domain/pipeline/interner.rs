use std::sync::{Arc, RwLock};
use std::time::{Duration, UNIX_EPOCH};

use dashmap::DashMap;

use photon_core::types::metric::{Metric, MetricError, MetricPoint};

use crate::domain::ports::resolver::PointResolver;

/// Compact handle for an interned metric key
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MetricKey(u32);

#[derive(Clone, Copy, Debug)]
pub(crate) struct RawPoint {
    pub key: MetricKey,
    pub value: f64,
    pub step: u64,
    pub timestamp_ns: u64,
}

/// Thread-safe bidirectional map between string keys and a MetricKey
pub(crate) struct MetricKeyInterner {
    forward: DashMap<String, MetricKey>,
    reverse: RwLock<Vec<Metric>>,
}

impl MetricKeyInterner {
    pub fn new() -> Self {
        Self {
            forward: DashMap::new(),
            reverse: RwLock::new(Vec::new()),
        }
    }

    pub fn get_or_intern(&self, key: &str) -> Result<MetricKey, MetricError> {
        if let Some(entry) = self.forward.get(key) {
            return Ok(*entry.value());
        }

        let metric = Metric::new(key)?;

        let mut reverse = self.reverse.write().unwrap();

        // Double-check after acquiring the write lock
        if let Some(entry) = self.forward.get(key) {
            return Ok(*entry.value());
        }

        let handle = MetricKey(reverse.len() as u32);
        reverse.push(metric);
        self.forward.insert(key.to_owned(), handle);

        Ok(handle)
    }

    fn resolve_handle(&self, handle: MetricKey) -> Metric {
        let reverse = self.reverse.read().unwrap();
        reverse[handle.0 as usize].clone()
    }
}

/// Converts a RawPoint into a MetricPoint
pub(crate) struct InternResolver {
    interner: Arc<MetricKeyInterner>,
}

impl InternResolver {
    pub fn new(interner: Arc<MetricKeyInterner>) -> Self {
        Self { interner }
    }
}

impl PointResolver for InternResolver {
    type Point = RawPoint;

    fn resolve(&self, points: &[RawPoint]) -> Vec<MetricPoint> {
        points
            .iter()
            .map(|raw| MetricPoint {
                key: self.interner.resolve_handle(raw.key),
                value: raw.value,
                step: raw.step,
                timestamp: UNIX_EPOCH + Duration::from_nanos(raw.timestamp_ns),
            })
            .collect()
    }
}
