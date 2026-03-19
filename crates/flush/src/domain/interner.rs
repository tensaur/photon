use std::sync::RwLock;

use dashmap::DashMap;

use photon_core::types::metric::{Metric, MetricError};

/// Compact handle for an interned metric key
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MetricKey(u32);

/// Thread-safe bidirectional map between string keys and a MetricKey
pub struct MetricKeyInterner {
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

    pub(crate) fn resolve(&self, handle: MetricKey) -> Metric {
        let reverse = self.reverse.read().unwrap();
        reverse[handle.0 as usize].clone()
    }
}
