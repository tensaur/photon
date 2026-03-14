use super::id::RunId;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Metric(String);

#[derive(Debug, Clone, thiserror::Error)]
pub enum MetricError {
    #[error("metric key cannot be empty")]
    Empty,
    #[error("metric key exceeds max length of 256 (got {0})")]
    TooLong(usize),
    #[error("metric key contains invalid character: {0:?}")]
    InvalidChar(char),
}

impl Metric {
    pub fn new(key: impl Into<String>) -> Result<Self, MetricError> {
        let key = key.into();

        if key.is_empty() {
            return Err(MetricError::Empty);
        }

        if key.len() > 256 {
            return Err(MetricError::TooLong(key.len()));
        }

        if let Some(c) = key
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/')))
        {
            return Err(MetricError::InvalidChar(c));
        }

        Ok(Self(key))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MetricPoint {
    pub key_index: u32,
    pub value: f64,
    pub step: u64,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug)]
pub struct MetricBatch {
    pub run_id: RunId,
    pub keys: Vec<Metric>,
    pub points: Vec<MetricPoint>,
}

impl MetricBatch {
    pub fn key(&self, point: &MetricPoint) -> &Metric {
        &self.keys[point.key_index as usize]
    }

    pub fn len(&self) -> usize {
        self.points.len()
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }
}
