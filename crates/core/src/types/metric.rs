use serde::{Deserialize, Serialize};

use super::id::RunId;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
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

    /// Create a Metric from a string that was already validated.
    pub fn new_unchecked(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn leaf_name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or(&self.0)
    }
}

impl<'de> Deserialize<'de> for Metric {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Metric::new(s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MetricPoint {
    pub key_index: u32,
    pub value: f64,
    pub step: Step,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

/// Training step or iteration number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Step(u64);

impl Step {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for Step {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Step> for u64 {
    fn from(step: Step) -> Self {
        step.0
    }
}

impl std::ops::Add<u64> for Step {
    type Output = Self;
    fn add(self, rhs: u64) -> Self {
        Self(self.0 + rhs)
    }
}

impl std::ops::Sub for Step {
    type Output = u64;
    fn sub(self, rhs: Self) -> u64 {
        self.0 - rhs.0
    }
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Compact handle for an interned metric key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MetricKey(usize);

impl MetricKey {
    pub fn new(index: usize) -> Self {
        Self(index)
    }

    pub fn index(self) -> usize {
        self.0
    }
}

/// A single logged data point before batching.
#[derive(Clone, Copy, Debug)]
pub struct RawPoint {
    pub key: MetricKey,
    pub value: f64,
    pub step: Step,
    pub timestamp_ns: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric(name: &str) -> Result<Metric, MetricError> {
        Metric::new(name)
    }

    #[test]
    fn test_valid_metric_names() {
        let valid = [
            "loss",
            "train/loss",
            "eval.accuracy",
            "lr-schedule",
            "layer_0/grad_norm",
            "a",
            "ABC123",
            "train/epoch.2/loss-val",
        ];
        for name in valid {
            assert!(metric(name).is_ok(), "expected {name:?} to be valid");
        }
    }

    #[test]
    fn test_empty_metric_name_fails() {
        assert!(matches!(metric(""), Err(MetricError::Empty)));
    }

    #[test]
    fn test_metric_name_too_long_fails() {
        let long = "a".repeat(257);
        assert!(matches!(metric(&long), Err(MetricError::TooLong(257))));

        // exactly 256 should be fine
        let at_limit = "b".repeat(256);
        assert!(metric(&at_limit).is_ok());
    }

    #[test]
    fn test_invalid_characters_fail() {
        let invalid = ["has space", "emoji\u{1F600}", "colon:bad", "semi;col", "q?mark"];
        for name in invalid {
            assert!(
                matches!(metric(name), Err(MetricError::InvalidChar(_))),
                "expected {name:?} to fail with InvalidChar"
            );
        }
    }
}
