use std::time::Duration;

use super::sequence::SequenceNumber;

/// Configuration for the batch builder's flush behaviour.
#[derive(Clone, Debug)]
pub struct BatchConfig {
    pub max_points: usize,
    pub max_bytes: usize,
    pub flush_interval: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_points: 10_000,
            max_bytes: 1_048_576, // 1MB
            flush_interval: Duration::from_millis(100),
        }
    }
}

/// Configuration for the sender's in-flight window and retry behaviour.
#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub max_in_flight: usize,
    pub max_streams: usize,
    pub retry: RetryConfig,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 64,
            max_streams: 2,
            retry: RetryConfig::default(),
        }
    }
}

/// Exponential backoff with jitter for transient failures.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f64,
    pub jitter: f64,
    pub max_attempts: Option<u32>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: 0.2,
            max_attempts: None,
        }
    }
}

/// Controls how aggressively the WAL fsyncs to disk.
#[derive(Clone, Debug)]
pub enum WalSyncPolicy {
    /// fsync after every batch write. Maximum durability, ~10% throughput cost.
    EveryBatch,
    /// fsync every N batches or every T, whichever comes first.
    /// A crash may lose at most N batches or T of data.
    Periodic { batches: u32, interval: Duration },
    /// Let the OS decide. Maximum throughput, weakest guarantee.
    /// Survives process crashes but not power loss.
    OsManaged,
}

impl Default for WalSyncPolicy {
    fn default() -> Self {
        Self::Periodic {
            batches: 10,
            interval: Duration::from_millis(500),
        }
    }
}

/// Configuration for the WAL storage backend.
#[derive(Clone, Debug)]
pub struct WalConfig {
    pub segment_size: u64,
    pub max_total_size: Option<u64>,
    pub sync_policy: WalSyncPolicy,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size: 64 * 1024 * 1024,               // 64MB
            max_total_size: Some(2 * 1024 * 1024 * 1024), // 2GB
            sync_policy: WalSyncPolicy::default(),
        }
    }
}

/// Persisted WAL state. Read on startup, written periodically and on
/// segment deletion. This is the recovery starting point after a crash.
#[derive(Clone, Debug)]
pub struct WalMeta {
    pub committed_sequence: SequenceNumber,
}

/// Shutdown behaviour for the pipeline.
#[derive(Clone, Debug)]
pub struct ShutdownConfig {
    pub timeout: Duration,
    pub drop_timeout: Duration,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            drop_timeout: Duration::from_secs(5),
        }
    }
}
