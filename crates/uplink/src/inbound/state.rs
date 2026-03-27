use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use photon_core::types::config::{RetryConfig, UplinkConfig};
use photon_core::types::sequence::SequenceNumber;
use photon_core::types::wal::WalOffset;
use photon_wal::Wal;

use crate::domain::ports::IngestConnection;

#[derive(Clone, Debug)]
struct InFlightEntry {
    sent_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    Connected,
    Reconnecting { attempt: u32, next_at: Instant },
    Shutdown,
}

pub(crate) struct ConnectionState {
    phase: Phase,
    in_flight: BTreeMap<SequenceNumber, InFlightEntry>,
    draining_since: Option<Instant>,
    max_in_flight: usize,
    in_flight_timeout: Duration,
    shutdown_timeout: Duration,
    retry: RetryConfig,
}

impl ConnectionState {
    pub(crate) fn new(config: &UplinkConfig) -> Self {
        Self {
            phase: Phase::Connected,
            in_flight: BTreeMap::new(),
            draining_since: None,
            max_in_flight: config.max_in_flight,
            in_flight_timeout: config.in_flight_timeout,
            shutdown_timeout: config.shutdown_timeout,
            retry: config.retry.clone(),
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.phase == Phase::Shutdown
    }

    pub(crate) fn can_send(&self) -> bool {
        self.phase == Phase::Connected && self.in_flight.len() < self.max_in_flight
    }

    pub(crate) fn in_flight_empty(&self) -> bool {
        self.in_flight.is_empty()
    }

    pub(crate) fn record_sent(&mut self, seq: SequenceNumber) {
        self.in_flight.insert(
            seq,
            InFlightEntry {
                sent_at: Instant::now(),
            },
        );
    }

    pub(crate) fn record_acked(&mut self, seq: SequenceNumber) {
        self.in_flight.remove(&seq);
    }

    pub(crate) fn enter_reconnecting(&mut self) {
        tracing::warn!(
            in_flight = self.in_flight.len(),
            "connection lost, entering reconnection"
        );
        self.phase = Phase::Reconnecting {
            attempt: 0,
            next_at: Instant::now(),
        };
    }

    pub(crate) fn reconnected(&mut self) {
        tracing::info!(
            "reconnected, replaying {} in-flight batches",
            self.in_flight.len()
        );
        self.in_flight.clear();
        self.phase = Phase::Connected;
    }

    pub(crate) fn reconnect_due(&self) -> Option<u32> {
        match &self.phase {
            Phase::Reconnecting { attempt, next_at } if Instant::now() >= *next_at => {
                Some(*attempt)
            }
            _ => None,
        }
    }

    pub(crate) fn schedule_next_reconnect(&mut self, attempt: u32) {
        let delay = backoff_delay(&self.retry, attempt);
        tracing::info!(
            attempt = attempt + 1,
            delay_ms = delay.as_millis(),
            "scheduling reconnection attempt"
        );
        self.phase = Phase::Reconnecting {
            attempt: attempt + 1,
            next_at: Instant::now() + delay,
        };
    }

    pub(crate) fn check_timeouts(&mut self) -> Option<u64> {
        if self.in_flight.is_empty() {
            return None;
        }
        let oldest = self.in_flight.values().next().unwrap();
        if oldest.sent_at.elapsed() > self.in_flight_timeout {
            tracing::warn!(
                elapsed_ms = oldest.sent_at.elapsed().as_millis(),
                in_flight = self.in_flight.len(),
                "in-flight batch timed out, reconnecting"
            );
            let retried = self.in_flight.len() as u64;
            self.enter_reconnecting();
            Some(retried)
        } else {
            None
        }
    }

    pub(crate) fn shutdown(&mut self) {
        self.phase = Phase::Shutdown;
    }

    pub(crate) fn check_drain_timeout(&mut self) -> Option<Duration> {
        let since = *self.draining_since.get_or_insert(Instant::now());
        if since.elapsed() > self.shutdown_timeout {
            Some(since.elapsed())
        } else {
            None
        }
    }

    pub(crate) fn reset_drain(&mut self) {
        self.draining_since = None;
    }
}

pub(crate) enum ReconnectResult {
    Ok,
    Failed,
}

pub(crate) async fn try_reconnect<M: Wal + Clone, C: IngestConnection>(
    wal: &M,
    connection: &C,
    wal_cursor: WalOffset,
) -> ReconnectResult {
    let batches = match wal.read_from(wal_cursor) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("WAL read during reconnection failed: {e}");
            return ReconnectResult::Failed;
        }
    };

    if let Some(batch) = batches.first() {
        match connection.send_batch(batch).await {
            Ok(()) => return ReconnectResult::Ok,
            Err(_) => return ReconnectResult::Failed,
        }
    }

    ReconnectResult::Ok
}

fn backoff_delay(config: &RetryConfig, attempt: u32) -> Duration {
    let base = config.base_delay.as_secs_f64();
    let delay = base * config.multiplier.powi(attempt as i32);
    let capped = delay.min(config.max_delay.as_secs_f64());
    let jitter_range = capped * config.jitter;
    let jittered = capped + jitter_range * (2.0 * rand() - 1.0);
    Duration::from_secs_f64(jittered.max(0.0))
}

fn rand() -> f64 {
    let mut hasher = DefaultHasher::new();
    Instant::now().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    (hasher.finish() % 10_000) as f64 / 10_000.0
}
