use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use photon_core::types::ack::{AckResult, AckStatus};
use photon_core::types::config::{RetryConfig, SenderConfig};
use photon_core::types::sequence::SequenceNumber;

use crate::domain::ports::transport::{BatchTransport, TransportError};
use crate::domain::ports::wal::{WalError, WalStorage};

pub struct Sender<T, W>
where
    T: BatchTransport,
    W: WalStorage,
{
    transport: T,
    wal: W,
    config: SenderConfig,
    state: SenderState,
    in_flight: BTreeMap<SequenceNumber, InFlightEntry>,
    last_sent: SequenceNumber,
    stats: SenderStats,
    shutdown_rx: oneshot::Receiver<()>,
    draining_since: Option<Instant>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SenderState {
    Connected,
    Reconnecting { attempt: u32, next_at: Instant },
    Shutdown,
}

#[derive(Clone, Debug)]
pub struct InFlightEntry {
    pub sequence_number: SequenceNumber,
    pub sent_at: Instant,
    pub attempt: u32,
}

#[derive(Clone, Debug, Default)]
pub struct SenderStats {
    pub batches_sent: u64,
    pub batches_acked: u64,
    pub batches_retried: u64,
    pub duplicates_received: u64,
    pub rejections_received: u64,
    pub watermark_advances: u64,
    pub segments_truncated: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum SenderError {
    #[error("WAL read failed")]
    Wal(#[from] WalError),

    #[error("transport error")]
    Transport(#[from] TransportError),

    #[error("shutdown timeout after {0:?}")]
    ShutdownTimeout(Duration),
}

impl<T, W> Sender<T, W>
where
    T: BatchTransport,
    W: WalStorage,
{
    pub fn new(
        transport: T,
        wal: W,
        config: SenderConfig,
        start_after: SequenceNumber,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            transport,
            wal,
            config,
            state: SenderState::Connected,
            in_flight: BTreeMap::new(),
            last_sent: start_after,
            stats: SenderStats::default(),
            shutdown_rx,
            draining_since: None,
        }
    }

    pub fn state(&self) -> &SenderState {
        &self.state
    }

    pub fn stats(&self) -> &SenderStats {
        &self.stats
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    pub async fn run(
        &mut self,
        mut ack_callback: impl FnMut(SequenceNumber),
    ) -> Result<SenderStats, SenderError> {
        loop {
            match &self.state {
                SenderState::Shutdown => {
                    tracing::info!(
                        state = ?self.state(),
                        sent = self.stats().batches_sent,
                        acked = self.stats().batches_acked,
                        retried = self.stats().batches_retried,
                        in_flight = self.in_flight_count(),
                        "sender shutting down"
                    );

                    return Ok(self.stats.clone());
                }

                SenderState::Reconnecting { attempt, next_at } => {
                    let attempt = *attempt;
                    let next_at = *next_at;
                    tokio::time::sleep_until(next_at.into()).await;
                    self.try_reconnect(attempt).await;
                }

                SenderState::Connected => {
                    let sent = self.send_next().await?;
                    let acked = self.recv_and_handle_ack(&mut ack_callback).await?;

                    self.check_in_flight_timeouts();

                    if !sent && !acked {
                        let alive = matches!(
                            self.shutdown_rx.try_recv(),
                            Err(oneshot::error::TryRecvError::Empty)
                        );

                        if alive {
                            self.draining_since = None;

                            tokio::time::sleep(self.config.idle_poll_interval).await;
                        } else if self.in_flight.is_empty() {
                            self.state = SenderState::Shutdown;
                        } else {
                            let since = *self.draining_since.get_or_insert(Instant::now());

                            if since.elapsed() > self.config.shutdown_timeout {
                                return Err(SenderError::ShutdownTimeout(since.elapsed()));
                            }

                            tokio::time::sleep(self.config.drain_poll_interval).await;
                        }
                    }
                }
            }
        }
    }

    async fn send_next(&mut self) -> Result<bool, SenderError> {
        if self.in_flight.len() >= self.config.max_in_flight {
            return Ok(false);
        }

        let batches = self.wal.read_from(self.last_sent)?;
        let batch = match batches.first() {
            Some(b) => b,
            None => return Ok(false),
        };

        match self.transport.send(batch).await {
            Ok(()) => {
                self.in_flight.insert(
                    batch.sequence_number,
                    InFlightEntry {
                        sequence_number: batch.sequence_number,
                        sent_at: Instant::now(),
                        attempt: 1,
                    },
                );

                self.last_sent = batch.sequence_number;
                self.stats.batches_sent += 1;

                Ok(true)
            }
            Err(TransportError::ConnectionLost { .. }) => {
                self.enter_reconnecting();
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn recv_and_handle_ack(
        &mut self,
        ack_callback: &mut impl FnMut(SequenceNumber),
    ) -> Result<bool, SenderError> {
        match self.try_recv_ack().await {
            Ok(Some(seq)) => {
                ack_callback(seq);
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(TransportError::ConnectionLost { .. }) => {
                self.enter_reconnecting();
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn check_in_flight_timeouts(&mut self) {
        if self.in_flight.is_empty() {
            return;
        }

        // BTreeMap ordered by sequence so first entry is oldest
        let oldest = self.in_flight.values().next().unwrap();
        let elapsed = oldest.sent_at.elapsed();

        if elapsed > self.config.in_flight_timeout {
            tracing::warn!(
                seq = u64::from(oldest.sequence_number),
                attempt = oldest.attempt,
                elapsed_ms = elapsed.as_millis(),
                in_flight = self.in_flight.len(),
                "in-flight batch timed out, reconnecting"
            );

            self.stats.batches_retried += self.in_flight.len() as u64;
            self.enter_reconnecting();
        }
    }

    async fn try_recv_ack(&mut self) -> Result<Option<SequenceNumber>, TransportError> {
        let ack = tokio::time::timeout(Duration::from_millis(10), self.transport.recv_ack()).await;

        match ack {
            Err(_) => Ok(None),
            Ok(Ok(ack)) => Ok(Some(self.handle_ack(ack))),
            Ok(Err(e)) => Err(e),
        }
    }

    fn handle_ack(&mut self, ack: AckResult) -> SequenceNumber {
        let seq = ack.sequence_number;
        self.in_flight.remove(&seq);

        match ack.status {
            AckStatus::Ok => {
                self.stats.batches_acked += 1;
            }
            AckStatus::Duplicate => {
                self.stats.batches_acked += 1;
                self.stats.duplicates_received += 1;
            }
            AckStatus::Rejected => {
                self.stats.rejections_received += 1;
                tracing::warn!(
                    sequence = u64::from(seq),
                    "batch permanently rejected by server, advancing past it"
                );
            }
        }

        seq
    }

    fn enter_reconnecting(&mut self) {
        tracing::warn!(
            in_flight = self.in_flight.len(),
            "connection lost, entering reconnection"
        );
        self.state = SenderState::Reconnecting {
            attempt: 0,
            next_at: Instant::now(),
        };
    }

    async fn try_reconnect(&mut self, attempt: u32) {
        let oldest = self.in_flight.keys().next().copied();

        if let Some(seq) = oldest {
            let batches = match self.wal.read_from(seq.prev_or_zero()) {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!("WAL read during reconnection failed: {e}");
                    self.schedule_next_reconnect(attempt);
                    return;
                }
            };

            if let Some(batch) = batches.first() {
                match self.transport.send(batch).await {
                    Ok(()) => {
                        tracing::info!(
                            "reconnected, replaying {} in-flight batches",
                            self.in_flight.len()
                        );

                        self.last_sent = seq.prev_or_zero();
                        self.in_flight.clear();
                        self.state = SenderState::Connected;
                        return;
                    }
                    Err(_) => {
                        self.schedule_next_reconnect(attempt);
                        return;
                    }
                }
            }
        }

        self.state = SenderState::Connected;
    }

    fn schedule_next_reconnect(&mut self, attempt: u32) {
        let delay = backoff_delay(&self.config.retry, attempt);

        tracing::info!(
            attempt = attempt + 1,
            delay_ms = delay.as_millis(),
            "scheduling reconnection attempt"
        );

        self.state = SenderState::Reconnecting {
            attempt: attempt + 1,
            next_at: Instant::now() + delay,
        };
    }
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
