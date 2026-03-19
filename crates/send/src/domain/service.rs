use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use photon_core::types::ack::{AckResult, AckStatus};
use photon_core::types::batch::WireBatch;
use photon_core::types::config::{RetryConfig, SenderConfig};
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_transport::ports::Transport;
use photon_wal::WalManager;

use super::error::TransportError;
use photon_wal::ports::WalError;

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("WAL operation failed")]
    Wal(#[from] WalError),

    #[error("transport error")]
    Transport(#[from] TransportError),

    #[error("shutdown timeout after {0:?}")]
    ShutdownTimeout(Duration),
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("WAL error during recovery")]
    Wal(#[from] WalError),

    #[error("transport error during recovery")]
    Transport(#[from] TransportError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SenderState {
    Connected,
    Reconnecting { attempt: u32, next_at: Instant },
    Shutdown,
}

#[derive(Clone, Debug)]
struct InFlightEntry {
    sent_at: Instant,
    attempt: u32,
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

pub struct SenderService<T, M>
where
    T: Transport<WireBatch, AckResult>,
    M: WalManager,
{
    transport: T,
    wal: M,
    config: SenderConfig,
    run_id: RunId,
    state: SenderState,
    in_flight: BTreeMap<SequenceNumber, InFlightEntry>,
    // Ack tracking (absorbed from AckTracker)
    committed: SequenceNumber,
    pending_acks: BTreeSet<SequenceNumber>,
    batches_since_flush: u32,
    ack_flush_interval: u32,
    // Misc
    last_sent: SequenceNumber,
    stats: SenderStats,
    draining_since: Option<Instant>,
}

impl<T, M> SenderService<T, M>
where
    T: Transport<WireBatch, AckResult>,
    M: WalManager,
{
    pub fn new(transport: T, wal: M, config: SenderConfig, run_id: RunId) -> Self {
        Self {
            transport,
            wal,
            config,
            run_id,
            state: SenderState::Connected,
            in_flight: BTreeMap::new(),
            committed: SequenceNumber::ZERO,
            pending_acks: BTreeSet::new(),
            batches_since_flush: 0,
            ack_flush_interval: 10,
            last_sent: SequenceNumber::ZERO,
            stats: SenderStats::default(),
            draining_since: None,
        }
    }

    pub fn stats(&self) -> SenderStats {
        self.stats.clone()
    }

    /// Compare local WAL watermark against the server and set the starting
    /// committed position. Called once at startup.
    pub async fn recover(&mut self) -> Result<SequenceNumber, RecoveryError>
    where
        T: Transport<RunId, SequenceNumber>,
    {
        let meta = self.wal.read_meta()?;
        let uncommitted = self.wal.read_from(meta.committed_sequence)?;

        if uncommitted.is_empty() {
            tracing::debug!(run_id = %self.run_id, "clean WAL, skipping recovery");
            return Ok(self.committed);
        }

        // Query server for its watermark
        self.transport
            .send(&self.run_id)
            .await
            .map_err(TransportError::from)?;
        let server: SequenceNumber = <T as Transport<RunId, SequenceNumber>>::recv(&self.transport)
            .await
            .map_err(TransportError::from)?;

        let local = meta.committed_sequence;
        let effective = std::cmp::max(local, server);

        self.committed = effective;
        self.last_sent = effective;

        let replay_batches = self.wal.read_from(effective)?;
        let bytes_to_replay: u64 = replay_batches
            .iter()
            .map(|b| b.compressed_size() as u64)
            .sum();

        if !replay_batches.is_empty() {
            tracing::info!(
                run_id = %self.run_id,
                local_watermark = u64::from(local),
                server_watermark = u64::from(server),
                effective_watermark = u64::from(effective),
                batches = replay_batches.len(),
                bytes = bytes_to_replay,
                "replaying uncommitted batches from WAL"
            );
        }

        Ok(effective)
    }

    /// Single tick of the send/ack loop.
    /// Returns `true` when shutdown is complete.
    pub async fn step(
        &mut self,
        new_batch: Option<WireBatch>,
        shutdown: bool,
    ) -> Result<bool, SendError> {
        match &self.state {
            SenderState::Shutdown => return Ok(true),

            SenderState::Reconnecting { attempt, next_at } => {
                let attempt = *attempt;
                let next_at = *next_at;
                if Instant::now() >= next_at {
                    self.try_reconnect(attempt).await;
                }
            }

            SenderState::Connected => {
                // Send new batch if provided and within in-flight limit
                if let Some(batch) = new_batch {
                    if self.in_flight.len() < self.config.max_in_flight {
                        self.send_batch(&batch).await?;
                    }
                }

                // Receive and process acks
                self.poll_acks().await?;

                // Check in-flight timeouts
                self.check_in_flight_timeouts();

                // Handle shutdown signal
                if shutdown {
                    if self.in_flight.is_empty() {
                        self.wal.sync()?;
                        self.state = SenderState::Shutdown;
                    } else {
                        let since = *self.draining_since.get_or_insert(Instant::now());
                        if since.elapsed() > self.config.shutdown_timeout {
                            return Err(SendError::ShutdownTimeout(since.elapsed()));
                        }
                    }
                } else {
                    self.draining_since = None;
                }
            }
        }

        Ok(self.state == SenderState::Shutdown)
    }

    async fn send_batch(&mut self, batch: &WireBatch) -> Result<(), SendError> {
        match self
            .transport
            .send(batch)
            .await
            .map_err(TransportError::from)
        {
            Ok(()) => {
                self.in_flight.insert(
                    batch.sequence_number,
                    InFlightEntry {
                        sent_at: Instant::now(),
                        attempt: 1,
                    },
                );
                self.last_sent = batch.sequence_number;
                self.stats.batches_sent += 1;
                Ok(())
            }
            Err(TransportError::ConnectionLost { .. }) => {
                self.enter_reconnecting();
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn poll_acks(&mut self) -> Result<(), SendError> {
        let ack = tokio::time::timeout(Duration::from_millis(1), self.transport.recv()).await;

        match ack {
            Err(_) => Ok(()), // timeout — no ack available
            Ok(Ok(ack)) => self.handle_ack(ack),
            Ok(Err(e)) => {
                let err = TransportError::from(e);
                match &err {
                    TransportError::ConnectionLost { .. } => {
                        self.enter_reconnecting();
                        Ok(())
                    }
                    _ => Err(err.into()),
                }
            }
        }
    }

    fn handle_ack(&mut self, ack: AckResult) -> Result<(), SendError> {
        let seq = ack.sequence_number;
        self.in_flight.remove(&seq);

        match ack.status {
            AckStatus::Ok => self.stats.batches_acked += 1,
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

        // Advance watermark through contiguous acked sequence
        self.pending_acks.insert(seq);

        let before = self.committed;
        loop {
            let next = self.committed.next();
            if self.pending_acks.remove(&next) {
                self.committed = next;
            } else {
                break;
            }
        }

        if self.committed > before {
            self.stats.watermark_advances += 1;
            self.wal.truncate_through(self.committed)?;
            self.stats.segments_truncated += 1;

            self.batches_since_flush += 1;
            if self.batches_since_flush >= self.ack_flush_interval {
                self.wal.sync()?;
                self.batches_since_flush = 0;
            }
        }

        Ok(())
    }

    fn check_in_flight_timeouts(&mut self) {
        if self.in_flight.is_empty() {
            return;
        }

        let oldest = self.in_flight.values().next().unwrap();
        let elapsed = oldest.sent_at.elapsed();

        if elapsed > self.config.in_flight_timeout {
            tracing::warn!(
                attempt = oldest.attempt,
                elapsed_ms = elapsed.as_millis(),
                in_flight = self.in_flight.len(),
                "in-flight batch timed out, reconnecting"
            );

            self.stats.batches_retried += self.in_flight.len() as u64;
            self.enter_reconnecting();
        }
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
