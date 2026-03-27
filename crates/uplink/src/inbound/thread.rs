use std::time::Duration;

use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::Run;
use photon_core::types::batch::WireBatch;
use photon_core::types::config::UplinkConfig;
use photon_core::types::ingest::{IngestMessage, IngestResult};
use photon_core::types::sequence::SequenceNumber;
use photon_wal::Wal;
use tokio::sync::oneshot;

use super::state::{ConnectionState, ReconnectResult, try_reconnect};
use crate::UplinkThreadError;
use crate::domain::ack::UplinkStats;
use crate::domain::error::{UplinkError, UplinkTransportError};
use crate::domain::ports::IngestConnection;
use crate::domain::service::{Service, UplinkService};

pub async fn run_uplink<C, M>(
    connection: C,
    run: Run,
    project: Project,
    experiment: Option<Experiment>,
    wal: M,
    config: UplinkConfig,
    start_sequence: SequenceNumber,
    mut shutdown_rx: oneshot::Receiver<()>,
    batch_rx: crossbeam_channel::Receiver<WireBatch>,
) -> Result<UplinkStats, UplinkThreadError>
where
    C: IngestConnection,
    M: Wal + Clone,
{
    let mut service = Service::new(connection.clone(), wal.clone(), run.id(), start_sequence);
    service.recover().await?;

    let _ = connection
        .send_message(IngestMessage::RegisterProject(project))
        .await;
    if let Some(exp) = experiment {
        let _ = connection
            .send_message(IngestMessage::RegisterExperiment(exp))
            .await;
    }
    let _ = connection
        .send_message(IngestMessage::RegisterRun(run))
        .await;

    let mut conn = ConnectionState::new(&config);
    let tick_interval = config.idle_poll_interval;
    let mut shutdown_signaled = false;

    loop {
        let batch = batch_rx.try_recv().ok();

        if !shutdown_signaled {
            shutdown_signaled = !matches!(
                shutdown_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            );
        }

        let had_work = batch.is_some() || shutdown_signaled;

        if conn.is_shutdown() {
            return Ok(service.stats());
        }

        if let Some(attempt) = conn.reconnect_due() {
            match try_reconnect(&wal, &connection, service.wal_cursor()).await {
                ReconnectResult::Ok => conn.reconnected(),
                ReconnectResult::Failed => conn.schedule_next_reconnect(attempt),
            }

            continue;
        }

        if let Some(batch) = batch
            && conn.can_send()
        {
            match service.send(&batch).await {
                Ok(()) => conn.record_sent(batch.sequence_number),
                Err(UplinkError::Transport(UplinkTransportError::ConnectionLost { .. })) => {
                    conn.enter_reconnecting();
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        match tokio::time::timeout(Duration::from_millis(1), connection.recv()).await {
            Ok(Ok(IngestResult::Ack(ack))) => {
                conn.record_acked(ack.sequence_number);
                service.handle_ack(ack)?;
            }
            Ok(Ok(IngestResult::Error(e))) => {
                tracing::warn!("ingest error from server: {e:?}");
            }
            Ok(Err(e)) => match &e {
                UplinkTransportError::ConnectionLost { .. } => {
                    conn.enter_reconnecting();
                    continue;
                }
                _ => return Err(UplinkError::from(e).into()),
            },
            _ => {}
        }

        if conn.check_timeouts().is_some() {
            continue;
        }

        if shutdown_signaled {
            if conn.in_flight_empty() {
                service.sync()?;
                conn.shutdown();
            } else if let Some(elapsed) = conn.check_drain_timeout() {
                return Err(UplinkError::ShutdownTimeout(elapsed).into());
            }
        } else {
            conn.reset_drain();
        }

        if !had_work {
            tokio::time::sleep(tick_interval).await;
        }
    }
}
