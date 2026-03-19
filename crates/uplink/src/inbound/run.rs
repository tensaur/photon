use std::time::Duration;

use photon_core::types::ack::AckResult;
use photon_core::types::batch::WireBatch;
use photon_core::types::config::UplinkConfig;
use photon_core::types::id::RunId;
use photon_transport::ports::Transport;
use photon_transport::TransportChoice;
use photon_wal::WalManagerChoice;
use tokio::sync::oneshot;

use super::connection::{ConnectionState, ReconnectResult, try_reconnect};
use crate::domain::ack::UplinkStats;
use crate::domain::error::{UplinkError, TransportError};
use crate::domain::service::{UplinkService, Service};
use crate::UplinkThreadError;

pub fn run_uplink_thread(
    transport: TransportChoice,
    endpoint: String,
    run_id: RunId,
    wal: WalManagerChoice,
    config: UplinkConfig,
    mut shutdown_rx: oneshot::Receiver<()>,
    batch_rx: crossbeam_channel::Receiver<WireBatch>,
) -> Result<UplinkStats, UplinkThreadError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(UplinkThreadError::Runtime)?;

    rt.block_on(async move {
        transport
            .connect(&endpoint)
            .await
            .map_err(TransportError::from)?;

        let mut service = Service::new(transport.clone(), wal.clone(), run_id);
        service.recover().await?;

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
                match try_reconnect(&wal, &transport, conn.oldest_in_flight()).await {
                    ReconnectResult::Ok => conn.reconnected(),
                    ReconnectResult::Failed => conn.schedule_next_reconnect(attempt),
                }
                continue;
            }

            if let Some(batch) = batch {
                if conn.can_send() {
                    match service.send(&batch).await {
                        Ok(()) => conn.record_sent(batch.sequence_number),
                        Err(UplinkError::Transport(TransportError::ConnectionLost { .. })) => {
                            conn.enter_reconnecting();
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }

            let recv = <TransportChoice as Transport<WireBatch, AckResult>>::recv(&transport);
            match tokio::time::timeout(Duration::from_millis(1), recv).await {
                Err(_) => {}
                Ok(Ok(ack)) => {
                    conn.record_acked(ack.sequence_number);
                    service.handle_ack(ack)?;
                }
                Ok(Err(e)) => {
                    let err: TransportError = e.into();
                    match &err {
                        TransportError::ConnectionLost { .. } => {
                            conn.enter_reconnecting();
                            continue;
                        }
                        _ => return Err(UplinkError::from(err).into()),
                    }
                }
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
    })
}
