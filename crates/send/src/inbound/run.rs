use photon_core::types::batch::WireBatch;
use photon_core::types::config::SenderConfig;
use photon_core::types::id::RunId;
use photon_transport::TransportChoice;
use photon_wal::WalManagerChoice;
use tokio::sync::oneshot;

use crate::domain::error::TransportError;
use crate::domain::service::{SenderService, SenderStats};
use crate::SenderThreadError;

pub fn run_sender_thread(
    transport: TransportChoice,
    endpoint: String,
    run_id: RunId,
    wal: WalManagerChoice,
    config: SenderConfig,
    mut shutdown_rx: oneshot::Receiver<()>,
    batch_rx: crossbeam_channel::Receiver<WireBatch>,
) -> Result<SenderStats, SenderThreadError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(SenderThreadError::Runtime)?;

    rt.block_on(async move {
        transport
            .connect(&endpoint)
            .await
            .map_err(TransportError::from)?;

        let tick_interval = config.idle_poll_interval;
        let mut sender = SenderService::new(transport, wal, config, run_id);

        sender.recover().await?;

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

            if sender.step(batch, shutdown_signaled).await? {
                return Ok(sender.stats());
            }

            // Only sleep when genuinely idle — no batches flowing and not
            // draining shutdown. The old Sender::run() only slept on the
            // `!sent && !acked` path; this preserves that behavior.
            if !had_work {
                tokio::time::sleep(tick_interval).await;
            }
        }
    })
}
