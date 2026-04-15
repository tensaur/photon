use photon_core::types::event::PhotonEvent;
use tokio::sync::broadcast;

use crate::domain::service::SubscriptionService;

/// Drives `SubscriptionService::handle_event`.
pub async fn run<S: SubscriptionService>(
    service: S,
    mut event_rx: broadcast::Receiver<PhotonEvent>,
) {
    loop {
        match event_rx.recv().await {
            Ok(event) => {
                if let Err(e) = service.handle_event(event).await {
                    tracing::error!("subscription event handling failed: {e}");
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("subscription event listener lagged by {n} events");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}
