use std::collections::HashSet;

use photon_core::types::event::PhotonEvent;
use photon_core::types::id::SubscriptionId;
use photon_core::types::stream::{StreamMessage, SubscriptionMessage, SubscriptionUpdate};
use photon_transport::ports::Transport;
use tokio::sync::{broadcast, mpsc};

use crate::domain::service::{SubscriptionReceiver, SubscriptionSender, SubscriptionService};

/// WebSocket handler for live subscriptions.
pub async fn handle<S, T>(
    service: &S,
    transport: &T,
    mut event_rx: broadcast::Receiver<PhotonEvent>,
) where
    S: SubscriptionService,
    T: Transport<StreamMessage, SubscriptionMessage>,
{
    let (updates_tx, mut updates_rx): (SubscriptionSender, SubscriptionReceiver) =
        mpsc::unbounded_channel();

    let mut owned_ids: HashSet<SubscriptionId> = HashSet::new();

    loop {
        tokio::select! {
            msg = transport.recv() => {
                match msg {
                    Ok(SubscriptionMessage::Subscribe(q)) => {
                        if let Err(e) = service.subscribe(q, updates_tx.clone()).await {
                            tracing::error!("subscribe failed: {e}");
                        }
                    }
                    Ok(SubscriptionMessage::Unsubscribe(id)) => {
                        service.unsubscribe(id).await;
                    }
                    Err(_) => break,
                }
            }
            update = updates_rx.recv() => {
                match update {
                    Some((id, update)) => {
                        match &update {
                            SubscriptionUpdate::Snapshot { .. } => {
                                owned_ids.insert(id);
                            }
                            SubscriptionUpdate::Unsubscribed => {
                                owned_ids.remove(&id);
                            }
                            _ => {}
                        }
                        let msg = StreamMessage::Subscription { id, update };
                        if transport.send(&msg).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            event = event_rx.recv() => {
                match event {
                    Ok(PhotonEvent::RunStatusChanged { .. }) => {
                        if transport.send(&StreamMessage::RunsChanged).await.is_err() {
                            break;
                        }
                    }
                    Ok(PhotonEvent::Finalised { run_id }) => {
                        if transport.send(&StreamMessage::RunFinalised { run_id }).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("ws handler lagged by {n} events");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    // Connection closed.
    if !owned_ids.is_empty() {
        service.disconnect(owned_ids.into_iter().collect()).await;
    }
}
