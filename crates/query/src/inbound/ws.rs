use std::collections::HashSet;

use photon_core::types::event::PhotonEvent;
use photon_core::types::id::SubscriptionId;
use photon_core::types::stream::{StreamMessage, SubscriptionMessage, SubscriptionUpdate};
use photon_transport::ports::Transport;
use tokio::sync::{broadcast, mpsc};

use crate::domain::subscription::ManagerCommand;

/// WebSocket handler for live subscriptions.
pub async fn handle<T>(
    transport: &T,
    cmd_tx: mpsc::UnboundedSender<ManagerCommand>,
    mut event_rx: broadcast::Receiver<PhotonEvent>,
) where
    T: Transport<StreamMessage, SubscriptionMessage>,
{
    // Per-connection channel.
    let (stream_tx, mut stream_rx) =
        mpsc::unbounded_channel::<(SubscriptionId, SubscriptionUpdate)>();

    // Subscriptions opened by this connection.
    let mut owned_ids: HashSet<SubscriptionId> = HashSet::new();

    loop {
        tokio::select! {
            msg = transport.recv() => {
                match msg {
                    Ok(SubscriptionMessage::Subscribe(ref q)) => {
                        let _ = cmd_tx.send(ManagerCommand::Subscribe {
                            query: q.clone(),
                            response_tx: stream_tx.clone(),
                        });
                    }
                    Ok(SubscriptionMessage::Unsubscribe(id)) => {
                        let _ = cmd_tx.send(ManagerCommand::Unsubscribe(id));
                    }
                    Err(_) => break,
                }
            }
            stream_msg = stream_rx.recv() => {
                match stream_msg {
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

    // Connection closed. Tell the manager to drop every subscription that was
    // opened on this connection so its indexes don't accumulate dead entries.
    if !owned_ids.is_empty() {
        let _ = cmd_tx.send(ManagerCommand::Disconnect(owned_ids.into_iter().collect()));
    }
}
