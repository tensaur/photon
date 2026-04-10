use std::collections::HashSet;

use photon_core::types::event::PhotonEvent;
use photon_core::types::query::{QueryMessage, SubscriptionId};
use photon_core::types::stream::{StreamFrame, SubscriptionUpdate};
use photon_transport::ports::Transport;
use tokio::sync::{broadcast, mpsc};

use crate::domain::subscription::ManagerCommand;

/// WebSocket handler for live subscriptions.
///
/// Accepts `QueryMessage`s from the client but only acts on subscription-related
/// variants (`Query` with `subscribe: true`, `Unsubscribe`). One-shot queries
/// should be sent over the `/api/query` HTTP endpoint instead.
pub async fn handle<T>(
    transport: &T,
    cmd_tx: mpsc::UnboundedSender<ManagerCommand>,
    mut event_rx: broadcast::Receiver<PhotonEvent>,
) where
    T: Transport<StreamFrame, QueryMessage>,
{
    // Per-connection channel: the manager pushes (id, update) pairs for any
    // subscription opened on this connection; we wrap each as a
    // StreamFrame::Subscription for transmission.
    let (stream_tx, mut stream_rx) =
        mpsc::unbounded_channel::<(SubscriptionId, SubscriptionUpdate)>();

    // Subscriptions opened by *this* connection. Tracked here so we can tell
    // the manager to clean them up when the connection closes — otherwise the
    // manager would leak `SubscriptionState` entries (and continue trying to
    // route events to a dead `response_tx`) for the lifetime of the process.
    let mut owned_ids: HashSet<SubscriptionId> = HashSet::new();

    loop {
        tokio::select! {
            msg = transport.recv() => {
                match msg {
                    Ok(QueryMessage::Query(ref q)) if q.subscribe => {
                        let _ = cmd_tx.send(ManagerCommand::Subscribe {
                            query: q.clone(),
                            response_tx: stream_tx.clone(),
                        });
                    }
                    Ok(QueryMessage::Unsubscribe(id)) => {
                        let _ = cmd_tx.send(ManagerCommand::Unsubscribe(id));
                    }
                    Ok(_) => {
                        // One-shot queries are not handled on the WS — use
                        // /api/query instead.
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
                        let msg = StreamFrame::Subscription { id, update };
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
                        if transport.send(&StreamFrame::RunsChanged).await.is_err() {
                            break;
                        }
                    }
                    Ok(PhotonEvent::Finalized { run_id }) => {
                        if transport.send(&StreamFrame::RunFinalized { run_id }).await.is_err() {
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
