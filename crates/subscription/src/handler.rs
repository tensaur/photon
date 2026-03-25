use std::collections::HashSet;

use photon_core::types::id::RunId;
use photon_core::types::subscription::{SubscriptionEvent, SubscriptionMessage};
use photon_transport::ports::{Transport, TransportError};
use tokio::sync::broadcast;

pub async fn handle<T>(transport: &T, mut events: broadcast::Receiver<SubscriptionEvent>)
where
    T: Transport<SubscriptionEvent, SubscriptionMessage>,
{
    let mut subscribed_runs: HashSet<RunId> = HashSet::new();

    loop {
        tokio::select! {
            msg = transport.recv() => {
                match msg {
                    Ok(SubscriptionMessage::Subscribe(run_id)) => {
                        subscribed_runs.insert(run_id);
                    }
                    Ok(SubscriptionMessage::Unsubscribe(run_id)) => {
                        subscribed_runs.remove(&run_id);
                    }
                    Err(TransportError::StreamClosed(_)) => break,
                    Err(_) => break,
                }
            }
            event = events.recv() => {
                match event {
                    Ok(ref ev @ SubscriptionEvent::LivePoints { ref run_id, .. }) if subscribed_runs.contains(run_id) => {
                        if transport.send(ev).await.is_err() { break; }
                    }
                    Ok(SubscriptionEvent::LivePoints { .. }) => {}
                    Ok(ref e @ SubscriptionEvent::RunsChanged) => {
                        if transport.send(e).await.is_err() { break; }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("subscription handler lagged by {n} events");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}
