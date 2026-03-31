pub mod broadcast;

use photon_core::types::event::PhotonEvent;
use tokio::sync::broadcast as tokio_broadcast;

/// Extension point for reacting to system events.
///
/// # Example
///
/// ```ignore
/// use photon_hook::Hook;
/// use photon_core::types::event::PhotonEvent;
///
/// #[derive(Clone)]
/// struct MyHook;
///
/// impl Hook for MyHook {
///     fn handle(&self, event: PhotonEvent) {
///         // react to events
///     }
/// }
/// ```
pub trait Hook: Clone + Send + Sync + 'static {
    fn handle(&self, event: PhotonEvent);

    fn spawn(
        &self,
        mut events: tokio_broadcast::Receiver<PhotonEvent>,
    ) -> tokio::task::JoinHandle<()> {
        let hook = self.clone();
        tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(event) => hook.handle(event),
                    Err(tokio_broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("hook lagged by {n} events");
                    }
                    Err(tokio_broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }
}
