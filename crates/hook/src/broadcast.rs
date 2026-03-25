use tokio::sync::broadcast;

/// Generic broadcast channel for distributing events to multiple subscribers.
#[derive(Clone)]
pub struct Broadcaster<E> {
    tx: broadcast::Sender<E>,
}

impl<E: Clone + Send + 'static> Broadcaster<E> {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn send(&self, event: E) {
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<E> {
        self.tx.subscribe()
    }
}
