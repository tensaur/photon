use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};

pub struct Accumulator<P: Copy + Send> {
    tx: Sender<P>,
    points_dropped: u64,
}

impl<P: Copy + Send> Accumulator<P> {
    pub fn new(channel_capacity: usize) -> (Self, Receiver<P>) {
        let (tx, rx) = bounded(channel_capacity);

        let accumulator = Self {
            tx,
            points_dropped: 0,
        };

        (accumulator, rx)
    }

    pub fn push(&mut self, point: P) {
        match self.tx.try_send(point) {
            Ok(()) => {}
            Err(TrySendError::Full(point)) => {
                // Block until there's space
                if self.tx.send(point).is_err() {
                    self.points_dropped += 1;
                }
            }
            Err(TrySendError::Disconnected(_)) => {
                self.points_dropped += 1;
            }
        }
    }

    pub fn points_dropped(&self) -> u64 {
        self.points_dropped
    }
}
