use crossbeam_channel::{bounded, Sender, Receiver, TrySendError};

pub struct Accumulator<P: Copy + Send> {
    tx: Sender<P>,
    spill: SpillRing<P>,
    points_dropped: u64,
}

impl<P: Copy + Send> Accumulator<P> {
    pub fn new(channel_capacity: usize, spill_capacity: usize) -> (Self, Receiver<P>) {
        let (tx, rx) = bounded(channel_capacity);

        let accumulator = Self {
            tx,
            spill: SpillRing::new(spill_capacity),
            points_dropped: 0,
        };

        (accumulator, rx)
    }

    pub fn push(&mut self, point: P) {
        self.drain_spill();

        match self.tx.try_send(point) {
            Ok(()) => {}
            Err(TrySendError::Full(point)) => {
                if self.spill.push(point) {
                    self.points_dropped += 1;
                }
            }
            Err(TrySendError::Disconnected(_)) => {
                self.points_dropped += 1;
            }
        }
    }

    fn drain_spill(&mut self) {
        while let Some(point) = self.spill.peek() {
            match self.tx.try_send(point) {
                Ok(()) => {
                    self.spill.pop();
                }
                Err(_) => break,
            }
        }
    }

    pub fn points_dropped(&self) -> u64 {
        self.points_dropped
    }
}

struct SpillRing<P: Copy> {
    buf: Vec<Option<P>>,
    head: usize,
    tail: usize,
    len: usize,
}

impl<P: Copy> SpillRing<P> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "spill ring capacity must be > 0");
        Self {
            buf: (0..capacity).map(|_| None).collect(),
            head: 0,
            tail: 0,
            len: 0,
        }
    }

    fn push(&mut self, point: P) -> bool {
        let overwritten = self.len == self.buf.len();
        self.buf[self.head] = Some(point);
        self.head = (self.head + 1) % self.buf.len();

        if overwritten {
            self.tail = self.head;
        } else {
            self.len += 1;
        }

        overwritten
    }

    fn peek(&self) -> Option<P> {
        if self.len == 0 {
            return None;
        }

        self.buf[self.tail]
    }

    fn pop(&mut self) {
        if self.len > 0 {
            self.buf[self.tail] = None;
            self.tail = (self.tail + 1) % self.buf.len();
            self.len -= 1;
        }
    }
}

