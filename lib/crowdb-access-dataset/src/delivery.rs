use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

/// Lock-free admission window for bounded batch delivery.
pub struct DeliveryWindow {
    max_in_flight: usize,
    in_flight: AtomicUsize,
    cancelled: AtomicBool,
}

impl DeliveryWindow {
    #[must_use]
    pub fn new(max_in_flight: usize) -> Option<Self> {
        (max_in_flight > 0).then(|| Self {
            max_in_flight,
            in_flight: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
        })
    }

    #[must_use]
    pub fn try_acquire(&self) -> bool {
        if self.cancelled.load(AtomicOrdering::Acquire) {
            return false;
        }
        let mut current = self.in_flight.load(AtomicOrdering::Relaxed);
        loop {
            if current >= self.max_in_flight || self.cancelled.load(AtomicOrdering::Acquire) {
                return false;
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => {
                    if self.cancelled.load(AtomicOrdering::Acquire) {
                        self.release();
                        return false;
                    }
                    return true;
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub fn release(&self) {
        let _ = self
            .in_flight
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Relaxed, |value| {
                value.checked_sub(1)
            });
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, AtomicOrdering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(AtomicOrdering::Acquire)
    }

    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(AtomicOrdering::Acquire)
    }
}
