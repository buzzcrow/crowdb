use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub const DEFAULT_READ_LEASE_SECONDS: u64 = 60;

/// Lock-free activity lease used by retention and reclamation checks.
pub struct ReadLease {
    last_activity: AtomicU64,
    released: AtomicBool,
    ttl_seconds: u64,
}

impl ReadLease {
    #[must_use]
    pub fn new(now_seconds: u64) -> Self {
        Self::with_ttl(now_seconds, DEFAULT_READ_LEASE_SECONDS)
    }

    #[must_use]
    pub fn with_ttl(now_seconds: u64, ttl_seconds: u64) -> Self {
        Self {
            last_activity: AtomicU64::new(now_seconds),
            released: AtomicBool::new(false),
            ttl_seconds,
        }
    }

    pub fn touch(&self, now_seconds: u64) {
        if !self.released.load(Ordering::Acquire) {
            self.last_activity.store(now_seconds, Ordering::Release);
        }
    }

    #[must_use]
    pub fn expired(&self, now_seconds: u64) -> bool {
        self.released.load(Ordering::Acquire)
            || now_seconds.saturating_sub(self.last_activity.load(Ordering::Acquire)) >= self.ttl_seconds
    }

    pub fn release(&self) {
        self.released.store(true, Ordering::Release);
    }
}
