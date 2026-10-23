// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free local projection of one fixed Group-0 process lease.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use crowdb_protocol::{
    chunk_stream::{StreamProcessIdentity, StreamProcessLease},
    timing::{DEFAULT_SELF_FENCE_MARGIN_MS, MAX_WRITE_CLOCK_SKEW_MS},
};
use serde::Serialize;

use super::{invalid, Result, StreamProcessLeaseError};

#[derive(Clone, Copy, Debug)]
pub struct StreamProcessLeaseTiming {
    pub max_clock_skew_ms: u64,
    pub self_fence_margin_ms: u64,
}

impl Default for StreamProcessLeaseTiming {
    fn default() -> Self {
        Self {
            max_clock_skew_ms: MAX_WRITE_CLOCK_SKEW_MS,
            self_fence_margin_ms: DEFAULT_SELF_FENCE_MARGIN_MS,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StreamProcessContext {
    identity: StreamProcessIdentity,
    deadline: Instant,
}

impl StreamProcessContext {
    #[must_use]
    pub const fn identity(&self) -> StreamProcessIdentity {
        self.identity
    }
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }
}

struct LeaseState {
    lease: Arc<StreamProcessLease>,
    deadline: Instant,
    fenced: bool,
}

#[derive(Debug, Serialize)]
pub struct StreamProcessLeaseStatus {
    pub lease: StreamProcessLease,
    pub remaining_ms: u64,
    pub fenced: bool,
}

pub struct StreamProcessAuthority {
    identity: StreamProcessIdentity,
    timing: StreamProcessLeaseTiming,
    state: ArcSwap<LeaseState>,
}

impl StreamProcessAuthority {
    /// Project the authoritative expiry using wall/monotonic clocks sampled together.
    /// # Errors
    /// Rejects invalid records, unsafe timing, expired receipts and clock overflow.
    pub fn new(
        lease: StreamProcessLease,
        timing: StreamProcessLeaseTiming,
        wall_ms: u64,
        received: Instant,
    ) -> Result<Self> {
        let deadline = project(&lease, timing, wall_ms, received)?;
        if Instant::now() >= deadline {
            return Err(StreamProcessLeaseError::Expired);
        }
        Ok(Self {
            identity: lease.identity,
            timing,
            state: ArcSwap::from_pointee(LeaseState {
                lease: Arc::new(lease),
                deadline,
                fenced: false,
            }),
        })
    }

    /// Observe renewal without extending an admitted IO context or reviving a gap.
    /// # Errors
    /// A different process fences immediately; a late renewal cannot clear a fence.
    pub fn observe(&self, lease: StreamProcessLease, wall_ms: u64, received: Instant) -> Result<()> {
        lease.validate().map_err(|error| invalid(&error.to_string()))?;
        if lease.identity != self.identity {
            self.fence();
            return Err(StreamProcessLeaseError::Replaced);
        }
        let deadline = match project(&lease, self.timing, wall_ms, received) {
            Ok(deadline) => deadline,
            Err(error) => {
                let superseded = lease.expires_at_ms <= self.state.load().lease.expires_at_ms;
                return if matches!(&error, StreamProcessLeaseError::Expired) && superseded {
                    self.live_deadline(received).map(|_| ())
                } else {
                    Err(error)
                };
            }
        };
        let lease = Arc::new(lease);
        self.state.rcu(|current| {
            let now = Instant::now().max(received);
            if current.fenced || now >= current.deadline {
                return fenced(current);
            }
            if lease.expires_at_ms <= current.lease.expires_at_ms {
                return Arc::clone(current);
            }
            Arc::new(LeaseState {
                lease: Arc::clone(&lease),
                deadline,
                fenced: now >= deadline,
            })
        });
        self.live_deadline(received).map(|_| ())
    }

    /// Capture a fixed absolute deadline before IO admission.
    /// # Errors
    /// Returns a lease fence instead of creating a new IO budget after expiry.
    pub fn capture(&self, now: Instant) -> Result<StreamProcessContext> {
        Ok(StreamProcessContext {
            identity: self.identity,
            deadline: self.live_deadline(now)?,
        })
    }

    /// Validate current ownership before acknowledgment; continuous renewal is allowed.
    /// # Errors
    /// Rejects a replaced context or any local expiry/revocation.
    pub fn validate_context(&self, context: &StreamProcessContext, now: Instant) -> Result<()> {
        if context.identity != self.identity {
            return Err(StreamProcessLeaseError::Replaced);
        }
        self.live_deadline(now).map(|_| ())
    }

    /// Permanently revoke this local boot after confirmed replacement or shutdown.
    pub fn fence(&self) {
        self.state.rcu(fenced);
    }

    #[must_use]
    pub fn status(&self, now: Instant) -> StreamProcessLeaseStatus {
        let state = self.state.load();
        let now = now.max(Instant::now());
        let is_fenced = state.fenced || now >= state.deadline;
        StreamProcessLeaseStatus {
            lease: state.lease.as_ref().clone(),
            remaining_ms: if is_fenced {
                0
            } else {
                u64::try_from((state.deadline - now).as_millis()).unwrap_or(u64::MAX)
            },
            fenced: is_fenced,
        }
    }

    fn live_deadline(&self, now: Instant) -> Result<Instant> {
        let now = now.max(Instant::now());
        let state = self.state.load();
        if !state.fenced && now < state.deadline {
            return Ok(state.deadline);
        }
        drop(state);
        self.state.rcu(|current| {
            if now >= current.deadline || current.fenced {
                fenced(current)
            } else {
                Arc::clone(current)
            }
        });
        let current = self.state.load();
        if current.fenced || now >= current.deadline {
            Err(StreamProcessLeaseError::Expired)
        } else {
            Ok(current.deadline)
        }
    }
}

fn fenced(current: &Arc<LeaseState>) -> Arc<LeaseState> {
    if current.fenced {
        return Arc::clone(current);
    }
    Arc::new(LeaseState {
        lease: Arc::clone(&current.lease),
        deadline: current.deadline,
        fenced: true,
    })
}

fn project(
    lease: &StreamProcessLease,
    timing: StreamProcessLeaseTiming,
    wall_ms: u64,
    received: Instant,
) -> Result<Instant> {
    lease.validate().map_err(|error| invalid(&error.to_string()))?;
    if timing.max_clock_skew_ms > MAX_WRITE_CLOCK_SKEW_MS {
        return Err(invalid("owner clock skew exceeds shared recovery bound"));
    }
    let margin = timing
        .max_clock_skew_ms
        .checked_add(timing.self_fence_margin_ms)
        .ok_or_else(|| invalid("owner safety margin overflow"))?;
    let remaining = lease
        .expires_at_ms
        .checked_sub(wall_ms)
        .and_then(|remaining| remaining.checked_sub(margin))
        .filter(|remaining| *remaining > 0)
        .ok_or(StreamProcessLeaseError::Expired)?;
    received
        .checked_add(Duration::from_millis(remaining))
        .ok_or_else(|| invalid("owner monotonic deadline overflow"))
}
