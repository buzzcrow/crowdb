// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use crowdb_protocol::chunk_kv::{
    DomainMonitorDescriptor, Id128, InstanceHealth, ServingAssignment, ServingGrant,
};
use thiserror::Error;

#[derive(Clone, Debug)]
struct AuthoritySnapshot {
    grant: Arc<ServingGrant>,
    local_deadline_ms: u64,
    continuity: Vec<u64>,
}

#[derive(Default)]
struct AuthorityState {
    highest_sequence: u64,
    current: Option<AuthoritySnapshot>,
}

/// One admitted assignment; later grants cannot revive it after exclusion.
#[derive(Clone, Copy, Debug)]
pub struct ServingContext {
    instance_id: u64,
    assignment: ServingAssignment,
    continuity: u64,
    deadline_ms: u64,
}

impl ServingContext {
    /// Original IO admission deadline; renewal does not extend already issued IO.
    #[must_use]
    pub const fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum AuthorityError {
    #[error("monitor policy is invalid")]
    InvalidPolicy,
    #[error("serving grant is invalid")]
    InvalidGrant,
    #[error("serving grant belongs to another instance")]
    WrongInstance,
    #[error("serving grant has expired or reached its safety deadline")]
    LeaseExpired,
    #[error("serving grant is older than the installed authority")]
    StaleGrant,
    #[error("catalog generation does not match the serving grant")]
    StaleCatalog,
    #[error("partition and ownership epoch are not authorized")]
    NotAssigned,
}

/// Lock-free request-path authority derived from monitor-issued serving grants.
pub struct ServingAuthority {
    instance_id: u64,
    clock_origin: Instant,
    state: ArcSwap<AuthorityState>,
}

impl ServingAuthority {
    #[must_use]
    pub fn new(instance_id: u64) -> Self {
        Self {
            instance_id,
            clock_origin: Instant::now(),
            state: ArcSwap::from_pointee(AuthorityState::default()),
        }
    }

    /// Local monotonic clock shared by receipt, installation and request checks.
    #[must_use]
    pub fn monotonic_ms(&self) -> u64 {
        u64::try_from(self.clock_origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Install a grant using wall/monotonic clocks sampled together at receipt.
    /// # Errors
    /// Rejects invalid policy, identity, expiry and superseded grant sequences.
    pub fn install(
        &self,
        grant: ServingGrant,
        policy: &DomainMonitorDescriptor,
        received_wall_ms: u64,
        received_monotonic_ms: u64,
    ) -> Result<(), AuthorityError> {
        let started = Instant::now();
        policy.validate().map_err(|_| AuthorityError::InvalidPolicy)?;
        grant.validate().map_err(|_| AuthorityError::InvalidGrant)?;
        if grant.instance_id != self.instance_id {
            return Err(AuthorityError::WrongInstance);
        }
        let local_deadline_ms = grant_deadline(&grant, policy, received_wall_ms, received_monotonic_ms)?;
        let grant = Arc::new(grant);
        self.state.rcu(|installed| {
            if grant.lease_sequence <= installed.highest_sequence {
                return Arc::clone(installed);
            }
            let published_at = self.current_time(received_monotonic_ms, started);
            let continuity = grant
                .assignments
                .iter()
                .map(|assignment| {
                    installed
                        .current
                        .as_ref()
                        .and_then(|previous| {
                            (published_at < previous.local_deadline_ms)
                                .then(|| previous.grant.assignments.binary_search(assignment).ok())
                                .flatten()
                                .map(|index| previous.continuity[index])
                        })
                        .unwrap_or(grant.lease_sequence)
                })
                .collect();
            Arc::new(AuthorityState {
                highest_sequence: grant.lease_sequence,
                current: Some(AuthoritySnapshot {
                    grant: Arc::clone(&grant),
                    local_deadline_ms,
                    continuity,
                }),
            })
        });
        let installed = self.state.load();
        let current = installed.current.as_ref().ok_or(AuthorityError::StaleGrant)?;
        if current.grant != grant {
            return Err(AuthorityError::StaleGrant);
        }
        if self.current_time(received_monotonic_ms, started) >= current.local_deadline_ms {
            return Err(AuthorityError::LeaseExpired);
        }
        Ok(())
    }

    /// Check an assignment before request admission.
    /// # Errors
    /// Returns the precise lease, catalog or assignment fence reason.
    pub fn authorize(
        &self,
        catalog_generation: u64,
        partition_id: Id128,
        owner_epoch: u64,
        now_monotonic_ms: u64,
    ) -> Result<(), AuthorityError> {
        self.capture(catalog_generation, partition_id, owner_epoch, now_monotonic_ms)
            .map(|_| ())
    }

    /// Capture continuity and the original deadline before asynchronous work.
    /// # Errors
    /// Rejects expired, unassigned or stale-catalog admission.
    pub fn capture(
        &self,
        catalog_generation: u64,
        partition_id: Id128,
        owner_epoch: u64,
        now_monotonic_ms: u64,
    ) -> Result<ServingContext, AuthorityError> {
        let state = self.state.load();
        let snapshot = live_snapshot(&state, now_monotonic_ms.max(self.monotonic_ms()))?;
        if catalog_generation != snapshot.grant.catalog_generation {
            return Err(AuthorityError::StaleCatalog);
        }
        let assignment = ServingAssignment {
            partition_id,
            owner_epoch,
        };
        let index = snapshot
            .grant
            .assignments
            .binary_search(&assignment)
            .map_err(|_| AuthorityError::NotAssigned)?;
        Ok(ServingContext {
            instance_id: self.instance_id,
            assignment,
            continuity: snapshot.continuity[index],
            deadline_ms: snapshot.local_deadline_ms,
        })
    }

    /// Validate the admitted assignment before acknowledging asynchronous work.
    /// # Errors
    /// Rejects expiry, replacement, removal or a break in lease continuity.
    pub fn validate_context(
        &self,
        context: &ServingContext,
        now_monotonic_ms: u64,
    ) -> Result<(), AuthorityError> {
        if context.instance_id != self.instance_id {
            return Err(AuthorityError::WrongInstance);
        }
        let state = self.state.load();
        let snapshot = live_snapshot(&state, now_monotonic_ms.max(self.monotonic_ms()))?;
        let index = snapshot
            .grant
            .assignments
            .binary_search(&context.assignment)
            .map_err(|_| AuthorityError::LeaseExpired)?;
        if snapshot.continuity[index] != context.continuity {
            return Err(AuthorityError::LeaseExpired);
        }
        Ok(())
    }

    /// Revoke authority without discarding the accepted sequence fence.
    pub fn clear(&self) {
        self.state.rcu(|state| {
            Arc::new(AuthorityState {
                highest_sequence: state.highest_sequence,
                current: None,
            })
        });
    }

    #[must_use]
    pub fn has_live_grant(&self, catalog_generation: u64, now_monotonic_ms: u64) -> bool {
        let now_monotonic_ms = now_monotonic_ms.max(self.monotonic_ms());
        self.state.load().current.as_ref().is_some_and(|snapshot| {
            snapshot.grant.catalog_generation == catalog_generation
                && now_monotonic_ms < snapshot.local_deadline_ms
        })
    }

    fn current_time(&self, received: u64, started: Instant) -> u64 {
        let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.monotonic_ms().max(received.saturating_add(elapsed))
    }
}

fn live_snapshot(state: &AuthorityState, now: u64) -> Result<&AuthoritySnapshot, AuthorityError> {
    state
        .current
        .as_ref()
        .filter(|snapshot| now < snapshot.local_deadline_ms)
        .ok_or(AuthorityError::LeaseExpired)
}

fn grant_deadline(
    grant: &ServingGrant,
    policy: &DomainMonitorDescriptor,
    wall: u64,
    monotonic: u64,
) -> Result<u64, AuthorityError> {
    let margin = policy
        .max_clock_skew_ms
        .checked_add(policy.self_fence_margin_ms)
        .ok_or(AuthorityError::InvalidPolicy)?;
    let remaining = grant
        .expires_at_ms
        .checked_sub(margin)
        .and_then(|safe| safe.checked_sub(wall))
        .filter(|remaining| *remaining > 0)
        .ok_or(AuthorityError::LeaseExpired)?;
    monotonic
        .checked_add(remaining)
        .ok_or(AuthorityError::InvalidGrant)
}

#[must_use]
pub fn classify_instance(
    last_heartbeat_ms: u64,
    now_ms: u64,
    policy: &DomainMonitorDescriptor,
) -> InstanceHealth {
    let elapsed = now_ms.saturating_sub(last_heartbeat_ms);
    if elapsed >= policy.dead_after_ms {
        InstanceHealth::Dead
    } else if elapsed >= policy.suspect_after_ms {
        InstanceHealth::Suspect
    } else {
        InstanceHealth::Healthy
    }
}

#[must_use]
pub fn replacement_may_activate(
    old_grant_expires_at_ms: u64,
    now_wall_ms: u64,
    max_clock_skew_ms: u64,
    explicit_fence_proven: bool,
) -> bool {
    explicit_fence_proven
        || old_grant_expires_at_ms
            .checked_add(max_clock_skew_ms)
            .is_some_and(|boundary| now_wall_ms >= boundary)
}
