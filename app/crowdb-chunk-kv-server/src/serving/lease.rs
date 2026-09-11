// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use crowdb_protocol::chunk_kv::{
    DomainMonitorDescriptor, Id128, InstanceHealth, ServingAssignment, ServingGrant,
};
use thiserror::Error;

#[derive(Clone, Debug)]
struct AuthoritySnapshot {
    grant: ServingGrant,
    local_deadline_ms: u64,
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
    snapshot: ArcSwapOption<AuthoritySnapshot>,
}

impl ServingAuthority {
    #[must_use]
    pub fn new(instance_id: u64) -> Self {
        Self {
            instance_id,
            snapshot: ArcSwapOption::empty(),
        }
    }

    /// Installs a validated grant and derives its conservative local deadline.
    ///
    /// `received_wall_ms` and `received_monotonic_ms` must be sampled together.
    /// Request authorization subsequently relies only on the monotonic clock.
    ///
    /// # Errors
    ///
    /// Returns a typed error for unsafe policy, invalid identity, expiry, or a
    /// grant that loses a concurrent monotonic installation race.
    pub fn install(
        &self,
        grant: ServingGrant,
        policy: &DomainMonitorDescriptor,
        received_wall_ms: u64,
        received_monotonic_ms: u64,
    ) -> Result<(), AuthorityError> {
        policy.validate().map_err(|_| AuthorityError::InvalidPolicy)?;
        grant.validate().map_err(|_| AuthorityError::InvalidGrant)?;
        if grant.instance_id != self.instance_id {
            return Err(AuthorityError::WrongInstance);
        }
        let safety_margin = policy
            .max_clock_skew_ms
            .checked_add(policy.self_fence_margin_ms)
            .ok_or(AuthorityError::InvalidPolicy)?;
        let safe_wall_deadline = grant
            .expires_at_ms
            .checked_sub(safety_margin)
            .ok_or(AuthorityError::LeaseExpired)?;
        let safe_remaining = safe_wall_deadline
            .checked_sub(received_wall_ms)
            .ok_or(AuthorityError::LeaseExpired)?;
        if safe_remaining == 0 {
            return Err(AuthorityError::LeaseExpired);
        }
        let local_deadline_ms = received_monotonic_ms
            .checked_add(safe_remaining)
            .ok_or(AuthorityError::InvalidGrant)?;
        let candidate = Arc::new(AuthoritySnapshot {
            grant,
            local_deadline_ms,
        });

        self.snapshot.rcu(|current| {
            if current
                .as_ref()
                .is_some_and(|installed| installed.grant.lease_sequence >= candidate.grant.lease_sequence)
            {
                current.clone()
            } else {
                Some(Arc::clone(&candidate))
            }
        });
        if self
            .snapshot
            .load_full()
            .as_ref()
            .is_some_and(|installed| installed.grant == candidate.grant)
        {
            Ok(())
        } else {
            Err(AuthorityError::StaleGrant)
        }
    }

    /// Checks one partition request without taking a lock.
    ///
    /// # Errors
    ///
    /// Returns the precise fence reason before a request reaches its WAL.
    pub fn authorize(
        &self,
        catalog_generation: u64,
        partition_id: Id128,
        owner_epoch: u64,
        now_monotonic_ms: u64,
    ) -> Result<(), AuthorityError> {
        let snapshot = self.snapshot.load_full().ok_or(AuthorityError::LeaseExpired)?;
        if now_monotonic_ms >= snapshot.local_deadline_ms {
            return Err(AuthorityError::LeaseExpired);
        }
        if catalog_generation != snapshot.grant.catalog_generation {
            return Err(AuthorityError::StaleCatalog);
        }
        let assignment = ServingAssignment {
            partition_id,
            owner_epoch,
        };
        if snapshot.grant.assignments.binary_search(&assignment).is_err() {
            return Err(AuthorityError::NotAssigned);
        }
        Ok(())
    }

    pub fn clear(&self) {
        self.snapshot.store(None);
    }

    #[must_use]
    pub fn has_live_grant(&self, now_monotonic_ms: u64) -> bool {
        self.snapshot
            .load()
            .as_ref()
            .is_some_and(|snapshot| now_monotonic_ms < snapshot.local_deadline_ms)
    }
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
