// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable admission context and shared grant-snapshot validation.

use super::{AuthorityError, AuthoritySnapshot, AuthorityState, ServingAuthority};
use crowdb_protocol::chunk_kv::{Id128, ServingAssignment};

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

impl ServingAuthority {
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
        self.validate_contexts(std::slice::from_ref(context), now_monotonic_ms)
    }

    pub(crate) fn validate_contexts(
        &self,
        contexts: &[ServingContext],
        now_monotonic_ms: u64,
    ) -> Result<(), AuthorityError> {
        if contexts.is_empty() {
            return Ok(());
        }
        let state = self.state.load();
        let snapshot = live_snapshot(&state, now_monotonic_ms.max(self.monotonic_ms()))?;
        for context in contexts {
            if context.instance_id != self.instance_id {
                return Err(AuthorityError::WrongInstance);
            }
            let index = snapshot
                .grant
                .assignments
                .binary_search(&context.assignment)
                .map_err(|_| AuthorityError::LeaseExpired)?;
            if snapshot.continuity[index] != context.continuity {
                return Err(AuthorityError::LeaseExpired);
            }
        }
        Ok(())
    }
}

fn live_snapshot(state: &AuthorityState, now: u64) -> Result<&AuthoritySnapshot, AuthorityError> {
    state
        .current
        .as_ref()
        .filter(|snapshot| now < snapshot.local_deadline_ms)
        .ok_or(AuthorityError::LeaseExpired)
}
