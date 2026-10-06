// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Concurrent owner writes; only ownership changes drain admitted work.

use super::group::{ProposeResult, PxGroup};
use crate::paxos::roles::RequestIdentity;
use bytes::Bytes;
use crowdb_protocol::owner_fence::is_owner_fence_key;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

const CLOSED: u64 = 1 << 63;

pub(crate) struct OwnerAdmission {
    tenure: u64,
    state: AtomicU64,
    unresolved: AtomicBool,
    drained: Notify,
}

struct OwnerWrite {
    admission: Arc<OwnerAdmission>,
    pending_proposal: bool,
}
impl Drop for OwnerWrite {
    fn drop(&mut self) {
        if self.pending_proposal {
            self.admission.unresolved.store(true, Ordering::Release);
        }
        if self.admission.state.fetch_sub(1, Ordering::AcqRel) == CLOSED + 1 {
            self.admission.drained.notify_waiters();
        }
    }
}

pub(crate) struct OwnerChange {
    admission: Arc<OwnerAdmission>,
    pending_proposal: bool,
}
impl OwnerChange {
    pub(crate) fn unresolved(&self) -> bool {
        self.admission.unresolved.load(Ordering::Acquire)
    }

    pub(crate) fn begin_proposal(&mut self) {
        self.pending_proposal = true;
    }

    pub(crate) fn finish_proposal(&mut self) {
        self.pending_proposal = false;
    }

    pub(crate) fn mark_unresolved(&self) {
        self.admission.unresolved.store(true, Ordering::Release);
    }
}
impl Drop for OwnerChange {
    fn drop(&mut self) {
        if self.pending_proposal {
            self.mark_unresolved();
        }
        self.admission.state.fetch_and(!CLOSED, Ordering::AcqRel);
    }
}

impl OwnerAdmission {
    fn admit(self: &Arc<Self>) -> Option<OwnerWrite> {
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state < CLOSED - 1).then_some(state + 1)
            })
            .ok()
            .map(|_| OwnerWrite {
                admission: Arc::clone(self),
                pending_proposal: false,
            })
    }
}

impl PxGroup {
    pub(crate) fn owner_admission_status(&self) -> (u64, u64) {
        let tenure = self.proposing_term.load(Ordering::Acquire);
        self.owner_admission
            .iter()
            .filter(|entry| entry.value().tenure == tenure)
            .fold((0, 0), |(active, closed), entry| {
                let state = entry.value().state.load(Ordering::Acquire);
                (
                    active.saturating_add(state & !CLOSED),
                    closed + u64::from(state & CLOSED != 0),
                )
            })
    }

    fn owner_admission(&self, key: Bytes, tenure: u64) -> Arc<OwnerAdmission> {
        if let Some(entry) = self.owner_admission.get(&key) {
            if entry.value().tenure >= tenure {
                return Arc::clone(entry.value());
            }
        }
        let replacement = Arc::new(OwnerAdmission {
            tenure,
            state: AtomicU64::new(0),
            unresolved: AtomicBool::new(false),
            drained: Notify::new(),
        });
        let entry = self
            .owner_admission
            .compare_insert(key, replacement, |current| current.tenure < tenure);
        Arc::clone(entry.value())
    }

    pub(crate) async fn begin_owner_change(&self, key: &Bytes, tenure: u64) -> Option<OwnerChange> {
        if !is_owner_fence_key(key) {
            return None;
        }
        let admission = self.owner_admission(key.clone(), tenure);
        if admission.tenure != tenure {
            return None;
        }
        admission.state.fetch_or(CLOSED, Ordering::AcqRel);
        let change = OwnerChange {
            admission,
            pending_proposal: false,
        };
        loop {
            let notified = change.admission.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if change.admission.state.load(Ordering::Acquire) == CLOSED {
                break;
            }
            notified.await;
        }
        Some(change)
    }

    pub(crate) async fn propose_owner_write(
        self: &Arc<Self>,
        payload: Vec<u8>,
        key: Bytes,
        expected_value: Bytes,
        record_condition: Option<(Bytes, u64)>,
        client_id: u64,
        seq: u64,
    ) -> ProposeResult {
        let group = Arc::clone(self);
        match tokio::spawn(async move {
            group
                .propose_owner_write_owned(payload, key, expected_value, record_condition, client_id, seq)
                .await
        })
        .await
        {
            Ok(result) => result,
            Err(error) => {
                self.leader_read_ready.store(false, Ordering::Release);
                ProposeResult::Err(format!("owner write task failed: {error}"))
            }
        }
    }

    async fn propose_owner_write_owned(
        self: &Arc<Self>,
        payload: Vec<u8>,
        key: Bytes,
        expected_value: Bytes,
        record_condition: Option<(Bytes, u64)>,
        client_id: u64,
        seq: u64,
    ) -> ProposeResult {
        let tenure = self.proposing_term.load(Ordering::Acquire);
        if !self.cas_admission_ready(tenure, Some(tenure)) {
            return self.cas_not_ready_result();
        }
        if let Some(slot) = self.local_replica.learner.request_result_lookup(client_id, seq) {
            return ProposeResult::Chosen { slot };
        }
        let admission = self.owner_admission(key.clone(), tenure);
        if admission.tenure != tenure {
            return self.cas_not_ready_result();
        }
        if admission.unresolved.load(Ordering::Acquire) {
            self.leader_read_ready.store(false, Ordering::Release);
            return ProposeResult::OutcomeUnknown;
        }
        let Some(mut write) = admission.admit() else {
            return ProposeResult::CasBusy;
        };
        let revision = match self.local_replica.learner.engine_get_versioned(&key).await {
            Ok(Some((revision, value))) if value == expected_value => revision,
            Ok(value) => {
                return ProposeResult::CasFailed {
                    current_revision: value.map_or(0, |(revision, _)| revision),
                }
            }
            Err(error) => return ProposeResult::Err(error),
        };
        if !self.cas_admission_ready(tenure, Some(tenure)) {
            return self.cas_not_ready_result();
        }
        // A panic or dropped proposal future must poison this tenure before
        // releasing admission, including across topology replacement.
        write.pending_proposal = true;
        let result = if let Some((record, expected)) = record_condition {
            self.propose_cas_in_tenure(payload, record, expected, client_id, seq, tenure)
                .await
        } else {
            let identity = [RequestIdentity { client_id, seq }];
            self.propose_inner_conditional_in_tenure(Bytes::from(payload), &identity, &key, revision, tenure)
                .await
        };
        match &result {
            ProposeResult::Chosen { slot } => {
                self.local_replica.await_apply_fence(*slot).await;
                write.pending_proposal = false;
            }
            ProposeResult::OutcomeUnknown | ProposeResult::Err(_) => {
                admission.unresolved.store(true, Ordering::Release);
                self.leader_read_ready.store(false, Ordering::Release);
            }
            ProposeResult::NotLeader { .. } => {}
            _ => write.pending_proposal = false,
        }
        result
    }
}

#[cfg(feature = "test-util")]
impl PxGroup {
    /// Retain an admitted write while testing the ownership handover barrier.
    ///
    /// # Panics
    /// Panics when the current owner admission is closed.
    pub fn hold_owner_write_for_tests(&self, key: Bytes) -> impl Send {
        let tenure = self.proposing_term.load(Ordering::Acquire);
        self.owner_admission(key, tenure)
            .admit()
            .expect("owner admission is open")
    }

    /// Retain an admitted proposal whose outcome has not been resolved.
    ///
    /// # Panics
    /// Panics when the current owner admission is closed.
    pub fn hold_owner_proposal_for_tests(&self, key: Bytes) -> impl Send {
        let tenure = self.proposing_term.load(Ordering::Acquire);
        let mut write = self
            .owner_admission(key, tenure)
            .admit()
            .expect("owner admission is open");
        write.pending_proposal = true;
        write
    }

    /// Whether the current tenure has closed owner write admission.
    #[must_use]
    pub fn owner_change_pending_for_tests(&self, key: &Bytes) -> bool {
        self.owner_admission
            .get(key)
            .is_some_and(|entry| entry.value().state.load(Ordering::Acquire) & CLOSED != 0)
    }

    /// Exercise the same admitted proposal as the owner-fenced RPC handler.
    #[allow(clippy::too_many_arguments)]
    pub async fn propose_owner_write_for_tests(
        self: &Arc<Self>,
        payload: Vec<u8>,
        key: Bytes,
        expected_value: Bytes,
        record_condition: Option<(Bytes, u64)>,
        client_id: u64,
        seq: u64,
    ) -> ProposeResult {
        self.propose_owner_write(payload, key, expected_value, record_condition, client_id, seq)
            .await
    }
}
