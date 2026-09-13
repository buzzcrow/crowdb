// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Leader-local guarded admission for revision-conditional mutations.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::cluster::group::{CasOwnerToken, ProposeResult, PxGroup};
use crate::cluster::local_replica::PxLocalReplicaRole;
use crate::paxos::roles::RequestIdentity;

impl PxGroup {
    pub async fn propose_cas(
        self: &Arc<Self>,
        payload: Vec<u8>,
        precondition_key: Bytes,
        expected_revision: u64,
        client_id: u64,
        seq: u64,
    ) -> ProposeResult {
        self.propose_cas_with_tenure(payload, precondition_key, expected_revision, client_id, seq, None)
            .await
    }

    /// Execute a conditional proposal only in `required_term`.
    pub async fn propose_cas_in_tenure(
        self: &Arc<Self>,
        payload: Vec<u8>,
        precondition_key: Bytes,
        expected_revision: u64,
        client_id: u64,
        seq: u64,
        required_term: u64,
    ) -> ProposeResult {
        self.propose_cas_with_tenure(
            payload,
            precondition_key,
            expected_revision,
            client_id,
            seq,
            Some(required_term),
        )
        .await
    }

    async fn propose_cas_with_tenure(
        self: &Arc<Self>,
        payload: Vec<u8>,
        precondition_key: Bytes,
        expected_revision: u64,
        client_id: u64,
        seq: u64,
        required_term: Option<u64>,
    ) -> ProposeResult {
        let group = Arc::clone(self);
        match tokio::spawn(async move {
            group
                .propose_cas_owned(
                    payload,
                    precondition_key,
                    expected_revision,
                    client_id,
                    seq,
                    required_term,
                )
                .await
        })
        .await
        {
            Ok(result) => result,
            Err(error) => {
                self.leader_read_ready.store(false, Ordering::Release);
                ProposeResult::Err(format!("conditional proposal task failed: {error}"))
            }
        }
    }

    async fn propose_cas_owned(
        &self,
        payload: Vec<u8>,
        precondition_key: Bytes,
        expected_revision: u64,
        client_id: u64,
        seq: u64,
        required_term: Option<u64>,
    ) -> ProposeResult {
        if client_id == 0 {
            return ProposeResult::Err("conditional write requires nonzero client_id".into());
        }
        if let Some(slot) = self.local_replica.learner.request_result_lookup(client_id, seq) {
            return ProposeResult::Chosen { slot };
        }

        let tenure = self.proposing_term.load(Ordering::Acquire);
        if !self.cas_admission_ready(tenure, required_term) {
            return self.cas_not_ready_result();
        }
        let token = CasOwnerToken {
            tenure,
            nonce: self.cas_request_nonce.fetch_add(1, Ordering::Relaxed),
        };
        let entry = self
            .cas_transient_map
            .compare_insert(precondition_key.clone(), token, |owner| owner.tenure != tenure);
        if *entry.value() != token {
            return ProposeResult::CasBusy;
        }

        let observed = self
            .local_replica
            .learner
            .engine_get_versioned(&precondition_key)
            .await;
        let result = match observed {
            Err(error) => ProposeResult::Err(error),
            Ok(value) => {
                let current_revision = value.as_ref().map_or(0, |(revision, _)| *revision);
                if current_revision != expected_revision {
                    ProposeResult::CasFailed { current_revision }
                } else if !self.cas_admission_ready(tenure, required_term) {
                    self.cas_not_ready_result()
                } else {
                    let tag = [RequestIdentity { client_id, seq }];
                    if let Some(required_term) = required_term {
                        self.propose_inner_conditional_in_tenure(
                            Bytes::from(payload),
                            &tag,
                            &precondition_key,
                            expected_revision,
                            required_term,
                        )
                        .await
                    } else {
                        self.propose_inner_conditional(
                            Bytes::from(payload),
                            &tag,
                            &precondition_key,
                            expected_revision,
                        )
                        .await
                    }
                }
            }
        };

        if let ProposeResult::Chosen { slot } = result {
            self.local_replica.await_apply_fence(slot).await;
            entry.remove();
            ProposeResult::Chosen { slot }
        } else {
            if matches!(result, ProposeResult::Err(_) | ProposeResult::OutcomeUnknown) {
                self.leader_read_ready.store(false, Ordering::Release);
            }
            entry.remove();
            result
        }
    }

    fn cas_admission_ready(&self, tenure: u64, required_term: Option<u64>) -> bool {
        self.leader_read_ready.load(Ordering::Acquire)
            && self.local_replica.role() == PxLocalReplicaRole::Leader
            && self.local_replica.current_term_snapshot() == tenure
            && required_term.map_or(true, |term| term == tenure)
    }

    fn cas_not_ready_result(&self) -> ProposeResult {
        if self.local_replica.role() == PxLocalReplicaRole::Leader {
            ProposeResult::OutcomeUnknown
        } else {
            ProposeResult::NotLeader {
                leader_hint: self.leader_endpoint().unwrap_or_default(),
            }
        }
    }
}
