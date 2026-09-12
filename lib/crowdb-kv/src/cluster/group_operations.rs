// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Consistent in-process KV operations for one Paxos group.
//!
//! RPC adapters and server-internal control planes share this layer so read
//! barriers, proposal admission, deduplication, apply fences, and leader-tenure
//! fencing cannot diverge.

#![allow(clippy::cast_possible_truncation)]

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use thiserror::Error;

use crate::cluster::group::{ProposeResult, PxGroup};
use crate::cluster::group_election::{LeaderElection, ReadBarrierOutcome};
use crate::cluster::local_replica::PxLocalReplicaRole;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvReadConsistency {
    Linearizable,
    MinAppliedSlot(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvRequestIdentity {
    pub client_id: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvGroupMutation {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvGroupRead {
    pub value: Option<Bytes>,
    pub revision: u64,
    pub read_slot: u64,
    pub safe_slot: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvGroupWrite {
    pub chosen_slot: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvGroupScanItem {
    pub key: Bytes,
    pub value: Bytes,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvGroupScanRequest {
    pub prefix: Bytes,
    pub start_after: Bytes,
    pub end_key: Bytes,
    pub limit: usize,
    pub consistency: KvReadConsistency,
    pub keys_only: bool,
    pub count_only: bool,
    pub deadline_ms: u64,
    pub bounded: bool,
    pub requested_scan_cutoff: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvGroupScan {
    pub items: Vec<KvGroupScanItem>,
    pub count: u64,
    pub truncated: bool,
    pub timed_out: bool,
    pub read_slot: u64,
    pub scan_cutoff: u64,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum KvGroupOperationError {
    #[error("not leader")]
    NotLeader { leader_hint: String },
    #[error("{0}")]
    Unavailable(String),
    #[error("proposal admission is busy")]
    Busy,
    #[error("compare-and-set precondition failed")]
    CompareFailed { current_revision: u64 },
    #[error("compare-and-set admission is busy")]
    CompareBusy,
    #[error("proposal outcome is unknown")]
    OutcomeUnknown,
    #[error("{0}")]
    Internal(String),
}

#[derive(Clone)]
pub struct KvGroupOperations {
    group: Arc<PxGroup>,
    scan_byte_budget: usize,
    required_leader_term: Option<u64>,
}

impl KvGroupOperations {
    #[must_use]
    pub fn new(group: Arc<PxGroup>, scan_byte_budget: usize) -> Self {
        Self {
            group,
            scan_byte_budget,
            required_leader_term: None,
        }
    }

    /// Acquire a linearizable barrier and bind all subsequent operations to
    /// the current leadership term.
    ///
    /// # Errors
    ///
    /// Returns `NotLeader` or `Unavailable` when the local replica cannot
    /// establish a linearizable read point in its current term.
    pub async fn bind_current_leader_tenure(mut self) -> Result<Self, KvGroupOperationError> {
        let term = self.group.local_replica().current_term_snapshot();
        self.required_leader_term = Some(term);
        self.resolve_read_point(KvReadConsistency::Linearizable).await?;
        self.ensure_tenure()?;
        Ok(self)
    }

    #[must_use]
    pub fn leader_term(&self) -> Option<u64> {
        self.required_leader_term
    }

    /// Read one key under the requested consistency discipline.
    ///
    /// # Errors
    ///
    /// Returns a routing or availability error when the read barrier cannot
    /// be established, or when a bound leader tenure has ended.
    pub async fn get(
        &self,
        key: &[u8],
        consistency: KvReadConsistency,
    ) -> Result<KvGroupRead, KvGroupOperationError> {
        let (read_slot, safe_slot) = self.resolve_read_point(consistency).await?;
        let engine_start = Instant::now();
        let value = self.group.local_replica().learner.engine_get_bytes(key).await;
        if let Some(handles) = self.group.read_handles() {
            handles
                .engine_get
                .observe(engine_start.elapsed().as_nanos() as u64);
        }
        self.ensure_tenure()?;
        let (revision, value) = value.map_or((0, None), |(revision, value)| (revision, Some(value)));
        Ok(KvGroupRead {
            value,
            revision,
            read_slot,
            safe_slot,
        })
    }

    /// Propose a mutation batch and return its chosen slot.
    ///
    /// # Errors
    ///
    /// Returns a typed admission, leadership, or Paxos failure.
    pub async fn write(
        &self,
        mutations: &[KvGroupMutation],
        identity: Option<KvRequestIdentity>,
    ) -> Result<KvGroupWrite, KvGroupOperationError> {
        self.ensure_tenure()?;
        let payload = encode_mutations(mutations);
        let result = if let Some(term) = self.required_leader_term {
            self.group
                .propose_in_tenure(
                    payload,
                    identity.map(|id| id.client_id),
                    identity.map(|id| id.sequence),
                    term,
                )
                .await
        } else {
            self.group
                .propose(
                    payload,
                    identity.map(|id| id.client_id),
                    identity.map(|id| id.sequence),
                )
                .await
        };
        self.finish_write(result, false).await
    }

    /// Propose a revision-conditional mutation and wait through local apply.
    ///
    /// # Errors
    ///
    /// Returns `CompareFailed` on a revision mismatch or another typed
    /// admission, leadership, or Paxos failure.
    pub async fn compare_and_write(
        &self,
        mutations: &[KvGroupMutation],
        precondition_key: Bytes,
        expected_revision: u64,
        identity: KvRequestIdentity,
    ) -> Result<KvGroupWrite, KvGroupOperationError> {
        let payload = encode_mutations(mutations);
        self.compare_encoded(payload, precondition_key, expected_revision, identity)
            .await
    }

    pub(crate) async fn compare_encoded(
        &self,
        payload: Vec<u8>,
        precondition_key: Bytes,
        expected_revision: u64,
        identity: KvRequestIdentity,
    ) -> Result<KvGroupWrite, KvGroupOperationError> {
        self.ensure_tenure()?;
        let result = if let Some(term) = self.required_leader_term {
            self.group
                .propose_cas_in_tenure(
                    payload,
                    precondition_key,
                    expected_revision,
                    identity.client_id,
                    identity.sequence,
                    term,
                )
                .await
        } else {
            self.group
                .propose_cas(
                    payload,
                    precondition_key,
                    expected_revision,
                    identity.client_id,
                    identity.sequence,
                )
                .await
        };
        self.finish_write(result, true).await
    }

    /// Scan one ordered key interval under a stable read cutoff.
    ///
    /// # Errors
    ///
    /// Returns a typed read-barrier, cutoff, tenure, or engine failure.
    pub async fn scan(&self, request: &KvGroupScanRequest) -> Result<KvGroupScan, KvGroupOperationError> {
        let (read_slot, _) = self.resolve_read_point(request.consistency).await?;
        let scan_cutoff = resolve_scan_cutoff(request, read_slot)?;
        let engine_keys_only = request.keys_only || request.count_only;
        let engine_byte_budget = if request.count_only || request.bounded {
            0
        } else {
            self.scan_byte_budget
        };
        let engine_limit = if request.bounded { 0 } else { request.limit };
        let (scanned, engine_truncated) = self
            .group
            .local_replica()
            .learner
            .engine_scan(
                &request.prefix,
                &request.start_after,
                &request.end_key,
                engine_limit,
                engine_byte_budget,
                engine_keys_only,
                request.deadline_ms,
            )
            .await
            .map_err(|error| KvGroupOperationError::Internal(format!("scan engine error: {error}")))?;
        self.ensure_tenure()?;

        if request.count_only {
            return Ok(KvGroupScan {
                count: scanned.len() as u64,
                items: Vec::new(),
                truncated: engine_truncated,
                timed_out: request.deadline_ms != 0 && engine_truncated,
                read_slot,
                scan_cutoff,
            });
        }

        let mut items: Vec<_> = scanned
            .into_iter()
            .filter(|(_, revision, _)| !request.bounded || *revision <= scan_cutoff)
            .map(|(key, revision, value)| KvGroupScanItem { key, value, revision })
            .collect();
        let bounded_truncated = request.limit != 0 && items.len() > request.limit;
        if bounded_truncated {
            items.truncate(request.limit);
        }
        let truncated = if request.bounded {
            bounded_truncated
        } else {
            engine_truncated
        };
        Ok(KvGroupScan {
            count: 0,
            items,
            truncated,
            timed_out: request.deadline_ms != 0 && truncated,
            read_slot,
            scan_cutoff,
        })
    }

    async fn finish_write(
        &self,
        result: ProposeResult,
        await_apply: bool,
    ) -> Result<KvGroupWrite, KvGroupOperationError> {
        match result {
            ProposeResult::Chosen { slot } => {
                if await_apply {
                    self.group.local_replica().await_apply_fence(slot).await;
                }
                self.ensure_tenure()?;
                Ok(KvGroupWrite { chosen_slot: slot })
            }
            ProposeResult::NotLeader { leader_hint } => Err(KvGroupOperationError::NotLeader { leader_hint }),
            ProposeResult::Busy => Err(KvGroupOperationError::Busy),
            ProposeResult::CasFailed { current_revision } => {
                Err(KvGroupOperationError::CompareFailed { current_revision })
            }
            ProposeResult::CasBusy => Err(KvGroupOperationError::CompareBusy),
            ProposeResult::OutcomeUnknown => Err(KvGroupOperationError::OutcomeUnknown),
            ProposeResult::Err(error) => Err(KvGroupOperationError::Internal(error)),
        }
    }

    pub(crate) async fn resolve_read_point(
        &self,
        consistency: KvReadConsistency,
    ) -> Result<(u64, u64), KvGroupOperationError> {
        self.ensure_tenure()?;
        let replica = self.group.local_replica();
        let safe_slot = self.group.group_safe_slot();
        if let Some(handles) = self.group.read_handles() {
            handles.safe_slot.set(safe_slot);
        }
        let read_slot = match consistency {
            KvReadConsistency::Linearizable => {
                if !replica.is_leader() {
                    return Err(self.not_leader());
                }
                match self.group.linearizable_read_barrier().await {
                    ReadBarrierOutcome::Ready { read_slot } => {
                        let fence_start = Instant::now();
                        replica.await_apply_fence(read_slot).await;
                        if let Some(handles) = self.group.read_handles() {
                            handles
                                .apply_fence
                                .observe(fence_start.elapsed().as_nanos() as u64);
                        }
                        read_slot
                    }
                    ReadBarrierOutcome::NotLeader => return Err(self.not_leader()),
                    ReadBarrierOutcome::NoQuorum => {
                        return Err(KvGroupOperationError::Unavailable(
                            "linearizable read: leadership quorum unavailable".to_string(),
                        ));
                    }
                }
            }
            KvReadConsistency::MinAppliedSlot(min_slot) => {
                let contiguous_applied = replica.contiguous_applied();
                if contiguous_applied < min_slot {
                    if let Some(handles) = self.group.read_handles() {
                        handles.minslot_fallback.inc();
                    }
                    return Err(self.not_leader());
                }
                contiguous_applied
            }
        };
        self.ensure_tenure()?;
        Ok((read_slot, safe_slot))
    }

    fn ensure_tenure(&self) -> Result<(), KvGroupOperationError> {
        let Some(required_term) = self.required_leader_term else {
            return Ok(());
        };
        let replica = self.group.local_replica();
        if replica.role() == PxLocalReplicaRole::Leader
            && replica.current_term_snapshot() == required_term
            && self.group.proposing_term() == required_term
        {
            Ok(())
        } else {
            Err(self.not_leader())
        }
    }

    fn not_leader(&self) -> KvGroupOperationError {
        KvGroupOperationError::NotLeader {
            leader_hint: self.group.leader_endpoint().unwrap_or_default(),
        }
    }
}

#[must_use]
pub fn encode_mutations(mutations: &[KvGroupMutation]) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&(mutations.len() as u16).to_le_bytes());
    for mutation in mutations {
        let (key, value) = match mutation {
            KvGroupMutation::Put { key, value } => (key.as_ref(), Some(value.as_ref())),
            KvGroupMutation::Delete { key } => (key.as_ref(), None),
        };
        buffer.push(u8::from(value.is_none()));
        buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buffer.extend_from_slice(key);
        let value_len = value.map_or(0, <[u8]>::len) as u32;
        buffer.extend_from_slice(&value_len.to_le_bytes());
        if let Some(value) = value {
            buffer.extend_from_slice(value);
        }
    }
    buffer
}

fn resolve_scan_cutoff(request: &KvGroupScanRequest, read_slot: u64) -> Result<u64, KvGroupOperationError> {
    if !request.bounded {
        return Ok(0);
    }
    if request.requested_scan_cutoff == 0 {
        return Ok(read_slot);
    }
    if request.requested_scan_cutoff > read_slot {
        return Err(KvGroupOperationError::Unavailable(format!(
            "bounded scan cutoff {} exceeds contiguous applied {read_slot}",
            request.requested_scan_cutoff
        )));
    }
    Ok(request.requested_scan_cutoff)
}
