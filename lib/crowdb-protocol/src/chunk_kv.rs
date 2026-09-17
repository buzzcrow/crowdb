// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable catalog, monitor, and serving-grant types for chunk-backed KV.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::chunk_stream::StreamName;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Id128 {
    pub high: u64,
    pub low: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && self.end.as_deref().map_or(true, |end| key < end)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkKvRangeCatalogPartitionState {
    #[default]
    Prepared,
    Serving,
    SplitPreparing,
    SplitFenced,
    Transferring,
    TargetCatchingUp,
    Retired,
    Faulted,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerDescriptor {
    pub instance_id: u64,
    pub rpc_endpoint: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailOverlayArtifact {
    pub source_partition_id: Id128,
    pub source_epoch: u64,
    pub source_stream_name: StreamName,
    pub source_stream_manifest_generation: u64,
    pub replay_offset: u64,
    pub cutover_offset: u64,
    pub base_tree_manifest: u64,
    pub base_applied_seq: u64,
    pub cutover_seq: u64,
    pub target_stream_start_seq: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionArtifact {
    pub tree_id: u64,
    pub stream_name: StreamName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_overlay: Option<TailOverlayArtifact>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkKvRangeCatalogEntry {
    pub partition_id: Id128,
    pub range: KeyRange,
    pub owner: OwnerDescriptor,
    pub owner_epoch: u64,
    pub state: ChunkKvRangeCatalogPartitionState,
    pub artifact: PartitionArtifact,
    pub transition_id: Option<Id128>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkKvRangeCatalogPage {
    pub generation: u64,
    pub page_index: u64,
    pub entries: Vec<ChunkKvRangeCatalogEntry>,
    pub checksum: [u8; 32],
}

impl ChunkKvRangeCatalogPage {
    /// Computes and installs the canonical checksum for this page.
    ///
    /// # Errors
    ///
    /// Returns an encoding error if the page cannot be serialized.
    pub fn seal(&mut self) -> Result<(), ChunkKvProtocolError> {
        self.checksum = page_checksum(self)?;
        Ok(())
    }

    /// Validates identity, ordering, and checksum before publication.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error for malformed page content.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if self.generation == 0 || self.entries.is_empty() || self.checksum != page_checksum(self)? {
            return Err(ChunkKvProtocolError::InvalidCatalogPage);
        }
        for entry in &self.entries {
            validate_entry(entry)?;
        }
        if self
            .entries
            .windows(2)
            .any(|pair| pair[0].range.start >= pair[1].range.start)
        {
            return Err(ChunkKvProtocolError::InvalidCatalogPage);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkKvRangeCatalogPageRef {
    pub page_generation: u64,
    pub page_index: u64,
    pub first_key: Vec<u8>,
    pub page_checksum: [u8; 32],
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkKvRangeCatalogHead {
    pub generation: u64,
    pub previous_generation: Option<u64>,
    pub pages: Vec<ChunkKvRangeCatalogPageRef>,
    pub checksum: [u8; 32],
}

impl ChunkKvRangeCatalogHead {
    /// Computes and installs the canonical checksum for this head.
    ///
    /// # Errors
    ///
    /// Returns an encoding error if the head cannot be serialized.
    pub fn seal(&mut self) -> Result<(), ChunkKvProtocolError> {
        self.checksum = head_checksum(self)?;
        Ok(())
    }

    /// Validates the head and every referenced page as one complete keyspace.
    ///
    /// # Errors
    ///
    /// Returns an error for bad checksums, identities, ordering, holes,
    /// overlaps, or incomplete binary-keyspace coverage.
    pub fn validate_pages(&self, pages: &[ChunkKvRangeCatalogPage]) -> Result<(), ChunkKvProtocolError> {
        if self.generation == 0
            || self.pages.is_empty()
            || self.checksum != head_checksum(self)?
            || pages.len() != self.pages.len()
            || self
                .previous_generation
                .is_some_and(|previous| previous >= self.generation)
        {
            return Err(ChunkKvProtocolError::InvalidCatalogHead);
        }
        let mut entries = Vec::new();
        for (reference, page) in self.pages.iter().zip(pages) {
            page.validate()?;
            if page.generation != reference.page_generation
                || page.generation > self.generation
                || page.page_index != reference.page_index
                || page.checksum != reference.page_checksum
                || page.entries.first().map(|entry| &entry.range.start) != Some(&reference.first_key)
            {
                return Err(ChunkKvProtocolError::InvalidCatalogHead);
            }
            entries.extend(page.entries.iter());
        }
        validate_complete_entries(&entries)
    }

    /// Validates a monotonic successor and prevents ownership epoch regression.
    ///
    /// # Errors
    ///
    /// Returns an error if either generation is invalid, the generation does
    /// not advance, or a retained partition's ownership epoch decreases.
    pub fn validate_successor(
        &self,
        pages: &[ChunkKvRangeCatalogPage],
        previous: &ChunkKvRangeCatalogHead,
        previous_pages: &[ChunkKvRangeCatalogPage],
    ) -> Result<(), ChunkKvProtocolError> {
        previous.validate_pages(previous_pages)?;
        self.validate_pages(pages)?;
        if self.generation <= previous.generation {
            return Err(ChunkKvProtocolError::CatalogRegression);
        }
        let prior_epochs: HashMap<Id128, u64> = previous_pages
            .iter()
            .flat_map(|page| page.entries.iter())
            .map(|entry| (entry.partition_id, entry.owner_epoch))
            .collect();
        if pages.iter().flat_map(|page| page.entries.iter()).any(|entry| {
            prior_epochs
                .get(&entry.partition_id)
                .is_some_and(|previous_epoch| entry.owner_epoch < *previous_epoch)
        }) {
            return Err(ChunkKvProtocolError::CatalogRegression);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainFailurePolicy {
    #[default]
    AutomaticSharedStorage,
    OperatorOnly,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkKvRangeBalancePolicy {
    pub target_partitions_per_owner: u32,
    pub target_partition_bytes: u64,
    pub minimum_weighted_improvement_percent: u32,
    pub cooldown_ms: u64,
    pub max_owner_request_rate: u64,
}

impl Default for ChunkKvRangeBalancePolicy {
    fn default() -> Self {
        Self {
            target_partitions_per_owner: 4,
            target_partition_bytes: 1 << 30,
            minimum_weighted_improvement_percent: 25,
            cooldown_ms: 10 * 60 * 1_000,
            max_owner_request_rate: 0,
        }
    }
}

impl ChunkKvRangeBalancePolicy {
    /// Validates persisted chunk-KV balancing bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for zero sizing/cooldown or a percentage above 100.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if self.target_partitions_per_owner == 0
            || self.target_partition_bytes == 0
            || self.minimum_weighted_improvement_percent > 100
            || self.cooldown_ms == 0
        {
            return Err(ChunkKvProtocolError::InvalidMonitorDescriptor);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainMonitorDescriptor {
    pub domain: String,
    pub service_registry_name: String,
    pub driver_version: u32,
    pub capability_version: u32,
    pub heartbeat_interval_ms: u64,
    pub suspect_after_ms: u64,
    pub dead_after_ms: u64,
    pub lease_duration_ms: u64,
    pub max_clock_skew_ms: u64,
    pub self_fence_margin_ms: u64,
    pub failure_policy: DomainFailurePolicy,
    pub balance_policy: String,
    #[serde(default)]
    pub chunk_kv_range_balance: Option<ChunkKvRangeBalancePolicy>,
}

impl DomainMonitorDescriptor {
    /// Validates ordered liveness and conservative lease timing bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for missing identity/version or unsafe timing.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if !valid_name(&self.domain)
            || !valid_name(&self.service_registry_name)
            || self.driver_version == 0
            || self.capability_version == 0
            || self.balance_policy.is_empty()
            || self.heartbeat_interval_ms == 0
            || self.suspect_after_ms < self.heartbeat_interval_ms
            || self.dead_after_ms < self.suspect_after_ms
            || self
                .max_clock_skew_ms
                .checked_add(self.self_fence_margin_ms)
                .map_or(true, |margin| self.lease_duration_ms <= margin)
            || self
                .dead_after_ms
                .checked_add(self.self_fence_margin_ms)
                .map_or(true, |deadline| deadline > self.lease_duration_ms)
        {
            return Err(ChunkKvProtocolError::InvalidMonitorDescriptor);
        }
        if self.domain == "chunk-kv" {
            if let Some(policy) = &self.chunk_kv_range_balance {
                policy.validate()?;
            } else {
                ChunkKvRangeBalancePolicy::default().validate()?;
            }
        } else if self.chunk_kv_range_balance.is_some() {
            return Err(ChunkKvProtocolError::InvalidMonitorDescriptor);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnsureDomainMonitorRequest {
    pub descriptor: DomainMonitorDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnsureDomainMonitorOutcome {
    Created,
    AlreadyExists,
    DescriptorConflict,
    UnsupportedMonitorDomain,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceHealth {
    #[default]
    Healthy,
    Suspect,
    Dead,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedPartition {
    pub partition_id: Id128,
    pub owner_epoch: u64,
    pub recovering: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkKvInstanceObservation {
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub last_heartbeat_ms: u64,
    pub capacity_bytes: u64,
    pub durable_bytes: u64,
    pub request_rate: u64,
    pub hosted: Vec<HostedPartition>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferPhase {
    #[default]
    Planned,
    SourcePreparing,
    AwaitingFence,
    TargetPreparing,
    TargetPrepared,
    TargetCatchingUp,
    CatchupPublished,
    TargetReady,
    CatalogCommitted,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthorityReleaseProof {
    ExplicitFence {
        source_instance_id: u64,
        source_epoch: u64,
        durable_tail: u64,
        durable_tail_offset: u64,
    },
    LeaseExpired {
        activation_not_before_ms: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetReadinessProof {
    pub target_instance_id: u64,
    pub target_epoch: u64,
    pub artifact: PartitionArtifact,
    pub durable_tail: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferReadinessLimits {
    pub max_tail_records: u64,
    pub max_tail_bytes: u64,
    pub max_estimated_catchup_ms: u64,
    pub prepare_deadline_ms: u64,
    pub forwarding_grace_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferTransition {
    pub transition_id: Id128,
    pub partition_id: Id128,
    pub range: KeyRange,
    pub source: OwnerDescriptor,
    pub source_epoch: u64,
    pub target: OwnerDescriptor,
    pub target_epoch: u64,
    /// Current source tree and stream identity.
    pub artifact: PartitionArtifact,
    /// Target-owned stream plus the pinned source base/tail overlay.
    pub target_artifact: PartitionArtifact,
    pub readiness_limits: TransferReadinessLimits,
    #[serde(default)]
    pub planned_at_ms: u64,
    pub old_grant_expires_at_ms: u64,
    pub phase: TransferPhase,
    pub release_proof: Option<AuthorityReleaseProof>,
    pub readiness_proof: Option<TargetReadinessProof>,
    pub catchup_proof: Option<TargetReadinessProof>,
    pub failure: Option<String>,
}

impl TransferTransition {
    /// Validates a persisted no-copy ownership transfer record.
    ///
    /// # Errors
    ///
    /// Returns an error for missing identity, non-advancing authority, invalid
    /// range/artifact, or proof fields inconsistent with the persisted phase.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        let valid_identity = self.transition_id != Id128::default()
            && self.partition_id != Id128::default()
            && self.source.instance_id != 0
            && self.target.instance_id != 0
            && self.source.instance_id != self.target.instance_id
            && !self.source.rpc_endpoint.is_empty()
            && !self.target.rpc_endpoint.is_empty()
            && self.source_epoch != 0
            && self.target_epoch > self.source_epoch
            && self.artifact.tree_id != 0
            && self.artifact.stream_name != StreamName::default()
            && self.target_artifact.tree_id == self.artifact.tree_id
            && self.target_artifact.stream_name != StreamName::default()
            && (self.target_artifact.stream_name != self.artifact.stream_name
                || matches!(
                    self.release_proof,
                    Some(AuthorityReleaseProof::LeaseExpired { .. })
                ))
            && self.readiness_limits.max_tail_records != 0
            && self.readiness_limits.max_tail_bytes != 0
            && self.readiness_limits.max_estimated_catchup_ms != 0
            && self.readiness_limits.prepare_deadline_ms >= self.planned_at_ms
            && self.readiness_limits.forwarding_grace_ms != 0
            && self
                .range
                .end
                .as_ref()
                .map_or(true, |end| self.range.start < *end);
        if !valid_identity {
            return Err(ChunkKvProtocolError::InvalidTransferTransition);
        }
        if let Some(proof) = &self.release_proof {
            match proof {
                AuthorityReleaseProof::ExplicitFence {
                    source_instance_id,
                    source_epoch,
                    ..
                } if *source_instance_id == self.source.instance_id && *source_epoch == self.source_epoch => {
                }
                AuthorityReleaseProof::LeaseExpired {
                    activation_not_before_ms,
                } if *activation_not_before_ms >= self.old_grant_expires_at_ms => {}
                _ => return Err(ChunkKvProtocolError::InvalidTransferTransition),
            }
        }
        if let Some(proof) = &self.readiness_proof {
            if proof.target_instance_id != self.target.instance_id
                || proof.target_epoch != self.target_epoch
                || proof.artifact.tree_id != self.target_artifact.tree_id
                || proof.artifact.stream_name != self.target_artifact.stream_name
            {
                return Err(ChunkKvProtocolError::InvalidTransferTransition);
            }
        }
        if let Some(proof) = &self.catchup_proof {
            if proof.target_instance_id != self.target.instance_id
                || proof.target_epoch != self.target_epoch
                || proof.artifact != self.target_artifact
                || !match self.release_proof {
                    Some(AuthorityReleaseProof::ExplicitFence { durable_tail, .. }) => {
                        proof.durable_tail >= durable_tail
                    }
                    Some(AuthorityReleaseProof::LeaseExpired { .. }) => true,
                    None => false,
                }
            {
                return Err(ChunkKvProtocolError::InvalidTransferTransition);
            }
        }
        let fields_match_phase = match self.phase {
            TransferPhase::Planned | TransferPhase::SourcePreparing => {
                self.release_proof.is_none() && self.readiness_proof.is_none() && self.catchup_proof.is_none()
            }
            TransferPhase::TargetPreparing => {
                (self.target_artifact.tail_overlay.is_some() && self.release_proof.is_none()
                    || self.target_artifact == self.artifact
                        && matches!(
                            self.release_proof,
                            Some(AuthorityReleaseProof::LeaseExpired { .. })
                        ))
                    && self.readiness_proof.is_none()
                    && self.catchup_proof.is_none()
            }
            TransferPhase::TargetPrepared | TransferPhase::AwaitingFence => {
                self.release_proof.is_none() && self.readiness_proof.is_some() && self.catchup_proof.is_none()
            }
            TransferPhase::TargetCatchingUp | TransferPhase::CatchupPublished => {
                self.release_proof.is_some() && self.readiness_proof.is_some() && self.catchup_proof.is_none()
            }
            TransferPhase::TargetReady | TransferPhase::CatalogCommitted => {
                self.release_proof.is_some() && self.readiness_proof.is_some() && self.catchup_proof.is_some()
            }
            TransferPhase::Aborted => self.failure.as_ref().is_some_and(|failure| !failure.is_empty()),
        };
        if !fields_match_phase {
            return Err(ChunkKvProtocolError::InvalidTransferTransition);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitPhase {
    #[default]
    Planned,
    ParentPreparing,
    ChildrenPrepared,
    CatalogCommitted,
    Aborted,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitChildAssignment {
    pub partition_id: Id128,
    pub range: KeyRange,
    pub owner: OwnerDescriptor,
    pub owner_epoch: u64,
    pub artifact: PartitionArtifact,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitReadinessProof {
    pub cutover_seq: u64,
    pub left_applied_seq: u64,
    pub right_applied_seq: u64,
    pub left_tail_overlay: TailOverlayArtifact,
    pub right_tail_overlay: TailOverlayArtifact,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitTransition {
    pub transition_id: Id128,
    pub parent_id: Id128,
    pub parent_range: KeyRange,
    pub parent_owner: OwnerDescriptor,
    pub parent_epoch: u64,
    pub parent_artifact: PartitionArtifact,
    pub split_key: Vec<u8>,
    pub left: SplitChildAssignment,
    pub right: SplitChildAssignment,
    #[serde(default)]
    pub planned_at_ms: u64,
    pub phase: SplitPhase,
    pub readiness_proof: Option<SplitReadinessProof>,
    pub failure: Option<String>,
}

impl SplitTransition {
    /// Validates exact half-open child coverage and phase-bound readiness.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, bounds, artifacts, or phase
    /// fields that cannot represent one atomic parent-to-children cutover.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        let valid_identity = self.transition_id != Id128::default()
            && self.parent_id != Id128::default()
            && self.left.partition_id != Id128::default()
            && self.right.partition_id != Id128::default()
            && self.parent_id != self.left.partition_id
            && self.parent_id != self.right.partition_id
            && self.left.partition_id != self.right.partition_id
            && self.parent_owner.instance_id != 0
            && !self.parent_owner.rpc_endpoint.is_empty()
            && self.parent_epoch != 0
            && valid_artifact(&self.parent_artifact)
            && valid_split_child(&self.left)
            && valid_split_child(&self.right);
        let exact_ranges = self.split_key > self.parent_range.start
            && self
                .parent_range
                .end
                .as_ref()
                .map_or(true, |end| self.split_key < *end)
            && self.left.range.start == self.parent_range.start
            && self.left.range.end.as_ref() == Some(&self.split_key)
            && self.right.range.start == self.split_key
            && self.right.range.end == self.parent_range.end;
        if !valid_identity || !exact_ranges {
            return Err(ChunkKvProtocolError::InvalidSplitTransition);
        }
        if let Some(proof) = &self.readiness_proof {
            if proof.cutover_seq == 0
                || proof.left_applied_seq != proof.cutover_seq
                || proof.right_applied_seq != proof.cutover_seq
                || !self.valid_split_overlay(&proof.left_tail_overlay, &self.left, proof.cutover_seq)
                || !self.valid_split_overlay(&proof.right_tail_overlay, &self.right, proof.cutover_seq)
            {
                return Err(ChunkKvProtocolError::InvalidSplitTransition);
            }
        }
        let fields_match_phase = match self.phase {
            SplitPhase::Planned | SplitPhase::ParentPreparing => {
                self.readiness_proof.is_none() && self.failure.is_none()
            }
            SplitPhase::ChildrenPrepared | SplitPhase::CatalogCommitted => {
                self.readiness_proof.is_some() && self.failure.is_none()
            }
            SplitPhase::Aborted => self.failure.as_ref().is_some_and(|failure| !failure.is_empty()),
        };
        if !fields_match_phase {
            return Err(ChunkKvProtocolError::InvalidSplitTransition);
        }
        Ok(())
    }

    fn valid_split_overlay(
        &self,
        overlay: &TailOverlayArtifact,
        child: &SplitChildAssignment,
        cutover_seq: u64,
    ) -> bool {
        valid_tail_overlay(overlay)
            && child.artifact.tail_overlay.as_ref() == Some(overlay)
            && overlay.source_partition_id == self.parent_id
            && overlay.source_epoch == self.parent_epoch
            && overlay.source_stream_name == self.parent_artifact.stream_name
            && overlay.cutover_seq == cutover_seq
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ServingAssignment {
    pub partition_id: Id128,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServingGrant {
    pub instance_id: u64,
    pub lease_sequence: u64,
    pub catalog_generation: u64,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub assignments: Vec<ServingAssignment>,
    pub assignment_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientRequestId {
    pub client_instance_id: Id128,
    pub client_sequence: u64,
}

impl ClientRequestId {
    /// Validates the stable logical request identity used across retries.
    ///
    /// # Errors
    ///
    /// Returns an error when either identity component is zero.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if self.client_instance_id == Id128::default() || self.client_sequence == 0 {
            return Err(ChunkKvProtocolError::InvalidRpcRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcJournalPosition {
    pub stream_name: Id128,
    pub offset: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestRouting {
    pub request_id: ClientRequestId,
    pub map_revision: u64,
    pub partition_id: Id128,
    pub owner_epoch: u64,
    pub min_journal_position: Option<RpcJournalPosition>,
    pub deadline_ms: Option<u64>,
}

impl RequestRouting {
    /// Validates identity and routing fields before request admission.
    ///
    /// # Errors
    ///
    /// Returns an error when a stable identity or authority field is absent.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        self.request_id.validate()?;
        if self.map_revision == 0 || self.partition_id == Id128::default() || self.owner_epoch == 0 {
            return Err(ChunkKvProtocolError::InvalidRpcRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PointOperation {
    Get {
        key: Vec<u8>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    PutIfAbsent {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    CompareExchange {
        key: Vec<u8>,
        condition: RpcCompareCondition,
        value: Vec<u8>,
    },
    ConditionalDelete {
        key: Vec<u8>,
        condition: RpcCompareCondition,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcCompareCondition {
    Revision(u64),
    Value(Vec<u8>),
}

impl PointOperation {
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get { key }
            | Self::Put { key, .. }
            | Self::Delete { key }
            | Self::PutIfAbsent { key, .. }
            | Self::CompareExchange { key, .. }
            | Self::ConditionalDelete { key, .. } => key,
        }
    }

    #[must_use]
    pub fn is_mutation(&self) -> bool {
        !matches!(self, Self::Get { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointRequest {
    pub routing: RequestRouting,
    pub operation: PointOperation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiGetRequest {
    pub routing: RequestRouting,
    pub keys: Vec<Vec<u8>>,
}

impl MultiGetRequest {
    /// Validates the complete group before any read executes.
    ///
    /// # Errors
    ///
    /// Returns an error when routing is invalid, the group is empty, or any
    /// key falls outside the declared partition range.
    pub fn validate_for_range(&self, range: &KeyRange) -> Result<(), ChunkKvProtocolError> {
        self.routing.validate()?;
        if self.keys.is_empty() || self.keys.iter().any(|key| !range.contains(key)) {
            return Err(ChunkKvProtocolError::InvalidRpcRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiGetResponse {
    pub map_revision: u64,
    pub result: Result<Vec<Option<RpcValue>>, RpcFailure>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRouting {
    pub map_revision: u64,
    pub partition_id: Id128,
    pub owner_epoch: u64,
    pub deadline_ms: Option<u64>,
}

impl PartitionRouting {
    /// Validates authority shared by a partition-local operation group.
    ///
    /// # Errors
    ///
    /// Returns an error when the catalog, partition, or owner epoch is absent.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if self.map_revision == 0 || self.partition_id == Id128::default() || self.owner_epoch == 0 {
            return Err(ChunkKvProtocolError::InvalidRpcRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMutationItem {
    pub request_id: ClientRequestId,
    pub operation: PointOperation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMutationRequest {
    pub routing: PartitionRouting,
    pub operations: Vec<BatchMutationItem>,
}

impl BatchMutationRequest {
    /// Validates the complete group before any operation reaches WAL admission.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid authority, an empty group, a read operation,
    /// invalid request identity, or any key outside the declared partition.
    pub fn validate_for_range(&self, range: &KeyRange) -> Result<(), ChunkKvProtocolError> {
        self.routing.validate()?;
        if self.operations.is_empty()
            || self.operations.iter().any(|item| {
                item.request_id.validate().is_err()
                    || !item.operation.is_mutation()
                    || !range.contains(item.operation.key())
            })
        {
            return Err(ChunkKvProtocolError::InvalidRpcRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMutationResult {
    pub request_id: ClientRequestId,
    pub journal_position: Option<RpcJournalPosition>,
    pub result: Result<OperationResult, RpcFailure>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMutationResponse {
    pub map_revision: u64,
    pub result: Result<Vec<BatchMutationResult>, RpcFailure>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeekKind {
    #[default]
    Ceiling,
    Higher,
    Floor,
    Lower,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeekRequest {
    pub routing: RequestRouting,
    pub key: Vec<u8>,
    pub kind: SeekKind,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScanDirection {
    #[default]
    Forward,
    Reverse,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanContinuation {
    pub direction: ScanDirection,
    pub last_key: Vec<u8>,
    pub partition_id: Id128,
    pub owner_epoch: u64,
    pub map_revision: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanRequest {
    pub routing: RequestRouting,
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub direction: ScanDirection,
    pub limit: u32,
    pub continuation: Option<ScanContinuation>,
}

impl ScanRequest {
    /// Validates the bounded single-partition scan envelope.
    ///
    /// # Errors
    ///
    /// Returns an error for missing routing identity, a zero limit, or an
    /// empty/reversed requested interval.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        self.routing.validate()?;
        if self.limit == 0
            || self
                .start
                .as_ref()
                .zip(self.end.as_ref())
                .is_some_and(|(start, end)| start >= end)
        {
            return Err(ChunkKvProtocolError::InvalidRpcRequest);
        }
        Ok(())
    }

    #[must_use]
    pub fn continuation_matches_topology(&self) -> bool {
        self.continuation.as_ref().map_or(true, |continuation| {
            continuation.direction == self.direction
                && continuation.partition_id == self.routing.partition_id
                && continuation.owner_epoch == self.routing.owner_epoch
                && continuation.map_revision == self.routing.map_revision
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationResult {
    Value(Option<RpcValue>),
    Mutation {
        applied: bool,
        revision: Option<u64>,
        observed: Option<RpcValue>,
    },
    Scan {
        items: Vec<RpcValue>,
        continuation: Option<ScanContinuation>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkKvRpcErrorCode {
    Overloaded,
    WriteStalled,
    Recovering,
    TargetNotReady,
    LeaseExpired,
    RequestExpired,
    RequestConflict,
    NotMyRange,
    RefreshRequired,
    InvalidRequest,
    Internal,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerHint {
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcFailure {
    pub code: ChunkKvRpcErrorCode,
    pub message: String,
    pub retry_after_ms: Option<u64>,
    pub latest_map_revision: Option<u64>,
    pub owner_hint: Option<OwnerHint>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkKvResponse {
    pub map_revision: u64,
    pub journal_position: Option<RpcJournalPosition>,
    pub result: Result<OperationResult, RpcFailure>,
}

impl ServingGrant {
    /// Sorts assignments and installs their canonical digest.
    pub fn seal(&mut self) {
        self.assignments.sort_unstable();
        self.assignment_digest = assignment_digest(&self.assignments);
    }

    /// Validates lease identity, deadline, sorted uniqueness, and digest.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or ambiguous authority.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        if self.instance_id == 0
            || self.lease_sequence == 0
            || self.catalog_generation == 0
            || self.expires_at_ms <= self.issued_at_ms
            || self.assignments.is_empty()
            || self.assignments.windows(2).any(|pair| pair[0] >= pair[1])
            || self
                .assignments
                .iter()
                .any(|assignment| assignment.owner_epoch == 0)
            || self.assignment_digest != assignment_digest(&self.assignments)
        {
            return Err(ChunkKvProtocolError::InvalidServingGrant);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ChunkKvProtocolError {
    #[error("catalog entry is invalid")]
    InvalidCatalogEntry,
    #[error("catalog page is invalid")]
    InvalidCatalogPage,
    #[error("catalog head is invalid")]
    InvalidCatalogHead,
    #[error("catalog does not cover the complete binary keyspace")]
    IncompleteKeyspace,
    #[error("catalog generation or ownership epoch regressed")]
    CatalogRegression,
    #[error("domain monitor descriptor is invalid")]
    InvalidMonitorDescriptor,
    #[error("serving grant is invalid")]
    InvalidServingGrant,
    #[error("chunk KV RPC request is invalid")]
    InvalidRpcRequest,
    #[error("chunk KV transfer transition is invalid")]
    InvalidTransferTransition,
    #[error("chunk KV split transition is invalid")]
    InvalidSplitTransition,
    #[error("protocol record encoding failed")]
    Encoding,
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_artifact(artifact: &PartitionArtifact) -> bool {
    artifact.tree_id != 0
        && artifact.stream_name != StreamName::default()
        && artifact.tail_overlay.as_ref().map_or(true, valid_tail_overlay)
}

fn valid_tail_overlay(overlay: &TailOverlayArtifact) -> bool {
    overlay.source_partition_id != Id128::default()
        && overlay.source_epoch != 0
        && overlay.source_stream_name != StreamName::default()
        && overlay.source_stream_manifest_generation != 0
        && overlay.replay_offset <= overlay.cutover_offset
        && overlay.base_applied_seq <= overlay.cutover_seq
        && overlay.target_stream_start_seq == overlay.cutover_seq.checked_add(1).unwrap_or(0)
}

fn valid_split_child(child: &SplitChildAssignment) -> bool {
    child.owner.instance_id != 0
        && !child.owner.rpc_endpoint.is_empty()
        && child.owner_epoch != 0
        && valid_artifact(&child.artifact)
        && child
            .range
            .end
            .as_ref()
            .map_or(true, |end| child.range.start < *end)
}

fn validate_entry(entry: &ChunkKvRangeCatalogEntry) -> Result<(), ChunkKvProtocolError> {
    if entry.partition_id == Id128::default()
        || entry.owner.instance_id == 0
        || entry.owner.rpc_endpoint.is_empty()
        || entry.owner_epoch == 0
        || entry.artifact.tree_id == 0
        || entry.artifact.stream_name == StreamName::default()
        || entry
            .range
            .end
            .as_ref()
            .is_some_and(|end| entry.range.start >= *end)
    {
        return Err(ChunkKvProtocolError::InvalidCatalogEntry);
    }
    Ok(())
}

fn validate_complete_entries(entries: &[&ChunkKvRangeCatalogEntry]) -> Result<(), ChunkKvProtocolError> {
    if entries
        .first()
        .map_or(true, |entry| !entry.range.start.is_empty())
        || entries.last().map_or(true, |entry| entry.range.end.is_some())
    {
        return Err(ChunkKvProtocolError::IncompleteKeyspace);
    }
    for pair in entries.windows(2) {
        if pair[0].range.end.as_ref() != Some(&pair[1].range.start) {
            return Err(ChunkKvProtocolError::IncompleteKeyspace);
        }
    }
    let mut identities = HashSet::with_capacity(entries.len());
    if entries.iter().any(|entry| !identities.insert(entry.partition_id)) {
        return Err(ChunkKvProtocolError::InvalidCatalogEntry);
    }
    Ok(())
}

fn page_checksum(page: &ChunkKvRangeCatalogPage) -> Result<[u8; 32], ChunkKvProtocolError> {
    hash_encoded(&(page.generation, page.page_index, &page.entries))
}

fn head_checksum(head: &ChunkKvRangeCatalogHead) -> Result<[u8; 32], ChunkKvProtocolError> {
    hash_encoded(&(head.generation, head.previous_generation, &head.pages))
}

#[must_use]
pub fn assignment_digest(assignments: &[ServingAssignment]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for assignment in assignments {
        digest.update(assignment.partition_id.high.to_le_bytes());
        digest.update(assignment.partition_id.low.to_le_bytes());
        digest.update(assignment.owner_epoch.to_le_bytes());
    }
    digest.finalize().into()
}

fn hash_encoded<T: Serialize>(value: &T) -> Result<[u8; 32], ChunkKvProtocolError> {
    let encoded = bincode::serialize(value).map_err(|_| ChunkKvProtocolError::Encoding)?;
    Ok(Sha256::digest(encoded).into())
}
