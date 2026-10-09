// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable split transition plans and proofs.

use super::{
    valid_artifact, valid_split_child, valid_tail_overlay, ChunkKvProtocolError, Id128, KeyRange,
    OwnerDescriptor, PartitionArtifact, TailOverlayArtifact,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitPhase {
    #[default]
    Planned,
    ParentPreparing,
    ChildPrepared,
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
    pub parent_next_epoch: u64,
    /// Durable retained-parent tree/WAL identity produced by the split session.
    pub retained_parent_artifact: PartitionArtifact,
    pub retained_parent_tree_manifest: u64,
    pub retained_parent_root_manifest_generation: u64,
    pub retained_parent_applied_seq: u64,
    pub child_applied_seq: u64,
    pub child_tree_manifest: u64,
    pub child_root_manifest_generation: u64,
    /// Historical dependency for legacy replacement-parent records only.
    #[serde(default)]
    pub retained_parent_tail_overlay: Option<TailOverlayArtifact>,
    pub child_tail_overlay: TailOverlayArtifact,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitTransition {
    pub transition_id: Id128,
    pub parent_id: Id128,
    pub parent_range: KeyRange,
    pub parent_owner: OwnerDescriptor,
    pub parent_epoch: u64,
    pub parent_artifact: PartitionArtifact,
    /// Unchanged tree/WAL identity of the retained parent.
    pub retained_parent_artifact: PartitionArtifact,
    pub parent_next_epoch: u64,
    pub split_key: Vec<u8>,
    pub child: SplitChildAssignment,
    #[serde(default)]
    pub planned_at_ms: u64,
    pub phase: SplitPhase,
    /// Durable local handoff evidence, independent of final readiness.
    #[serde(default)]
    pub handoff_proof: Option<TailOverlayArtifact>,
    pub readiness_proof: Option<SplitReadinessProof>,
    pub failure: Option<String>,
}

impl SplitTransition {
    /// Checks that a successor retains the exact committed handoff obligation.
    #[must_use]
    pub fn preserves_handoff(&self, successor: &Self) -> bool {
        self.handoff_proof.is_none()
            || (self.handoff_proof == successor.handoff_proof
                && preserves_handoff_phase(self.phase, successor.phase)
                && self.transition_id == successor.transition_id
                && self.parent_id == successor.parent_id
                && self.parent_range == successor.parent_range
                && self.parent_owner == successor.parent_owner
                && self.parent_epoch == successor.parent_epoch
                && self.parent_next_epoch == successor.parent_next_epoch
                && self.parent_artifact == successor.parent_artifact
                && self.split_key == successor.split_key
                && self.retained_parent_artifact.tree_id == successor.retained_parent_artifact.tree_id
                && self.retained_parent_artifact.stream_name
                    == successor.retained_parent_artifact.stream_name
                && self.child.partition_id == successor.child.partition_id
                && self.child.range == successor.child.range
                && self.child.owner == successor.child.owner
                && self.child.owner_epoch == successor.child.owner_epoch
                && self.child.artifact.tree_id == successor.child.artifact.tree_id
                && self.child.artifact.stream_name == successor.child.artifact.stream_name)
    }

    /// Validates exact half-open child coverage and phase-bound readiness.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, bounds, artifacts, or phase
    /// fields that cannot represent one atomic retained-parent-to-child cutover.
    pub fn validate(&self) -> Result<(), ChunkKvProtocolError> {
        let valid_identity = self.transition_id != Id128::default()
            && self.parent_id != Id128::default()
            && self.child.partition_id != Id128::default()
            && self.parent_id != self.child.partition_id
            && self.parent_owner.instance_id != 0
            && !self.parent_owner.rpc_endpoint.is_empty()
            && self.parent_epoch != 0
            && self.parent_next_epoch > self.parent_epoch
            && valid_artifact(&self.parent_artifact)
            && valid_artifact(&self.retained_parent_artifact)
            && valid_split_child(&self.child);
        let exact_ranges = self.split_key > self.parent_range.start
            && self
                .parent_range
                .end
                .as_ref()
                .map_or(true, |end| self.split_key < *end)
            && self.child.range.start == self.split_key
            && self.child.range.end == self.parent_range.end;
        if !valid_identity || !exact_ranges {
            return Err(ChunkKvProtocolError::InvalidSplitTransition(
                "identity, artifact, epoch, or range coverage",
            ));
        }
        self.validate_handoff()?;
        if let Some(proof) = &self.readiness_proof {
            if self
                .handoff_proof
                .as_ref()
                .is_some_and(|handoff| !preserves_handoff_base(handoff, &proof.child_tail_overlay))
            {
                return Err(ChunkKvProtocolError::InvalidSplitTransition(
                    "readiness differs from committed handoff base",
                ));
            }
            let invalid_readiness = if proof.cutover_seq == 0 {
                Some("zero cutover sequence")
            } else if proof.parent_next_epoch != self.parent_next_epoch {
                Some("readiness parent epoch")
            } else if proof.retained_parent_artifact != self.retained_parent_artifact {
                Some("readiness retained-parent artifact")
            } else if proof.retained_parent_tree_manifest == 0 {
                Some("zero retained-parent tree manifest")
            } else if proof.retained_parent_root_manifest_generation == 0 {
                Some("zero retained-parent root manifest generation")
            } else if proof.retained_parent_applied_seq != proof.cutover_seq {
                Some("retained-parent cutover frontier")
            } else if proof.child_applied_seq != proof.cutover_seq {
                Some("child cutover frontier")
            } else if proof.child_tree_manifest == 0 {
                Some("zero child tree manifest")
            } else if proof.child_root_manifest_generation == 0 {
                Some("zero child root manifest generation")
            } else if !self.valid_retained_parent(proof) {
                Some("retained-parent recovery artifact")
            } else if !self.valid_split_overlay(&proof.child_tail_overlay, &self.child, proof.cutover_seq) {
                Some("child tail overlay")
            } else {
                None
            };
            if let Some(reason) = invalid_readiness {
                return Err(ChunkKvProtocolError::InvalidSplitTransition(reason));
            }
        }
        let fields_match_phase = match self.phase {
            SplitPhase::Planned | SplitPhase::ParentPreparing => {
                self.readiness_proof.is_none() && self.failure.is_none()
            }
            SplitPhase::ChildPrepared | SplitPhase::CatalogCommitted => {
                self.readiness_proof.is_some() && self.failure.is_none()
            }
            SplitPhase::Aborted => self.failure.as_ref().is_some_and(|failure| !failure.is_empty()),
        };
        if !fields_match_phase {
            return Err(ChunkKvProtocolError::InvalidSplitTransition(
                "phase-bound readiness or failure fields",
            ));
        }
        Ok(())
    }

    fn valid_retained_parent(&self, proof: &SplitReadinessProof) -> bool {
        if self.retained_parent_artifact.tree_id == self.parent_artifact.tree_id
            && self.retained_parent_artifact.stream_name == self.parent_artifact.stream_name
        {
            return self.retained_parent_artifact.tail_overlay.is_none()
                && proof.retained_parent_tail_overlay.is_none();
        }
        proof
            .retained_parent_tail_overlay
            .as_ref()
            .is_some_and(|overlay| {
                valid_tail_overlay(overlay)
                    && self.retained_parent_artifact.tail_overlay.as_ref() == Some(overlay)
                    && overlay.source_partition_id == self.parent_id
                    && overlay.source_epoch == self.parent_epoch
                    && overlay.source_stream_name == self.parent_artifact.stream_name
                    && overlay.cutover_seq == proof.cutover_seq
            })
    }

    fn validate_handoff(&self) -> Result<(), ChunkKvProtocolError> {
        if self.handoff_proof.as_ref().is_some_and(|proof| {
            !valid_tail_overlay(proof)
                || proof.source_partition_id != self.parent_id
                || proof.source_epoch != self.parent_epoch
                || proof.source_stream_name != self.parent_artifact.stream_name
                || matches!(self.phase, SplitPhase::Planned | SplitPhase::Aborted)
        }) {
            return Err(ChunkKvProtocolError::InvalidSplitTransition("handoff proof"));
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

fn preserves_handoff_base(handoff: &TailOverlayArtifact, ready: &TailOverlayArtifact) -> bool {
    handoff.source_partition_id == ready.source_partition_id
        && handoff.source_epoch == ready.source_epoch
        && handoff.source_stream_name == ready.source_stream_name
        && handoff.source_stream_manifest_generation == ready.source_stream_manifest_generation
        && handoff.replay_offset == ready.replay_offset
        && handoff.base_root_manifest_generation == ready.base_root_manifest_generation
        && handoff.base_tree_manifest == ready.base_tree_manifest
        && handoff.base_applied_seq == ready.base_applied_seq
        && handoff.cutover_seq <= ready.cutover_seq
        && handoff.cutover_offset <= ready.cutover_offset
}

fn preserves_handoff_phase(current: SplitPhase, next: SplitPhase) -> bool {
    match current {
        SplitPhase::ParentPreparing => matches!(
            next,
            SplitPhase::ParentPreparing | SplitPhase::ChildPrepared | SplitPhase::CatalogCommitted
        ),
        SplitPhase::ChildPrepared => matches!(next, SplitPhase::ChildPrepared | SplitPhase::CatalogCommitted),
        SplitPhase::CatalogCommitted => next == SplitPhase::CatalogCommitted,
        SplitPhase::Planned | SplitPhase::Aborted => false,
    }
}
