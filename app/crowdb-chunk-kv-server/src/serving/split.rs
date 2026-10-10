// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{SplitPhase, SplitReadinessProof, SplitTransition, TailOverlayArtifact};

use crate::MonitorError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SplitAction {
    PrepareParent { instance_id: u64, owner_epoch: u64 },
    PublishCatalog,
    Complete,
    Aborted,
}

/// Idempotent reducer for one persisted retained-parent-to-child split.
pub struct SplitStateMachine {
    transition: SplitTransition,
}

impl SplitStateMachine {
    /// Restores one persisted split after validating exact range coverage.
    ///
    /// # Errors
    ///
    /// Returns a planning error when durable split state is inconsistent.
    pub fn restore(transition: SplitTransition) -> Result<Self, MonitorError> {
        transition
            .validate()
            .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
        Ok(Self { transition })
    }

    #[must_use]
    pub fn transition(&self) -> &SplitTransition {
        &self.transition
    }

    #[must_use]
    pub fn next_action(&self) -> SplitAction {
        match self.transition.phase {
            SplitPhase::Planned | SplitPhase::ParentPreparing => SplitAction::PrepareParent {
                instance_id: self.transition.parent_owner.instance_id,
                owner_epoch: self.transition.parent_epoch,
            },
            SplitPhase::ChildPrepared => SplitAction::PublishCatalog,
            SplitPhase::CatalogCommitted => SplitAction::Complete,
            SplitPhase::Aborted => SplitAction::Aborted,
        }
    }

    /// Marks the parent worker as responsible for idempotent R142 preparation.
    ///
    /// # Errors
    ///
    /// Returns an error after readiness, commit, or abort.
    pub fn begin_parent_prepare(&mut self) -> Result<(), MonitorError> {
        match self.transition.phase {
            SplitPhase::Planned | SplitPhase::ParentPreparing => {
                self.transition.phase = SplitPhase::ParentPreparing;
                Ok(())
            }
            _ => Err(MonitorError::PlanFailed(
                "split phase cannot prepare parent".into(),
            )),
        }
    }

    /// Records the proof to persist before installing local child dispatch.
    ///
    /// # Errors
    ///
    /// Returns an error for a conflicting proof or outside parent preparation.
    pub fn record_handoff(&mut self, proof: TailOverlayArtifact) -> Result<(), MonitorError> {
        if let Some(existing) = &self.transition.handoff_proof {
            return if existing == &proof {
                Ok(())
            } else {
                Err(MonitorError::PlanFailed("split handoff proof conflicts".into()))
            };
        }
        if self.transition.phase != SplitPhase::ParentPreparing {
            return Err(MonitorError::PlanFailed("split parent was not preparing".into()));
        }
        let mut next = self.transition.clone();
        next.handoff_proof = Some(proof);
        next.validate()
            .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
        self.transition = next;
        Ok(())
    }

    /// Records the exact common cutover frontier reported by the child.
    ///
    /// # Errors
    ///
    /// Returns an error outside parent preparation or for conflicting proof.
    pub fn record_child_ready(&mut self, proof: SplitReadinessProof) -> Result<(), MonitorError> {
        if let Some(existing) = &self.transition.readiness_proof {
            return if existing == &proof {
                Ok(())
            } else {
                Err(MonitorError::PlanFailed("split readiness proof conflicts".into()))
            };
        }
        if self.transition.phase != SplitPhase::ParentPreparing {
            return Err(MonitorError::PlanFailed("split parent was not preparing".into()));
        }
        let mut next = self.transition.clone();
        if next.retained_parent_artifact.tree_id != next.parent_artifact.tree_id
            || next.retained_parent_artifact.stream_name != next.parent_artifact.stream_name
        {
            next.retained_parent_artifact
                .tail_overlay
                .clone_from(&proof.retained_parent_tail_overlay);
        }
        next.child.artifact.tail_overlay = Some(proof.child_tail_overlay.clone());
        next.readiness_proof = Some(proof);
        next.phase = SplitPhase::ChildPrepared;
        next.validate()
            .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
        self.transition = next;
        Ok(())
    }

    /// Marks the atomic catalog update after the retained parent and child are ready.
    ///
    /// # Errors
    ///
    /// Returns an error before child readiness.
    pub fn commit_catalog(&mut self) -> Result<(), MonitorError> {
        if self.transition.phase == SplitPhase::CatalogCommitted {
            return Ok(());
        }
        if self.transition.phase != SplitPhase::ChildPrepared {
            return Err(MonitorError::PlanFailed(
                "split catalog cutover requires a prepared child".into(),
            ));
        }
        self.transition.phase = SplitPhase::CatalogCommitted;
        self.validate_current()
    }

    /// Aborts before catalog commit while retaining every referenced artifact.
    ///
    /// # Errors
    ///
    /// Returns an error after commit or for an empty/conflicting reason.
    pub fn abort(&mut self, reason: &str) -> Result<(), MonitorError> {
        if reason.is_empty()
            || self.transition.phase == SplitPhase::CatalogCommitted
            || self.transition.handoff_proof.is_some()
        {
            return Err(MonitorError::PlanFailed(
                "committed split handoff cannot abort".into(),
            ));
        }
        if self.transition.phase == SplitPhase::Aborted {
            return if self.transition.failure.as_deref() == Some(reason) {
                Ok(())
            } else {
                Err(MonitorError::PlanFailed("split abort reason conflicts".into()))
            };
        }
        self.transition.phase = SplitPhase::Aborted;
        self.transition.failure = Some(reason.into());
        self.validate_current()
    }

    fn validate_current(&self) -> Result<(), MonitorError> {
        self.transition
            .validate()
            .map_err(|error| MonitorError::PlanFailed(error.to_string()))
    }
}
