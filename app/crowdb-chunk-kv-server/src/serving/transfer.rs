// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, TargetReadinessProof, TransferPhase, TransferTransition,
};

use crate::MonitorError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferAction {
    PrepareSource { instance_id: u64, owner_epoch: u64 },
    PrepareTarget { instance_id: u64, owner_epoch: u64 },
    FenceSource { instance_id: u64, owner_epoch: u64 },
    WaitForFence { activation_not_before_ms: u64 },
    PublishCatchingUp,
    CatchUpTarget { instance_id: u64, owner_epoch: u64 },
    PublishCatalog,
    Complete,
    Aborted,
}

/// Idempotent transition reducer; callers persist each resulting record before
/// performing the returned external action.
pub struct TransferStateMachine {
    transition: TransferTransition,
}

impl TransferStateMachine {
    /// Restores one persisted transition after validating every embedded proof.
    ///
    /// # Errors
    ///
    /// Returns a planning error when durable transition state is inconsistent.
    pub fn restore(transition: TransferTransition) -> Result<Self, MonitorError> {
        transition
            .validate()
            .map_err(|error| MonitorError::PlanFailed(error.to_string()))?;
        Ok(Self { transition })
    }

    #[must_use]
    pub fn transition(&self) -> &TransferTransition {
        &self.transition
    }

    #[must_use]
    pub fn next_action(&self, source_reachable: bool, max_clock_skew_ms: u64) -> TransferAction {
        match self.transition.phase {
            TransferPhase::Planned | TransferPhase::SourcePreparing => TransferAction::PrepareSource {
                instance_id: self.transition.source.instance_id,
                owner_epoch: self.transition.source_epoch,
            },
            TransferPhase::TargetPreparing => TransferAction::PrepareTarget {
                instance_id: self.transition.target.instance_id,
                owner_epoch: self.transition.target_epoch,
            },
            TransferPhase::TargetPrepared | TransferPhase::AwaitingFence => {
                if source_reachable {
                    TransferAction::FenceSource {
                        instance_id: self.transition.source.instance_id,
                        owner_epoch: self.transition.source_epoch,
                    }
                } else {
                    TransferAction::WaitForFence {
                        activation_not_before_ms: self
                            .transition
                            .old_grant_expires_at_ms
                            .saturating_add(max_clock_skew_ms),
                    }
                }
            }
            TransferPhase::TargetCatchingUp => TransferAction::PublishCatchingUp,
            TransferPhase::CatchupPublished => TransferAction::CatchUpTarget {
                instance_id: self.transition.target.instance_id,
                owner_epoch: self.transition.target_epoch,
            },
            TransferPhase::TargetReady => TransferAction::PublishCatalog,
            TransferPhase::CatalogCommitted => TransferAction::Complete,
            TransferPhase::Aborted => TransferAction::Aborted,
        }
    }

    /// Enters source snapshot preparation without changing writer authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the persisted phase cannot prepare the source.
    pub fn begin_source_prepare(&mut self) -> Result<(), MonitorError> {
        if matches!(
            self.transition.phase,
            TransferPhase::Planned | TransferPhase::SourcePreparing
        ) {
            self.transition.phase = TransferPhase::SourcePreparing;
            return self.validate_current();
        }
        Err(MonitorError::PlanFailed(
            "transfer phase cannot prepare source snapshot".into(),
        ))
    }

    /// Persists the exact source base and initial tail cursor for target replay.
    ///
    /// # Errors
    ///
    /// Returns an error for a mismatched target identity or missing overlay.
    pub fn record_source_base(
        &mut self,
        target_artifact: crowdb_protocol::chunk_kv::PartitionArtifact,
    ) -> Result<(), MonitorError> {
        if self.transition.phase != TransferPhase::SourcePreparing
            || target_artifact.tree_id != self.transition.artifact.tree_id
            || target_artifact.stream_name != self.transition.target_artifact.stream_name
            || target_artifact.tail_overlay.is_none()
        {
            return Err(MonitorError::PlanFailed(
                "source base does not match the transfer target".into(),
            ));
        }
        self.transition.target_artifact = target_artifact;
        self.transition.phase = TransferPhase::TargetPreparing;
        self.validate_current()
    }

    /// Records a source fence that proves the old owner stopped admission.
    ///
    /// # Errors
    ///
    /// Returns an error for mismatched identity/frontier or a conflicting proof.
    pub fn record_source_fence(&mut self, proof: AuthorityReleaseProof) -> Result<(), MonitorError> {
        if !matches!(proof, AuthorityReleaseProof::ExplicitFence { .. }) {
            return Err(MonitorError::PlanFailed("expected explicit source fence".into()));
        }
        self.install_release_proof(proof)
    }

    /// Records lease exclusion only after expiry plus the clock-skew budget.
    ///
    /// # Errors
    ///
    /// Returns an error when the fake/real wall clock has not reached the safe
    /// activation boundary or a different proof was already persisted.
    pub fn record_lease_expiry(
        &mut self,
        now_wall_ms: u64,
        max_clock_skew_ms: u64,
    ) -> Result<(), MonitorError> {
        let boundary = self
            .transition
            .old_grant_expires_at_ms
            .checked_add(max_clock_skew_ms)
            .ok_or_else(|| MonitorError::PlanFailed("lease exclusion boundary overflowed".into()))?;
        if now_wall_ms < boundary {
            return Err(MonitorError::PlanFailed(
                "old owner grant has not been excluded".into(),
            ));
        }
        self.install_release_proof(AuthorityReleaseProof::LeaseExpired {
            activation_not_before_ms: boundary,
        })
    }

    /// Advances the persisted plan to target preparation while the source serves.
    ///
    /// # Errors
    ///
    /// Returns an error unless the source base has already been persisted.
    pub fn begin_target_prepare(&mut self) -> Result<(), MonitorError> {
        if self.transition.phase == TransferPhase::TargetPreparing {
            return Ok(());
        }
        Err(MonitorError::PlanFailed(
            "transfer phase cannot prepare target".into(),
        ))
    }

    /// Records exact target recovery proof without changing storage identities.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong target, epoch, artifact, or durable tail.
    pub fn record_target_ready(&mut self, proof: TargetReadinessProof) -> Result<(), MonitorError> {
        if let Some(existing) = &self.transition.readiness_proof {
            return if existing == &proof {
                Ok(())
            } else {
                Err(MonitorError::PlanFailed(
                    "target readiness proof conflicts".into(),
                ))
            };
        }
        if self.transition.phase != TransferPhase::TargetPreparing {
            return Err(MonitorError::PlanFailed("target was not preparing".into()));
        }
        let mut candidate = self.transition.clone();
        candidate.readiness_proof = Some(proof);
        if matches!(
            candidate.release_proof,
            Some(AuthorityReleaseProof::LeaseExpired { .. })
        ) {
            candidate.catchup_proof = candidate.readiness_proof.clone();
            candidate.phase = TransferPhase::TargetReady;
        } else {
            candidate.phase = TransferPhase::TargetPrepared;
        }
        validate_transition(&candidate)?;
        self.transition = candidate;
        Ok(())
    }

    /// Records that the catching-up catalog entry is authoritative.
    ///
    /// # Errors
    ///
    /// Returns an error unless the released target is awaiting publication.
    pub fn mark_catchup_published(&mut self) -> Result<(), MonitorError> {
        if self.transition.phase == TransferPhase::CatchupPublished {
            return Ok(());
        }
        if self.transition.phase != TransferPhase::TargetCatchingUp {
            return Err(MonitorError::PlanFailed(
                "catch-up publication requires a released source".into(),
            ));
        }
        self.transition.phase = TransferPhase::CatchupPublished;
        self.validate_current()
    }

    /// Records final target replay through the released source cursor.
    ///
    /// # Errors
    ///
    /// Returns an error for a conflicting proof or incorrect persisted phase.
    pub fn record_target_caught_up(&mut self, proof: TargetReadinessProof) -> Result<(), MonitorError> {
        if let Some(existing) = &self.transition.catchup_proof {
            return if existing == &proof {
                Ok(())
            } else {
                Err(MonitorError::PlanFailed("target catch-up proof conflicts".into()))
            };
        }
        if self.transition.phase != TransferPhase::CatchupPublished {
            return Err(MonitorError::PlanFailed(
                "target catch-up requires a published catching-up assignment".into(),
            ));
        }
        let mut candidate = self.transition.clone();
        candidate.catchup_proof = Some(proof);
        candidate.phase = TransferPhase::TargetReady;
        validate_transition(&candidate)?;
        self.transition = candidate;
        Ok(())
    }

    /// Marks the catalog cutover only after a prepared target proof.
    ///
    /// # Errors
    ///
    /// Returns an error if readiness has not been proven.
    pub fn commit_catalog(&mut self) -> Result<(), MonitorError> {
        if self.transition.phase == TransferPhase::CatalogCommitted {
            return Ok(());
        }
        if self.transition.phase != TransferPhase::TargetReady {
            return Err(MonitorError::PlanFailed(
                "catalog cutover requires prepared target".into(),
            ));
        }
        self.transition.phase = TransferPhase::CatalogCommitted;
        self.validate_current()
    }

    /// Aborts a pre-commit transition while retaining its referenced artifacts.
    ///
    /// # Errors
    ///
    /// Returns an error after catalog commit or for an empty/conflicting reason.
    pub fn abort(&mut self, reason: &str) -> Result<(), MonitorError> {
        if reason.is_empty()
            || self.transition.phase == TransferPhase::CatalogCommitted
            || self.transition.release_proof.is_some()
        {
            return Err(MonitorError::PlanFailed("committed transfer cannot abort".into()));
        }
        if self.transition.phase == TransferPhase::Aborted {
            return if self.transition.failure.as_deref() == Some(reason) {
                Ok(())
            } else {
                Err(MonitorError::PlanFailed("abort reason conflicts".into()))
            };
        }
        self.transition.phase = TransferPhase::Aborted;
        self.transition.failure = Some(reason.into());
        self.validate_current()
    }

    fn install_release_proof(&mut self, proof: AuthorityReleaseProof) -> Result<(), MonitorError> {
        if let Some(existing) = &self.transition.release_proof {
            return if existing == &proof {
                Ok(())
            } else {
                Err(MonitorError::PlanFailed(
                    "authority release proof conflicts".into(),
                ))
            };
        }
        if !matches!(
            self.transition.phase,
            TransferPhase::TargetPrepared | TransferPhase::AwaitingFence
        ) {
            return Err(MonitorError::PlanFailed(
                "transfer phase cannot accept a fence".into(),
            ));
        }
        let mut candidate = self.transition.clone();
        if let AuthorityReleaseProof::ExplicitFence {
            durable_tail,
            durable_tail_offset,
            ..
        } = &proof
        {
            let overlay = candidate
                .target_artifact
                .tail_overlay
                .as_mut()
                .ok_or_else(|| MonitorError::PlanFailed("transfer target overlay is absent".into()))?;
            if *durable_tail < overlay.cutover_seq || *durable_tail_offset < overlay.cutover_offset {
                return Err(MonitorError::PlanFailed(
                    "source release precedes target preparation".into(),
                ));
            }
            overlay.cutover_seq = *durable_tail;
            overlay.cutover_offset = *durable_tail_offset;
            overlay.target_stream_start_seq = durable_tail.saturating_add(1);
        }
        candidate.release_proof = Some(proof);
        candidate.phase = TransferPhase::TargetCatchingUp;
        validate_transition(&candidate)?;
        self.transition = candidate;
        Ok(())
    }

    fn validate_current(&self) -> Result<(), MonitorError> {
        self.transition
            .validate()
            .map_err(|error| MonitorError::PlanFailed(error.to_string()))
    }
}

fn validate_transition(transition: &TransferTransition) -> Result<(), MonitorError> {
    transition
        .validate()
        .map_err(|error| MonitorError::PlanFailed(error.to_string()))
}
