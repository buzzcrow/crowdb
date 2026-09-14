// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, TargetReadinessProof, TransferPhase, TransferTransition,
};

use crate::MonitorError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferAction {
    FenceSource { instance_id: u64, owner_epoch: u64 },
    WaitForFence { activation_not_before_ms: u64 },
    PrepareTarget { instance_id: u64, owner_epoch: u64 },
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
            TransferPhase::Planned | TransferPhase::AwaitingFence
                if self.transition.release_proof.is_none() =>
            {
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
            TransferPhase::Planned | TransferPhase::AwaitingFence | TransferPhase::TargetPreparing => {
                TransferAction::PrepareTarget {
                    instance_id: self.transition.target.instance_id,
                    owner_epoch: self.transition.target_epoch,
                }
            }
            TransferPhase::TargetPrepared => TransferAction::PublishCatalog,
            TransferPhase::CatalogCommitted => TransferAction::Complete,
            TransferPhase::Aborted => TransferAction::Aborted,
        }
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

    /// Advances the persisted plan to target preparation after authority release.
    ///
    /// # Errors
    ///
    /// Returns an error if old-owner exclusion is not yet proven.
    pub fn begin_target_prepare(&mut self) -> Result<(), MonitorError> {
        if self.transition.release_proof.is_none() {
            return Err(MonitorError::PlanFailed(
                "target preparation requires old-owner fence".into(),
            ));
        }
        if matches!(
            self.transition.phase,
            TransferPhase::Planned | TransferPhase::AwaitingFence | TransferPhase::TargetPreparing
        ) {
            self.transition.phase = TransferPhase::TargetPreparing;
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
        self.transition.readiness_proof = Some(proof);
        self.transition.phase = TransferPhase::TargetPrepared;
        self.validate_current()
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
        if self.transition.phase != TransferPhase::TargetPrepared {
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
        if reason.is_empty() || self.transition.phase == TransferPhase::CatalogCommitted {
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
            TransferPhase::Planned | TransferPhase::AwaitingFence
        ) {
            return Err(MonitorError::PlanFailed(
                "transfer phase cannot accept a fence".into(),
            ));
        }
        self.transition.release_proof = Some(proof);
        self.transition.phase = TransferPhase::AwaitingFence;
        self.validate_current()
    }

    fn validate_current(&self) -> Result<(), MonitorError> {
        self.transition
            .validate()
            .map_err(|error| MonitorError::PlanFailed(error.to_string()))
    }
}
