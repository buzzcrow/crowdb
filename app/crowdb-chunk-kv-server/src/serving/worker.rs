// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv::{Partition, SplitArtifact};
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, CatalogEntry, CatalogPartitionState, SplitPhase, SplitReadinessProof,
    SplitTransition, TargetReadinessProof, TransferPhase, TransferTransition,
};

use crate::{ChunkKvService, ChunkKvStorage, MonitorError};

/// Executes process-local storage work requested by persisted transitions.
pub struct TransitionExecutor {
    instance_id: u64,
    service: Arc<ChunkKvService>,
    storage: Arc<dyn TransitionStorage>,
    max_split_fence_lag_records: u64,
}

impl TransitionExecutor {
    /// Creates a worker bound to one server identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the split fence budget is zero.
    pub fn new(
        instance_id: u64,
        service: Arc<ChunkKvService>,
        storage: Arc<ChunkKvStorage>,
        max_split_fence_lag_records: u64,
    ) -> Result<Self, MonitorError> {
        Self::with_storage(instance_id, service, storage, max_split_fence_lag_records)
    }

    /// Creates a worker with an injected storage backend.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker identity or split budget is zero.
    pub fn with_storage(
        instance_id: u64,
        service: Arc<ChunkKvService>,
        storage: Arc<dyn TransitionStorage>,
        max_split_fence_lag_records: u64,
    ) -> Result<Self, MonitorError> {
        if instance_id == 0 || max_split_fence_lag_records == 0 {
            return Err(MonitorError::PlanFailed(
                "transition worker identity and split fence budget must be nonzero".into(),
            ));
        }
        Ok(Self {
            instance_id,
            service,
            storage,
            max_split_fence_lag_records,
        })
    }

    /// Fences and drains a locally owned transfer source.
    ///
    /// # Errors
    ///
    /// Returns an error for stale transition identity or a missing local source.
    pub async fn fence_transfer_source(
        &self,
        transition: &TransferTransition,
    ) -> Result<AuthorityReleaseProof, MonitorError> {
        transition
            .validate()
            .map_err(|error| plan_error(&error.to_string()))?;
        if transition.source.instance_id != self.instance_id
            || !matches!(
                transition.phase,
                TransferPhase::Planned | TransferPhase::AwaitingFence
            )
        {
            return Err(plan_error("transfer does not request a local source fence"));
        }
        let partition = self
            .service
            .hosted_partition(transition.partition_id)
            .ok_or_else(|| plan_error("transfer source partition is not hosted"))?;
        partition
            .fence_mutations(transition.source_epoch)
            .await
            .map_err(|error| plan_error(&error.to_string()))?;
        let snapshot = partition.snapshot();
        Ok(AuthorityReleaseProof::ExplicitFence {
            source_instance_id: self.instance_id,
            source_epoch: transition.source_epoch,
            durable_tail: snapshot.journal_durable_seq,
        })
    }

    /// Reopens and replays the exact transfer target without activating it.
    ///
    /// # Errors
    ///
    /// Returns an error unless target preparation was durably entered first.
    pub async fn prepare_transfer_target(
        &self,
        transition: &TransferTransition,
    ) -> Result<TargetReadinessProof, MonitorError> {
        transition
            .validate()
            .map_err(|error| plan_error(&error.to_string()))?;
        if transition.target.instance_id != self.instance_id
            || transition.phase != TransferPhase::TargetPreparing
        {
            return Err(plan_error("transfer does not request local target preparation"));
        }
        let entry = CatalogEntry {
            partition_id: transition.partition_id,
            range: transition.range.clone(),
            owner: transition.target.clone(),
            owner_epoch: transition.target_epoch,
            state: CatalogPartitionState::Prepared,
            artifact: transition.artifact.clone(),
            transition_id: Some(transition.transition_id),
        };
        let partition = self
            .storage
            .recover_partition(&entry)
            .await
            .map_err(|error| plan_error(&error.to_string()))?;
        let snapshot = partition.snapshot();
        self.service
            .install_partition(&partition)
            .map_err(|error| plan_error(&error.to_string()))?;
        Ok(TargetReadinessProof {
            target_instance_id: self.instance_id,
            target_epoch: transition.target_epoch,
            artifact: transition.artifact.clone(),
            durable_tail: snapshot.journal_durable_seq,
        })
    }

    /// Rebuilds both children from the local parent and returns one common cutover.
    ///
    /// A repeated call reuses an in-memory prepared artifact. After process
    /// restart it rebuilds the same durable child identities from the current
    /// authoritative parent before producing a new proof.
    ///
    /// # Errors
    ///
    /// Returns an error unless parent preparation was durably entered first.
    pub async fn prepare_split_parent(
        &self,
        transition: &SplitTransition,
    ) -> Result<SplitReadinessProof, MonitorError> {
        transition
            .validate()
            .map_err(|error| plan_error(&error.to_string()))?;
        if transition.parent_owner.instance_id != self.instance_id
            || transition.phase != SplitPhase::ParentPreparing
        {
            return Err(plan_error("split does not request local parent preparation"));
        }
        let parent = self
            .service
            .hosted_partition(transition.parent_id)
            .ok_or_else(|| plan_error("split parent partition is not hosted"))?;
        let transition_id = crowdb_chunk_kv::TransitionId {
            high: transition.transition_id.high,
            low: transition.transition_id.low,
        };
        let artifact = if let Some(artifact) = parent.prepared_split_artifact(transition_id).await {
            artifact
        } else {
            self.storage
                .prepare_split(&parent, transition, self.max_split_fence_lag_records)
                .await?
        };
        if artifact.left.applied_seq != artifact.cutover_seq
            || artifact.right.applied_seq != artifact.cutover_seq
        {
            return Err(plan_error("split children do not share the cutover frontier"));
        }
        Ok(SplitReadinessProof {
            cutover_seq: artifact.cutover_seq,
            left_applied_seq: artifact.left.applied_seq,
            right_applied_seq: artifact.right.applied_seq,
        })
    }
}

#[async_trait]
pub trait TransitionStorage: Send + Sync {
    async fn recover_partition(&self, entry: &CatalogEntry) -> Result<Partition, MonitorError>;
    async fn prepare_split(
        &self,
        parent: &Partition,
        transition: &SplitTransition,
        max_fence_lag_records: u64,
    ) -> Result<SplitArtifact, MonitorError>;
}

#[async_trait]
impl TransitionStorage for ChunkKvStorage {
    async fn recover_partition(&self, entry: &CatalogEntry) -> Result<Partition, MonitorError> {
        ChunkKvStorage::recover_partition(self, entry)
            .await
            .map_err(|error| plan_error(&error.to_string()))
    }

    async fn prepare_split(
        &self,
        parent: &Partition,
        transition: &SplitTransition,
        max_fence_lag_records: u64,
    ) -> Result<SplitArtifact, MonitorError> {
        ChunkKvStorage::prepare_split(self, parent, transition, max_fence_lag_records).await
    }
}

fn plan_error(error: &str) -> MonitorError {
    MonitorError::PlanFailed(error.into())
}
