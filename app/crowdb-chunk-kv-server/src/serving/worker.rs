// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use crowdb_chunk_kv::{Partition, PartitionConfig, SplitArtifact};
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, PartitionArtifact,
    SplitPhase, SplitReadinessProof, SplitTransition, TailOverlayArtifact, TargetReadinessProof,
    TransferPhase, TransferTransition,
};

use crate::{ChunkKvService, ChunkKvStorage, MonitorError};

/// A prepared split artifact together with the process-local child handles.
pub struct PreparedLocalSplit {
    pub artifact: SplitArtifact,
    pub retained_parent: Partition,
    pub child: Partition,
}

/// Executes process-local storage work requested by persisted transitions.
pub struct TransitionExecutor {
    instance_id: u64,
    service: Arc<ChunkKvService>,
    storage: Arc<dyn TransitionStorage>,
    max_split_catchup_lag_records: u64,
}

impl TransitionExecutor {
    /// Creates a worker bound to one server identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the split catch-up budget is zero.
    pub fn new(
        instance_id: u64,
        service: Arc<ChunkKvService>,
        storage: Arc<ChunkKvStorage>,
        max_split_catchup_lag_records: u64,
    ) -> Result<Self, MonitorError> {
        Self::with_storage(instance_id, service, storage, max_split_catchup_lag_records)
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
        max_split_catchup_lag_records: u64,
    ) -> Result<Self, MonitorError> {
        if instance_id == 0 || max_split_catchup_lag_records == 0 {
            return Err(MonitorError::PlanFailed(
                "transition worker identity and split catch-up budget must be nonzero".into(),
            ));
        }
        Ok(Self {
            instance_id,
            service,
            storage,
            max_split_catchup_lag_records,
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
                TransferPhase::TargetPrepared | TransferPhase::AwaitingFence
            )
        {
            return Err(plan_error("transfer does not request a local source fence"));
        }
        let partition = self
            .service
            .hosted_partition(transition.partition_id)
            .ok_or_else(|| plan_error("transfer source partition is not hosted"))?;
        let before = partition.snapshot();
        let overlay = transition
            .target_artifact
            .tail_overlay
            .as_ref()
            .ok_or_else(|| plan_error("transfer target overlay is absent"))?;
        let tail_records = before.journal_durable_seq.saturating_sub(overlay.cutover_seq);
        let tail_bytes = before
            .journal_durable_offset
            .saturating_sub(overlay.cutover_offset);
        if tail_records > transition.readiness_limits.max_tail_records
            || tail_bytes > transition.readiness_limits.max_tail_bytes
            || tail_records > transition.readiness_limits.max_estimated_catchup_ms
        {
            return Err(plan_error(
                "transfer target is outside the source-fence readiness budget",
            ));
        }
        partition
            .suspend_for_transfer(transition.source_epoch)
            .await
            .map_err(|error| plan_error(&error.to_string()))?;
        let snapshot = partition.snapshot();
        Ok(AuthorityReleaseProof::ExplicitFence {
            source_instance_id: self.instance_id,
            source_epoch: transition.source_epoch,
            durable_tail: snapshot.journal_durable_seq,
            durable_tail_offset: snapshot.journal_durable_offset,
        })
    }

    /// Checkpoints the live source and pins its initial target replay frontier.
    ///
    /// # Errors
    ///
    /// Returns an error for stale transition identity, missing source, or storage failure.
    pub async fn prepare_transfer_source(
        &self,
        transition: &TransferTransition,
    ) -> Result<PartitionArtifact, MonitorError> {
        transition
            .validate()
            .map_err(|error| plan_error(&error.to_string()))?;
        if transition.source.instance_id != self.instance_id
            || transition.phase != TransferPhase::SourcePreparing
        {
            return Err(plan_error("transfer does not request local source preparation"));
        }
        let partition = self
            .service
            .hosted_partition(transition.partition_id)
            .ok_or_else(|| plan_error("transfer source partition is not hosted"))?;
        self.storage.prepare_transfer_source(&partition, transition).await
    }

    /// Idempotently releases a transfer's exact-root pin after durable
    /// transition evidence no longer requires its overlay.
    ///
    /// # Errors
    ///
    /// Returns an error when the hosted partition cannot delete the durable
    /// pin.
    pub fn release_transfer_generation_pin(
        &self,
        transition: &TransferTransition,
    ) -> Result<(), MonitorError> {
        let Some(partition) = self.service.hosted_partition(transition.partition_id) else {
            return Ok(());
        };
        partition
            .release_generation_pin(crowdb_chunk_kv::TransitionId {
                high: transition.transition_id.high,
                low: transition.transition_id.low,
            })
            .map_err(|error| plan_error(&error.to_string()))
    }

    /// Idempotently releases a split child's exact-root pin after completion
    /// or an authoritative unpublished abort.
    ///
    /// # Errors
    ///
    /// Returns an error when the hosted child cannot delete the durable pin.
    pub fn release_split_generation_pin(&self, transition: &SplitTransition) -> Result<(), MonitorError> {
        let Some(partition) = self.service.hosted_partition(transition.child.partition_id) else {
            return Ok(());
        };
        partition
            .release_generation_pin(crowdb_chunk_kv::TransitionId {
                high: transition.transition_id.high,
                low: transition.transition_id.low,
            })
            .map_err(|error| plan_error(&error.to_string()))
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
            || !matches!(
                transition.phase,
                TransferPhase::TargetPreparing | TransferPhase::CatchupPublished
            )
        {
            return Err(plan_error("transfer does not request local target preparation"));
        }
        let entry = ChunkKvRangeCatalogEntry {
            partition_id: transition.partition_id,
            range: transition.range.clone(),
            owner: transition.target.clone(),
            owner_epoch: transition.target_epoch,
            state: ChunkKvRangeCatalogPartitionState::Prepared,
            artifact: transition.target_artifact.clone(),
            transition_id: Some(transition.transition_id),
        };
        if transition.phase == TransferPhase::CatchupPublished {
            self.service.remove_partition(transition.partition_id);
        }
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
            artifact: transition.target_artifact.clone(),
            durable_tail: snapshot.journal_durable_seq,
        })
    }

    /// Rebuilds one child from the retained local parent and returns their common cutover.
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
            let child_id = crowdb_protocol::chunk_kv::Id128 {
                high: artifact.child.partition_id.high,
                low: artifact.child.partition_id.low,
            };
            if self.service.hosted_partition(child_id).is_none() {
                return Err(plan_error(
                    "prepared split child is not installed; preparation must be rebuilt",
                ));
            }
            artifact
        } else {
            let prepared = self
                .storage
                .prepare_split(&parent, transition, self.max_split_catchup_lag_records)
                .await?;
            self.service
                .install_partition(&prepared.child)
                .map_err(|error| plan_error(&error.to_string()))?;
            prepared.artifact
        };
        if artifact.child.applied_seq != artifact.cutover_seq {
            return Err(plan_error("split child does not share the cutover frontier"));
        }
        Ok(SplitReadinessProof {
            cutover_seq: artifact.cutover_seq,
            parent_next_epoch: artifact.parent_next_epoch,
            retained_parent_artifact: transition.retained_parent_artifact.clone(),
            retained_parent_tree_manifest: artifact.retained_parent.tree_manifest,
            retained_parent_root_manifest_generation: artifact.retained_parent.root_manifest_generation,
            retained_parent_applied_seq: artifact.retained_parent.applied_seq,
            child_applied_seq: artifact.child.applied_seq,
            child_tree_manifest: artifact.child.tree_manifest,
            child_root_manifest_generation: artifact.child.root_manifest_generation,
            child_tail_overlay: split_tail_overlay(&artifact, &artifact.child),
        })
    }
}

#[async_trait]
pub trait TransitionStorage: Send + Sync {
    async fn recover_partition(&self, entry: &ChunkKvRangeCatalogEntry) -> Result<Partition, MonitorError>;
    async fn prepare_transfer_source(
        &self,
        source: &Partition,
        transition: &TransferTransition,
    ) -> Result<PartitionArtifact, MonitorError>;
    async fn prepare_split(
        &self,
        parent: &Partition,
        transition: &SplitTransition,
        max_catchup_lag_records: u64,
    ) -> Result<PreparedLocalSplit, MonitorError>;
}

#[async_trait]
impl TransitionStorage for ChunkKvStorage {
    async fn recover_partition(&self, entry: &ChunkKvRangeCatalogEntry) -> Result<Partition, MonitorError> {
        ChunkKvStorage::recover_partition(self, entry)
            .await
            .map_err(|error| plan_error(&error.to_string()))
    }

    async fn prepare_transfer_source(
        &self,
        source: &Partition,
        transition: &TransferTransition,
    ) -> Result<PartitionArtifact, MonitorError> {
        ChunkKvStorage::prepare_transfer_source(self, source, transition).await
    }

    async fn prepare_split(
        &self,
        parent: &Partition,
        transition: &SplitTransition,
        max_catchup_lag_records: u64,
    ) -> Result<PreparedLocalSplit, MonitorError> {
        let prepared =
            ChunkKvStorage::prepare_split(self, parent, transition, max_catchup_lag_records).await?;
        let artifact = prepared.artifact.clone();
        let retained_parent = prepared
            .retained_parent
            .ok_or_else(|| plan_error("split session did not create a retained parent writer"))?
            .open(PartitionConfig::default())
            .await
            .map_err(|error| plan_error(&error.to_string()))?;
        let child = prepared
            .child
            .open(PartitionConfig::default())
            .await
            .map_err(|error| plan_error(&error.to_string()))?;
        retained_parent
            .activate_local_split_writer(&artifact)
            .map_err(|error| plan_error(&error.to_string()))?;
        child
            .activate_local_split_writer(&artifact)
            .map_err(|error| plan_error(&error.to_string()))?;
        parent
            .install_split_ingress(retained_parent.clone(), child.clone())
            .await
            .map_err(|error| plan_error(&error.to_string()))?;
        Ok(PreparedLocalSplit {
            artifact,
            retained_parent,
            child,
        })
    }
}

fn plan_error(error: &str) -> MonitorError {
    MonitorError::PlanFailed(error.into())
}

fn split_tail_overlay(
    split: &SplitArtifact,
    child: &crowdb_chunk_kv::PreparedSplitWriterArtifact,
) -> TailOverlayArtifact {
    TailOverlayArtifact {
        source_partition_id: crowdb_protocol::chunk_kv::Id128 {
            high: split.parent_id.high,
            low: split.parent_id.low,
        },
        source_epoch: split.parent_epoch,
        source_stream_name: child.parent_stream_name,
        source_stream_manifest_generation: child.parent_stream_manifest_generation,
        replay_offset: child.parent_replay_offset,
        cutover_offset: child.parent_cutover_offset,
        base_root_manifest_generation: child.root_manifest_generation,
        base_tree_manifest: child.tree_manifest,
        base_applied_seq: child.base_applied_seq,
        cutover_seq: child.applied_seq,
        target_stream_start_seq: child.child_stream_start_seq,
    }
}
