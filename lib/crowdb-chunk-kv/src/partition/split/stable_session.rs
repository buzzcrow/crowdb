// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Online child preparation with an unchanged parent tree and journal.

use super::{
    prepare_retained_parent, prepare_writer, Checkpoint, ChunkKvError, Partition, PartitionLifecycle,
    PartitionTree, PreparedSplit, PreparedSplitWriter, PreparedSplitWriterArtifact, Result, SplitArtifact,
    SplitPlan, SplitSourceFrontier, SplitWriterTarget,
};
use async_trait::async_trait;
use std::sync::{atomic::Ordering, Arc};

/// Persists the exact child recovery base before local write dispatch changes.
#[async_trait]
pub trait SplitHandoffStore: Send + Sync {
    async fn commit(&self, artifact: &PreparedSplitWriterArtifact) -> Result<()>;
}

pub(crate) struct ChildSplitCutoverRequest {
    parent: Partition,
    plan: SplitPlan,
    target: SplitWriterTarget,
    base_checkpoint: Checkpoint,
    child_tree: Arc<dyn PartitionTree>,
    child_base: (u64, u64, u64),
    completion: tokio::sync::oneshot::Sender<Result<(SplitArtifact, PreparedSplitWriter)>>,
}

impl Partition {
    /// Retries background inheritance for an already installed child writer.
    ///
    /// # Errors
    /// Rejects missing dispatch or conflicting proof, and returns publication errors.
    pub async fn finalize_installed_split_child_session(&self, plan: SplitPlan) -> Result<PreparedSplit> {
        let ingress = self
            .split_ingress()
            .ok_or_else(|| ChunkKvError::SplitRetry("split dispatch is absent".into()))?;
        let child = ingress.child();
        let proof = child
            .prepared_artifact
            .load_full()
            .ok_or_else(|| ChunkKvError::SplitRetry("installed child handoff proof is absent".into()))?;
        let (manifest, base_seq) = self.tree.checkpoint_state()?;
        let checkpoint = Checkpoint {
            tree_id: self.tree_id(),
            tree_manifest: manifest,
            root_manifest_generation: self.tree.root_manifest_generation()?,
            applied_seq: base_seq,
            stream_name: self.journal.stream_name(),
            stream_manifest_generation: self.journal.manifest_generation(),
            replay_offset: proof.parent_replay_offset,
        };
        let frontier = SplitSourceFrontier {
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            checkpoint: checkpoint.clone(),
            cutover_offset: proof.parent_cutover_offset,
            cutover_seq: proof.applied_seq,
            journal: self.journal.clone(),
        };
        let mut retained = prepare_retained_parent(
            &plan,
            (manifest, checkpoint.root_manifest_generation, base_seq),
            &frontier,
        )?;
        retained.applied_seq = proof.applied_seq;
        retained.child_stream_start_seq = proof.child_stream_start_seq;
        let artifact = SplitArtifact {
            transition_id: plan.transition_id,
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            parent_next_epoch: plan.parent_next_epoch,
            shared_view_generation: 0,
            cutover_seq: proof.applied_seq,
            retained_parent: retained,
            child: (*proof).clone(),
        };
        super::super::validate_split_artifact(&plan, &artifact, proof.applied_seq)?;
        self.finish_child_inheritance(&plan, &artifact, child.tree.as_ref())
            .await?;
        self.record_split_artifact(artifact.clone()).await?;
        let writer = PreparedSplitWriter {
            artifact: (*proof).clone(),
            checkpoint: Checkpoint {
                tree_id: proof.tree_id,
                tree_manifest: proof.tree_manifest,
                root_manifest_generation: proof.root_manifest_generation,
                applied_seq: proof.base_applied_seq,
                stream_name: proof.stream_name,
                stream_manifest_generation: child.journal.manifest_generation(),
                replay_offset: 0,
            },
            tree: child.tree.clone(),
            journal: child.journal.clone(),
            parent_journal: self.journal.clone(),
            live: Some(child),
        };
        Ok(PreparedSplit {
            artifact,
            child: writer,
            child_rebuild: crowdb_tree_ffi::RangeRebuildStats::default(),
            delta_records: 0,
        })
    }

    pub(super) async fn install_child_cutover_session(
        &self,
        plan: SplitPlan,
        target: SplitWriterTarget,
        base_checkpoint: Checkpoint,
        child_tree: Arc<dyn PartitionTree>,
        child_base: (u64, u64, u64),
    ) -> Result<(SplitArtifact, PreparedSplitWriter)> {
        let (completion, installed) = tokio::sync::oneshot::channel();
        self.sender
            .send(super::super::WorkerRequest::ChildSplitCutover(Box::new(
                ChildSplitCutoverRequest {
                    parent: self.clone(),
                    plan: plan.clone(),
                    target,
                    base_checkpoint,
                    child_tree: Arc::clone(&child_tree),
                    child_base,
                    completion,
                },
            )))
            .await?;
        let (artifact, child) = installed.await.map_err(|_| ChunkKvError::WriteStalled)??;
        self.pending_child_preparation.store(None);
        self.finish_child_inheritance(&plan, &artifact, child_tree.as_ref())
            .await?;
        self.record_split_artifact(artifact.clone()).await?;
        self.metrics.split_finalization();
        Ok((artifact, child))
    }

    async fn finish_child_inheritance(
        &self,
        plan: &SplitPlan,
        artifact: &SplitArtifact,
        child_tree: &dyn PartitionTree,
    ) -> Result<()> {
        let (generation, captured) = self.tree.begin_split_memtable_view().await?;
        let publication = if captured < artifact.cutover_seq {
            Err(ChunkKvError::ApplyStateUnknown)
        } else {
            self.tree
                .publish_split_memtable_view(generation, artifact.cutover_seq, child_tree, &plan.child.range)
                .await
        };
        let release = self.tree.release_split_memtable_view(generation).await;
        publication?;
        release?;
        child_tree
            .clear_split_memtable_overlay(self.tree.as_ref())
            .await?;
        Ok(())
    }
}

pub(crate) async fn install_child_cutover(
    state: &mut super::super::WorkerState,
    request: ChildSplitCutoverRequest,
) {
    let result = install_child_cutover_inner(state, &request).await;
    let _ = request.completion.send(result);
}

async fn install_child_cutover_inner(
    state: &mut super::super::WorkerState,
    request: &ChildSplitCutoverRequest,
) -> Result<(SplitArtifact, PreparedSplitWriter)> {
    let cutover_seq = state.applied_seq.load(Ordering::Acquire);
    let source = SplitSourceFrontier {
        parent_id: request.plan.parent_id,
        parent_epoch: request.plan.parent_epoch,
        checkpoint: request.base_checkpoint.clone(),
        cutover_offset: state.journal.tail(),
        cutover_seq,
        journal: Arc::clone(&request.parent.journal),
    };
    request
        .child_tree
        .install_split_memtable_overlay(request.parent.tree.as_ref(), cutover_seq)
        .await?;
    let mut child = prepare_writer(
        &request.plan.child,
        request.target.clone(),
        Arc::clone(&request.child_tree),
        request.child_base,
        &source,
    )?;
    let mut retained = prepare_retained_parent(
        &request.plan,
        (
            request.base_checkpoint.tree_manifest,
            request.base_checkpoint.root_manifest_generation,
            request.base_checkpoint.applied_seq,
        ),
        &source,
    )?;
    retained.applied_seq = cutover_seq;
    retained.child_stream_start_seq = cutover_seq
        .checked_add(1)
        .ok_or_else(|| ChunkKvError::InvalidRequest("split cutover sequence overflows".into()))?;
    let artifact = SplitArtifact {
        transition_id: request.plan.transition_id,
        parent_id: request.plan.parent_id,
        parent_epoch: request.plan.parent_epoch,
        parent_next_epoch: request.plan.parent_next_epoch,
        shared_view_generation: 0,
        cutover_seq,
        retained_parent: retained,
        child: child.artifact.clone(),
    };
    let seed = super::super::RecoverySeed {
        applied_seq: cutover_seq,
        applied_position: 0,
        retry_replay_offset: 0,
        results: state.results.clone(),
        result_order: state.result_order.clone(),
        expired_floor: state.expired_floor.clone(),
        recovered: false,
    };
    let live = child
        .start_live((*request.parent.config).clone(), &artifact, seed)
        .await?;
    request
        .parent
        .install_child_split_ingress(&request.plan, live)
        .await?;
    state.split_ingress = Arc::clone(&request.parent.split_ingress);
    state.ownership_epoch = Arc::clone(&request.parent.ownership_epoch);
    state.lifecycle = Arc::clone(&request.parent.lifecycle);
    state.lifecycle.store(
        super::super::lifecycle_code(PartitionLifecycle::SplitFinalizing),
        Ordering::Release,
    );
    Ok((artifact, child))
}
