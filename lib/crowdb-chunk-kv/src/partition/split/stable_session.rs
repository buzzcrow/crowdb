// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Online child preparation with an unchanged parent tree and journal.

use super::{
    prepare_retained_parent, prepare_writer_at, Checkpoint, ChunkKvError, Partition, PartitionLifecycle,
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
    replayed_prefix: u64,
    overlay: Arc<dyn crate::PreparedSplitOverlay>,
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
        let artifact = if let Some(retained) = self.retained_child_artifact(&plan).await? {
            retained
        } else {
            let (manifest, base_seq) = self.tree.checkpoint_state()?;
            let checkpoint = Checkpoint {
                tree_id: self.tree_id(),
                tree_manifest: manifest,
                root_manifest_generation: self.tree.root_manifest_generation()?,
                applied_seq: base_seq,
                stream_name: self.journal.stream_name(),
                stream_generation: self.journal.record_generation(),
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
            SplitArtifact {
                transition_id: plan.transition_id,
                parent_id: plan.parent_id,
                parent_epoch: plan.parent_epoch,
                parent_next_epoch: plan.parent_next_epoch,
                shared_view_generation: 0,
                cutover_seq: proof.applied_seq,
                retained_parent: retained,
                child: (*proof).clone(),
            }
        };
        super::super::validate_split_artifact(&plan, &artifact, proof.applied_seq)?;
        let artifact = self
            .finish_child_inheritance(&plan, &artifact, child.tree.as_ref())
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
                stream_generation: child.journal.record_generation(),
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
        replay_offset: u64,
    ) -> Result<(SplitArtifact, PreparedSplitWriter, u64)> {
        let permit = self.sender.reserve().await?;
        let mut cursor = super::DeltaCursor {
            offset: replay_offset,
            applied_seq: child_tree.last_applied_seq(),
            delta_records: 0,
        };
        let (overlay, start_frontier) = self
            .tree
            .arm_split_overlay(
                child_tree.as_ref(),
                crate::SplitOverlayLimits {
                    max_records: self.config.queue_requests,
                    max_bytes: self.config.queue_bytes,
                },
            )
            .await?;
        let target_prefix = start_frontier.max(self.applied_seq.load(Ordering::Acquire));
        super::replay_writer_until(
            self,
            &mut cursor,
            target_prefix,
            child_tree.as_ref(),
            &plan.parent_range,
            &plan.child,
        )
        .await?;
        overlay.prepare(cursor.applied_seq).await?;
        let (completion, installed) = tokio::sync::oneshot::channel();
        // Cancellation drops the request's token before its completion sender,
        // including while waiting for the host cleanup task to start.
        let request = Box::new(ChildSplitCutoverRequest {
            parent: self.clone(),
            plan: plan.clone(),
            target,
            base_checkpoint,
            child_tree: Arc::clone(&child_tree),
            child_base,
            replayed_prefix: cursor.applied_seq,
            overlay: overlay.clone(),
            completion,
        });
        let (started, running) = tokio::sync::oneshot::channel();
        // A started blocking task survives caller cancellation and runtime
        // shutdown. All native preparation is finished before it occupies a
        // host thread, so even a one-thread blocking pool can make progress.
        let installed = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            complete_prepared_cutover(overlay, installed)
        });
        running.await.map_err(|_| ChunkKvError::WriteStalled)?;
        // The observer was registered before replay. A full observer expires
        // this installation, without rejecting any parent mutation.
        permit.send(super::super::WorkerRequest::ChildSplitCutover(request));
        let (artifact, child) = installed.await.map_err(|_| ChunkKvError::WriteStalled)??;
        let artifact = self
            .finish_child_inheritance(&plan, &artifact, child_tree.as_ref())
            .await?;
        self.record_split_artifact(artifact.clone()).await?;
        self.metrics.split_finalization();
        Ok((artifact, child, cursor.delta_records))
    }
}

pub(crate) fn install_child_cutover(
    state: &mut super::super::WorkerState,
    request: ChildSplitCutoverRequest,
) {
    let result = install_child_cutover_inner(state, &request);
    let ChildSplitCutoverRequest {
        overlay, completion, ..
    } = request;
    drop(overlay);
    let _ = completion.send(result);
}

fn install_child_cutover_inner(
    state: &mut super::super::WorkerState,
    request: &ChildSplitCutoverRequest,
) -> Result<(SplitArtifact, PreparedSplitWriter)> {
    let cutover_seq = state.applied_seq.load(Ordering::Acquire);
    if state.journal_durable_seq.load(Ordering::Acquire) != cutover_seq {
        return Err(ChunkKvError::ApplyStateUnknown);
    }
    let cutover_offset = state.applied_end;
    let source = SplitSourceFrontier {
        parent_id: request.plan.parent_id,
        parent_epoch: request.plan.parent_epoch,
        checkpoint: request.base_checkpoint.clone(),
        cutover_offset,
        cutover_seq,
        journal: Arc::clone(&request.parent.journal),
    };
    let (artifact, mut child) = prepare_cutover_writers(request, &source)?;
    let seed = super::super::RecoverySeed {
        applied_seq: cutover_seq,
        applied_position: 0,
        applied_end: 0,
        retry_replay_offset: 0,
        results: state.results.clone(),
        result_order: state.result_order.clone(),
        expired_floor: state.expired_floor.clone(),
        recovered: false,
    };
    let live = child.prepare_live_cutover((*request.parent.config).clone(), &artifact, seed)?;
    let ingress = Arc::new(request.parent.prepare_child_split_ingress(&request.plan, live)?);
    let retained_artifact = artifact.clone();
    let mut transition =
        request.parent.split_transition.try_lock().map_err(|_| {
            ChunkKvError::SplitRetry("split transition is busy before route installation".into())
        })?;
    let active = transition
        .as_mut()
        .filter(|active| active.plan == request.plan)
        .ok_or_else(|| ChunkKvError::SplitRetry("cutover has no matching prepared transition".into()))?;
    if active.inheritance.is_some() || request.parent.split_ingress.load().is_some() {
        return Err(ChunkKvError::SplitRetry("cutover is already installed".into()));
    }
    let installed = request.overlay.try_install(cutover_seq)?;
    // Every fallible validation and allocation precedes this commit. The
    // private child is now complete, so route publication cannot strand a
    // committed native prefix behind a still-writing parent.
    active.inheritance = Some(super::inheritance::ChildInheritance::installed(
        retained_artifact,
        installed,
    ));
    request.parent.split_ingress.store(Some(ingress));
    // Once the route exists, retries must finalize this installed prefix even
    // if the control caller is cancelled before receiving its acknowledgement.
    request.parent.pending_child_preparation.store(None);
    state.split_ingress = Arc::clone(&request.parent.split_ingress);
    state.ownership_epoch = Arc::clone(&request.parent.ownership_epoch);
    state.lifecycle = Arc::clone(&request.parent.lifecycle);
    state.lifecycle.store(
        super::super::lifecycle_code(PartitionLifecycle::SplitFinalizing),
        Ordering::Release,
    );
    Ok((artifact, child))
}

fn prepare_cutover_writers(
    request: &ChildSplitCutoverRequest,
    source: &SplitSourceFrontier,
) -> Result<(SplitArtifact, PreparedSplitWriter)> {
    let child = prepare_writer_at(
        &request.plan.child,
        request.target.clone(),
        Arc::clone(&request.child_tree),
        request.child_base,
        source,
        request.replayed_prefix,
    )?;
    let mut retained = prepare_retained_parent(
        &request.plan,
        (
            request.base_checkpoint.tree_manifest,
            request.base_checkpoint.root_manifest_generation,
            request.base_checkpoint.applied_seq,
        ),
        source,
    )?;
    retained.applied_seq = source.cutover_seq;
    retained.child_stream_start_seq = source
        .cutover_seq
        .checked_add(1)
        .ok_or_else(|| ChunkKvError::InvalidRequest("split cutover sequence overflows".into()))?;
    let artifact = SplitArtifact {
        transition_id: request.plan.transition_id,
        parent_id: request.plan.parent_id,
        parent_epoch: request.plan.parent_epoch,
        parent_next_epoch: request.plan.parent_next_epoch,
        shared_view_generation: 0,
        cutover_seq: source.cutover_seq,
        retained_parent: retained,
        child: child.artifact.clone(),
    };
    Ok((artifact, child))
}

fn complete_prepared_cutover(
    overlay: Arc<dyn crate::PreparedSplitOverlay>,
    installed: tokio::sync::oneshot::Receiver<Result<(SplitArtifact, PreparedSplitWriter)>>,
) -> Result<(SplitArtifact, PreparedSplitWriter)> {
    let result = installed.blocking_recv().map_err(|_| ChunkKvError::WriteStalled);
    // The worker (or a cancelled request) drops its token before completing
    // this channel. Native capability destruction therefore stays on the host.
    drop(overlay);
    result?
}
