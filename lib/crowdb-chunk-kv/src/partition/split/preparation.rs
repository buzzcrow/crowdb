// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Exact preparation reuse while handoff publication is unconfirmed.

use super::{
    checkpoint_prepared_tree, prepare_writer, validate_target, Checkpoint, ChunkKvError, DeltaCursor,
    Partition, PartitionTree, PreparedSplit, PreparedSplitWriterArtifact, Result, SplitHandoffStore,
    SplitPlan, SplitSourceFrontier, SplitWriterTarget,
};
use std::sync::{atomic::Ordering, Arc};

pub(crate) struct PendingChildPreparation {
    plan: SplitPlan,
    target: SplitWriterTarget,
    base_checkpoint: Checkpoint,
    child_tree: Arc<dyn PartitionTree>,
    child_base: (u64, u64, u64),
    artifact: PreparedSplitWriterArtifact,
    child_rebuild: crowdb_tree_ffi::RangeRebuildStats,
    delta_records: u64,
}

impl Partition {
    /// Returns the target retained for an unconfirmed handoff attempt.
    ///
    /// # Errors
    /// Rejects a different split plan without releasing the pending base's pins.
    pub fn pending_split_child_target(&self, plan: &SplitPlan) -> Result<Option<SplitWriterTarget>> {
        self.pending_child_preparation
            .load_full()
            .map(|prepared| {
                if &prepared.plan != plan {
                    return Err(ChunkKvError::SplitRetry(
                        "another child base awaits handoff confirmation".into(),
                    ));
                }
                Ok(prepared.target.clone())
            })
            .transpose()
    }

    pub(in crate::partition) fn discard_pending_child_preparation(&self) -> Result<()> {
        if let Some(prepared) = self.pending_child_preparation.load_full() {
            prepared
                .child_tree
                .unpin_generation(prepared.plan.transition_id)?;
            self.pending_child_preparation.store(None);
        }
        Ok(())
    }

    /// Prepares only the new child and commits handoff before local dispatch.
    ///
    /// # Errors
    /// Returns a base, journal, pin, handoff persistence or dispatch error.
    pub async fn prepare_split_child_session(
        &self,
        plan: SplitPlan,
        target: SplitWriterTarget,
        max_lag: u64,
        handoff: Arc<dyn SplitHandoffStore>,
    ) -> Result<PreparedSplit> {
        validate_target(self, &plan, &target)?;
        if target.journal.tail() != 0 {
            return Err(ChunkKvError::SplitRetry(
                "child WAL requires its persisted handoff recovery".into(),
            ));
        }
        if max_lag == 0 {
            return Err(ChunkKvError::InvalidRequest(
                "split catch-up lag bound must be nonzero".into(),
            ));
        }
        self.begin_split(plan.clone()).await?;
        let prepared = if let Some(prepared) = self.pending_child_preparation.load_full() {
            if prepared.plan != plan
                || prepared.target.tree_id != target.tree_id
                || prepared.target.journal.stream_name() != target.journal.stream_name()
            {
                return Err(ChunkKvError::SplitRetry(
                    "another child base awaits handoff confirmation".into(),
                ));
            }
            prepared
        } else {
            if self.checkpoint_pin_generation.load(Ordering::Acquire) != 0 {
                return Err(ChunkKvError::SplitRetry(
                    "parent still has a dependent transition checkpoint".into(),
                ));
            }
            let prepared = match self.prepare_child_base(plan.clone(), target, max_lag).await {
                Ok(prepared) => Arc::new(prepared),
                Err(error) => {
                    // Handoff has not been attempted, so this candidate cannot
                    // authorize dispatch. Release only this transition's pin.
                    self.release_generation_pin(plan.transition_id)?;
                    return Err(error);
                }
            };
            self.pending_child_preparation.store(Some(prepared.clone()));
            prepared
        };
        // An error can mean the CAS committed but its response was lost. Keep
        // this exact base and pins; rebuilding would change the durable proof.
        handoff.commit(&prepared.artifact).await?;
        let (artifact, child) = self
            .install_child_cutover_session(
                plan,
                prepared.target.clone(),
                prepared.base_checkpoint.clone(),
                prepared.child_tree.clone(),
                prepared.child_base,
            )
            .await?;
        Ok(PreparedSplit {
            artifact,
            child,
            child_rebuild: prepared.child_rebuild,
            delta_records: prepared.delta_records,
        })
    }

    async fn prepare_child_base(
        &self,
        plan: SplitPlan,
        target: SplitWriterTarget,
        max_lag: u64,
    ) -> Result<PendingChildPreparation> {
        let (base_checkpoint, source) = self.split_base_snapshot(&plan).await?;
        self.retain_generation_pin(plan.transition_id, base_checkpoint.root_manifest_generation)?;
        let (child_tree, child_rebuild) = source
            .rebuild_range(target.tree_id, &plan.child.range, target.tree_config.clone())
            .await?;
        let mut cursor = DeltaCursor {
            offset: base_checkpoint.replay_offset,
            applied_seq: base_checkpoint.applied_seq,
            delta_records: 0,
        };
        cursor
            .catch_up_to_lag(self, &plan, child_tree.as_ref(), &plan.child, max_lag)
            .await?;
        let child_base = checkpoint_prepared_tree(child_tree.as_ref(), cursor.applied_seq).await?;
        child_tree.pin_generation(plan.transition_id, child_base.1)?;
        let frontier = SplitSourceFrontier {
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            checkpoint: base_checkpoint.clone(),
            cutover_offset: cursor.offset,
            cutover_seq: cursor.applied_seq,
            journal: self.journal.clone(),
        };
        let prepared = prepare_writer(
            &plan.child,
            target.clone(),
            child_tree.clone(),
            child_base,
            &frontier,
        )?;
        Ok(PendingChildPreparation {
            plan,
            target,
            base_checkpoint,
            child_tree,
            child_base,
            artifact: prepared.artifact().clone(),
            child_rebuild,
            delta_records: cursor.delta_records,
        })
    }
}
