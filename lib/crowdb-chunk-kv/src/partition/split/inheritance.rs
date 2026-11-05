// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Retains one native source capture across background publication retries.

use super::{ChunkKvError, Partition, PartitionTree, Result, SplitArtifact, SplitPlan};

pub(in crate::partition) struct ChildInheritance {
    generation: Option<u64>,
    artifact: SplitArtifact,
    published: bool,
    journal_published: bool,
}

impl ChildInheritance {
    pub(super) fn installed(artifact: SplitArtifact, installed: crate::InstalledSplit) -> Self {
        debug_assert_eq!(artifact.cutover_seq, installed.frontier);
        Self {
            generation: Some(installed.generation),
            artifact,
            published: false,
            journal_published: false,
        }
    }

    pub(super) fn replayed(artifact: SplitArtifact) -> Self {
        Self {
            generation: None,
            artifact,
            published: true,
            journal_published: true,
        }
    }
}

impl Partition {
    pub(super) async fn retained_child_artifact(&self, plan: &SplitPlan) -> Result<Option<SplitArtifact>> {
        let transition = self.split_transition.lock().await;
        let active = transition
            .as_ref()
            .filter(|active| active.plan == *plan)
            .ok_or_else(|| ChunkKvError::SplitRetry("split inheritance has no matching transition".into()))?;
        Ok(active
            .inheritance
            .as_ref()
            .map(|inheritance| inheritance.artifact.clone()))
    }

    pub(super) async fn finish_child_inheritance(
        &self,
        plan: &SplitPlan,
        artifact: &SplitArtifact,
        child_tree: &dyn PartitionTree,
    ) -> Result<SplitArtifact> {
        // This existing control-plane mutex serializes finalizers. Foreground
        // reads and mutations use the installed ingress without taking it.
        let mut transition = self.split_transition.lock().await;
        let active = transition
            .as_mut()
            .filter(|active| active.plan == *plan)
            .ok_or_else(|| ChunkKvError::SplitRetry("split inheritance has no matching transition".into()))?;
        let inheritance = active.inheritance.get_or_insert_with(|| ChildInheritance {
            generation: None,
            artifact: artifact.clone(),
            published: false,
            journal_published: false,
        });
        if inheritance.artifact.cutover_seq != artifact.cutover_seq
            || inheritance.artifact.child != artifact.child
        {
            return Err(ChunkKvError::SplitRetry("split inheritance proof changed".into()));
        }
        if !inheritance.journal_published {
            self.publish_split_journal_prefix(inheritance.artifact.child.parent_cutover_offset)
                .await?;
            inheritance.journal_published = true;
        }
        if inheritance.generation.is_none() && !inheritance.published {
            let (generation, captured) = self.tree.begin_split_memtable_view().await?;
            if captured < artifact.cutover_seq {
                self.tree.release_split_memtable_view(generation).await?;
                return Err(ChunkKvError::ApplyStateUnknown);
            }
            inheritance.generation = Some(generation);
        }
        if let Some(generation) = inheritance.generation {
            if !inheritance.published {
                self.tree
                    .publish_split_memtable_view(
                        generation,
                        inheritance.artifact.cutover_seq,
                        child_tree,
                        &plan.child.range,
                    )
                    .await?;
                inheritance.published = true;
            }
            // A failed publication retains the same source generation. If
            // release alone fails, the next attempt only releases that owner.
            self.tree.release_split_memtable_view(generation).await?;
            inheritance.generation = None;
        }
        Ok(inheritance.artifact.clone())
    }
}
