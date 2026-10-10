// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recovery of a committed local handoff before final readiness publication.

use super::{decode_frame, FrameDecode, Partition, PartitionJournal, PartitionTree};
use crate::{Checkpoint, ChunkKvError, PartitionConfig, PreparedSplitWriterArtifact, Result};
use bytes::{Buf, BytesMut};
use std::sync::Arc;

impl Partition {
    /// Restores child dispatch after replaying a durable handoff.
    ///
    /// # Errors
    /// Rejects a conflicting plan, child artifact or parent replay frontier.
    #[allow(clippy::too_many_lines)]
    pub async fn resume_split_child_session(
        &self,
        plan: crate::SplitPlan,
        child: Partition,
        child_artifact: PreparedSplitWriterArtifact,
    ) -> Result<super::PreparedSplit> {
        plan.validate()?;
        self.validate_epoch(plan.parent_epoch)?;
        if self.lifecycle() != crate::PartitionLifecycle::Prepared {
            return self
                .resume_live_child_preparation(plan, child, child_artifact)
                .await;
        }
        if plan.parent_id != self.id
            || plan.parent_range != *self.range.load_full()
            || self.snapshot().applied_seq < child_artifact.applied_seq
        {
            return Err(ChunkKvError::SplitRetry(
                "recovered split parent does not cover handoff".into(),
            ));
        }
        let (manifest, applied) = self.tree.checkpoint_state()?;
        self.retain_generation_pin(plan.transition_id, self.tree.root_manifest_generation()?)?;
        let frontier = super::SplitSourceFrontier {
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            checkpoint: Checkpoint {
                tree_id: self.tree_id(),
                tree_manifest: manifest,
                root_manifest_generation: self.tree.root_manifest_generation()?,
                applied_seq: applied,
                stream_name: self.journal.stream_name(),
                stream_manifest_generation: self.journal.manifest_generation(),
                replay_offset: child_artifact.parent_replay_offset,
            },
            cutover_offset: child_artifact.parent_cutover_offset,
            cutover_seq: child_artifact.applied_seq,
            journal: self.journal.clone(),
        };
        let mut retained = super::prepare_retained_parent(
            &plan,
            (manifest, frontier.checkpoint.root_manifest_generation, applied),
            &frontier,
        )?;
        retained.applied_seq = child_artifact.applied_seq;
        retained.child_stream_start_seq = child_artifact.child_stream_start_seq;
        let artifact = crate::SplitArtifact {
            transition_id: plan.transition_id,
            parent_id: plan.parent_id,
            parent_epoch: plan.parent_epoch,
            parent_next_epoch: plan.parent_next_epoch,
            shared_view_generation: 0,
            cutover_seq: child_artifact.applied_seq,
            retained_parent: retained,
            child: child_artifact.clone(),
        };
        super::super::validate_split_artifact(&plan, &artifact, artifact.cutover_seq)?;
        child
            .tree
            .force_advance_split_frontier(artifact.cutover_seq)
            .await?;
        child.activate_local_split_writer(&artifact)?;
        {
            let mut current = self.split_transition.lock().await;
            if current.as_ref().is_some_and(|active| active.plan != plan) {
                return Err(ChunkKvError::SplitRetry(
                    "another recovered split is active".into(),
                ));
            }
            *current = Some(super::super::SplitTransition {
                plan: plan.clone(),
                artifact: None,
            });
        }
        self.install_child_split_ingress(&plan, child.clone()).await?;
        super::stable_session::finish_child_inheritance(
            self.tree.as_ref(),
            &plan,
            &artifact,
            child.tree.as_ref(),
        )
        .await?;
        self.lifecycle.store(
            super::super::lifecycle_code(crate::PartitionLifecycle::SplitFinalizing),
            std::sync::atomic::Ordering::Release,
        );
        self.record_split_artifact(artifact.clone()).await?;
        let checkpoint = Checkpoint {
            tree_id: child_artifact.tree_id,
            tree_manifest: child_artifact.tree_manifest,
            root_manifest_generation: child_artifact.root_manifest_generation,
            applied_seq: child_artifact.base_applied_seq,
            stream_name: child_artifact.stream_name,
            stream_manifest_generation: child.journal.manifest_generation(),
            replay_offset: 0,
        };
        Ok(super::PreparedSplit {
            artifact,
            child: super::PreparedSplitWriter {
                artifact: child_artifact,
                checkpoint,
                tree: child.tree.clone(),
                journal: child.journal.clone(),
                parent_journal: self.journal.clone(),
                live: Some(child),
            },
            child_rebuild: crowdb_tree_ffi::RangeRebuildStats::default(),
            delta_records: 0,
        })
    }

    async fn resume_live_child_preparation(
        &self,
        plan: crate::SplitPlan,
        child: Partition,
        proof: PreparedSplitWriterArtifact,
    ) -> Result<super::PreparedSplit> {
        if self.split_ingress().is_some() || child.journal.tail() != 0 {
            return Err(ChunkKvError::SplitRetry(
                "existing split dispatch must be finalized in place".into(),
            ));
        }
        self.begin_split(plan.clone()).await?;
        let (manifest, applied_seq) = self.tree.checkpoint_state()?;
        let checkpoint = Checkpoint {
            tree_id: self.tree_id(),
            tree_manifest: manifest,
            root_manifest_generation: self.tree.root_manifest_generation()?,
            applied_seq,
            stream_name: self.journal.stream_name(),
            stream_manifest_generation: self.journal.manifest_generation(),
            replay_offset: proof.parent_replay_offset,
        };
        let target = super::SplitWriterTarget {
            tree_id: proof.tree_id,
            tree_config: crowdb_tree_ffi::Config::default(),
            journal: child.journal.clone(),
        };
        let (artifact, child) = self
            .install_child_cutover_session(
                plan,
                target,
                checkpoint,
                child.tree.clone(),
                (
                    proof.tree_manifest,
                    proof.root_manifest_generation,
                    proof.base_applied_seq,
                ),
            )
            .await?;
        Ok(super::PreparedSplit {
            artifact,
            child,
            child_rebuild: crowdb_tree_ffi::RangeRebuildStats::default(),
            delta_records: 0,
        })
    }

    /// Opens the pinned native child base and resumes both durable WALs.
    ///
    /// # Errors
    /// Returns an exact-base, stream identity or WAL replay error.
    pub async fn recover_native_split_handoff(
        artifact: PreparedSplitWriterArtifact,
        config: PartitionConfig,
        tree_config: crowdb_tree_ffi::Config,
        page_store: Arc<crowdb_tree_ffi::PageStore>,
        stream: crowdb_chunk_stream::ChunkStream,
        parent_stream: crowdb_chunk_stream::ChunkStream,
    ) -> Result<(Self, PreparedSplitWriterArtifact)> {
        let (tree, journal) = super::super::native_storage_parts(
            artifact.tree_id,
            &artifact.range,
            tree_config,
            page_store,
            stream,
        )?;
        let source = Arc::new(crate::StreamPartitionJournal::new(
            parent_stream,
            artifact.parent_stream_name,
        )?);
        Self::recover_split_handoff(artifact, config, tree, journal, source).await
    }

    /// Replays a committed split from its pinned base and both journal tails.
    ///
    /// # Errors
    /// Rejects missing parent coverage, conflicting identities or malformed WAL.
    pub async fn recover_split_handoff(
        mut artifact: PreparedSplitWriterArtifact,
        config: PartitionConfig,
        tree: Arc<dyn PartitionTree>,
        journal: Arc<dyn PartitionJournal>,
        parent_journal: Arc<dyn PartitionJournal>,
    ) -> Result<(Self, PreparedSplitWriterArtifact)> {
        let first_child = first_record(journal.as_ref()).await?;
        let inherited_end = first_child
            .as_ref()
            .map(|record| {
                record
                    .mutation_seq
                    .checked_sub(1)
                    .ok_or_else(|| ChunkKvError::JournalCorruption("zero child WAL sequence".into()))
            })
            .transpose()?;
        if let Some(first) = &first_child {
            super::super::validate_replay_record(artifact.partition_id, artifact.ownership_epoch, first)?;
        }
        let (sequence, offset) =
            inherited_frontier(&artifact, parent_journal.as_ref(), inherited_end).await?;
        artifact.applied_seq = sequence;
        artifact.parent_cutover_offset = offset;
        artifact.child_stream_start_seq = sequence
            .checked_add(1)
            .ok_or_else(|| ChunkKvError::JournalCorruption("child WAL sequence overflow".into()))?;
        let checkpoint = Checkpoint {
            tree_id: artifact.tree_id,
            tree_manifest: artifact.tree_manifest,
            root_manifest_generation: artifact.root_manifest_generation,
            applied_seq: artifact.base_applied_seq,
            stream_name: artifact.stream_name,
            stream_manifest_generation: journal.manifest_generation(),
            replay_offset: 0,
        };
        let partition = Self::recover_prepared_overlay(
            artifact.clone(),
            checkpoint,
            config,
            tree,
            journal,
            parent_journal,
        )
        .await?;
        Ok((partition, artifact))
    }
}

async fn first_record(journal: &dyn PartitionJournal) -> Result<Option<crate::WalRecord>> {
    let mut buffered = BytesMut::new();
    let mut offset = 0;
    while offset < journal.tail() {
        let bytes = journal.read_window(offset, 64 * 1024).await?;
        if bytes.is_empty() {
            return Err(ChunkKvError::IncompleteFrame);
        }
        offset += bytes.len() as u64;
        buffered.extend_from_slice(&bytes);
        if let FrameDecode::Complete(frame) = decode_frame(&buffered)? {
            return Ok(Some(frame.record));
        }
        if buffered.len() > crate::MAX_FRAME_BYTES {
            return Err(ChunkKvError::IncompleteFrame);
        }
    }
    if !buffered.is_empty() {
        return Err(ChunkKvError::IncompleteFrame);
    }
    Ok(None)
}

async fn inherited_frontier(
    artifact: &PreparedSplitWriterArtifact,
    journal: &dyn PartitionJournal,
    target: Option<u64>,
) -> Result<(u64, u64)> {
    if target.is_some_and(|sequence| sequence < artifact.applied_seq) {
        return Err(ChunkKvError::JournalCorruption(
            "child WAL precedes committed handoff".into(),
        ));
    }
    let mut sequence = artifact.base_applied_seq;
    let mut offset = artifact.parent_replay_offset;
    let mut frame_offset = offset;
    let mut buffered = BytesMut::new();
    while offset < journal.tail() {
        let bytes = journal.read_window(offset, 1024 * 1024).await?;
        if bytes.is_empty() {
            return Err(ChunkKvError::IncompleteFrame);
        }
        offset += bytes.len() as u64;
        buffered.extend_from_slice(&bytes);
        while let FrameDecode::Complete(frame) = decode_frame(&buffered)? {
            super::super::validate_replay_record(artifact.parent_id, artifact.parent_epoch, &frame.record)?;
            if frame.record.mutation_seq > sequence {
                if frame.record.mutation_seq != sequence.checked_add(1).unwrap_or(0) {
                    return Err(ChunkKvError::JournalCorruption(
                        "parent handoff WAL sequence gap".into(),
                    ));
                }
                sequence = frame.record.mutation_seq;
            }
            frame_offset += frame.bytes_consumed as u64;
            buffered.advance(frame.bytes_consumed);
            if target == Some(sequence) && frame_offset >= artifact.parent_cutover_offset {
                return Ok((sequence, frame_offset));
            }
        }
        if buffered.len() > crate::MAX_FRAME_BYTES {
            return Err(ChunkKvError::IncompleteFrame);
        }
    }
    if !buffered.is_empty() || sequence < artifact.applied_seq || target.is_some_and(|end| end != sequence) {
        return Err(ChunkKvError::JournalCorruption(
            "parent WAL does not cover child inheritance".into(),
        ));
    }
    Ok((sequence, frame_offset))
}
