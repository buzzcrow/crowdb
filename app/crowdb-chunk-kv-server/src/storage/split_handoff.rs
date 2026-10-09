// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Revision-fenced local split handoff persistence.

use crate::{Group0ControlStore, SplitStateMachine};
use async_trait::async_trait;
use crowdb_chunk_kv::{ChunkKvError, PreparedSplitWriterArtifact, SplitHandoffStore};
use crowdb_protocol::chunk_kv::{SplitTransition, TailOverlayArtifact};
use std::sync::Arc;

pub(super) struct SplitHandoffCommit {
    pub store: Arc<Group0ControlStore>,
    pub expected: SplitTransition,
}

#[async_trait]
impl SplitHandoffStore for SplitHandoffCommit {
    async fn commit(&self, artifact: &PreparedSplitWriterArtifact) -> crowdb_chunk_kv::Result<()> {
        let (current, revision) = self
            .store
            .load_split_transition(self.expected.transition_id)
            .await
            .map_err(|error| ChunkKvError::SplitRetry(error.to_string()))?
            .ok_or_else(|| ChunkKvError::SplitRetry("split transition disappeared before handoff".into()))?;
        if current != self.expected {
            return Err(ChunkKvError::SplitRetry(
                "split transition changed before handoff".into(),
            ));
        }
        let proof = TailOverlayArtifact {
            source_partition_id: current.parent_id,
            source_epoch: current.parent_epoch,
            source_stream_name: artifact.parent_stream_name,
            source_stream_manifest_generation: artifact.parent_stream_manifest_generation,
            replay_offset: artifact.parent_replay_offset,
            cutover_offset: artifact.parent_cutover_offset,
            base_tree_manifest: artifact.tree_manifest,
            base_root_manifest_generation: artifact.root_manifest_generation,
            base_applied_seq: artifact.base_applied_seq,
            cutover_seq: artifact.applied_seq,
            target_stream_start_seq: artifact.child_stream_start_seq,
        };
        let mut machine = SplitStateMachine::restore(current)
            .map_err(|error| ChunkKvError::SplitRetry(error.to_string()))?;
        machine
            .record_handoff(proof)
            .map_err(|error| ChunkKvError::SplitRetry(error.to_string()))?;
        self.store
            .persist_split_transition(machine.transition(), revision)
            .await
            .map_err(|error| ChunkKvError::SplitRetry(error.to_string()))?;
        Ok(())
    }
}
