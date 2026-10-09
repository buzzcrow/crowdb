// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Recovery of the exact child base recorded before local dispatch.

use super::{prepared_overlay_artifact, split_plan, storage_plan_error, ChunkKvStorage};
use crate::MonitorError;
use crowdb_chunk_kv::{Partition, PartitionConfig, PreparedSplit};
use crowdb_chunk_stream::StreamRegistry;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, SplitTransition,
};
use crowdb_tree_ffi::ChunkPageStoreOptions;

impl ChunkKvStorage {
    pub(super) async fn resume_split_handoff(
        &self,
        parent: &Partition,
        transition: &SplitTransition,
    ) -> Result<PreparedSplit, MonitorError> {
        if parent.split_ingress().is_some() {
            return parent
                .finalize_installed_split_child_session(split_plan(transition))
                .await
                .map_err(|error| storage_plan_error(&error.to_string()));
        }
        let proof = transition
            .handoff_proof
            .as_ref()
            .ok_or_else(|| storage_plan_error("split handoff proof is absent"))?;
        let binding = self
            .streams
            .registry()
            .load(transition.child.artifact.stream_name)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?
            .ok_or_else(|| storage_plan_error("handoff child stream is absent"))?;
        let child_stream = self
            .open_stream(
                transition.child.artifact.stream_name,
                transition.child.owner_epoch,
            )
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        let source_stream = self
            .streams
            .open_read_only_current(proof.source_stream_name, self.metadata_store_id)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        let page_store = self
            .open_durable_tree_page_store(
                ChunkPageStoreOptions {
                    tree_id: transition.child.artifact.tree_id,
                    owner_epoch: transition.child.owner_epoch,
                    open_generation: proof.base_root_manifest_generation,
                    pack_bytes: 0,
                    iu_size: 0,
                    max_concurrent_packs: 0,
                    materialization_bytes_per_pass: 0,
                    mirror_copies: 0,
                    max_chunk_bytes: 0,
                },
                binding.metadata_group_id,
            )
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?;
        let entry = ChunkKvRangeCatalogEntry {
            partition_id: transition.child.partition_id,
            range: transition.child.range.clone(),
            owner: transition.child.owner.clone(),
            owner_epoch: transition.child.owner_epoch,
            state: ChunkKvRangeCatalogPartitionState::Prepared,
            artifact: transition.child.artifact.clone(),
            transition_id: Some(transition.transition_id),
        };
        let (child, artifact) = Partition::recover_native_split_handoff(
            prepared_overlay_artifact(&entry, proof),
            PartitionConfig::default(),
            crowdb_tree_ffi::Config::default(),
            page_store,
            child_stream,
            source_stream,
        )
        .await
        .map_err(|error| storage_plan_error(&error.to_string()))?;
        parent
            .resume_split_child_session(split_plan(transition), child, artifact)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))
    }
}
