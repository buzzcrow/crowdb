// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A live grant cannot substitute for activated writers of current assignments.

use super::{CatalogSnapshot, ChunkKvService};
use crowdb_chunk_kv::PartitionLifecycle;
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogPartitionState, RequestRouting};

impl ChunkKvService {
    pub(super) fn assignments_ready(&self, catalog: &CatalogSnapshot, now: u64) -> bool {
        catalog
            .entries
            .iter()
            .filter(|entry| {
                entry.owner.instance_id == self.instance_id
                    && entry.state != ChunkKvRangeCatalogPartitionState::Retired
            })
            .all(|entry| {
                if !matches!(
                    entry.state,
                    ChunkKvRangeCatalogPartitionState::Serving
                        | ChunkKvRangeCatalogPartitionState::SplitPreparing
                        | ChunkKvRangeCatalogPartitionState::SplitFinalizing
                ) || self
                    .authority
                    .authorize(catalog.generation, entry.partition_id, entry.owner_epoch, now)
                    .is_err()
                {
                    return false;
                }
                let routing = RequestRouting {
                    request_id: crowdb_protocol::chunk_kv::ClientRequestId::default(),
                    map_revision: catalog.generation,
                    partition_id: entry.partition_id,
                    owner_epoch: entry.owner_epoch,
                    min_journal_position: None,
                    deadline_ms: None,
                };
                self.partition_for_request(&routing, &entry.range.start, entry)
                    .is_some_and(|partition| {
                        let writer = partition.split_ingress().map_or(partition.clone(), |ingress| {
                            ingress.writer_for_key(&entry.range.start)
                        });
                        matches!(
                            writer.lifecycle(),
                            PartitionLifecycle::Serving
                                | PartitionLifecycle::SplitPreparing
                                | PartitionLifecycle::SplitFinalizing
                        )
                    })
            })
    }
}
