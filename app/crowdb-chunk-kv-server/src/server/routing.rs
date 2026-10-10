// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Current assignment selection and bounded historical split compatibility.

use super::{partition_matches_entry, ChunkKvService};
use crowdb_chunk_kv::Partition;
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogEntry, RequestRouting};

impl ChunkKvService {
    pub(super) fn partition_for_request(
        &self,
        routing: &RequestRouting,
        key: &[u8],
        entry: &ChunkKvRangeCatalogEntry,
    ) -> Option<Partition> {
        let current = self
            .partitions
            .load()
            .get(&entry.partition_id)
            .filter(|partition| partition_matches_entry(partition, entry))
            .cloned();
        if routing.partition_id == entry.partition_id && routing.owner_epoch == entry.owner_epoch {
            if let Some(partition) = current.as_ref() {
                return Some(partition.clone());
            }
        }
        self.local_split_sessions
            .load()
            .get(&routing.partition_id)
            .map(|session| session.dispatcher.clone())
            .filter(|dispatcher| {
                dispatcher.snapshot().range.contains(key)
                    && (partition_matches_entry(dispatcher, entry)
                        || dispatcher.split_ingress().is_some_and(|ingress| {
                            partition_matches_entry(&ingress.writer_for_key(key), entry)
                        }))
            })
            .or(current)
            .or_else(|| self.partition_for_catalog_entry(entry))
    }
}
