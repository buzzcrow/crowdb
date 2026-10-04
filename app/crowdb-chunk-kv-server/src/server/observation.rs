// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded observations of one hosted writer; never scan keys or journal data.

use std::sync::{atomic::Ordering, Arc};

use crowdb_protocol::chunk_kv::Id128;
use serde_json::{json, Value};

use super::ChunkKvService;

mod pages;
mod storage;
pub(crate) use pages::PageQuery;

impl ChunkKvService {
    /// Samples one writer under the requested catalog and ownership fences.
    /// Counters are independently sampled; this does not acquire serving authority.
    ///
    /// # Errors
    /// Returns a conflict when routing changed, or not-found for an unhosted writer.
    pub fn observe_partition(
        &self,
        id: Id128,
        generation: u64,
        epoch: u64,
        stream_generation: Option<u64>,
        stream_offset: usize,
    ) -> Result<Value, &'static str> {
        let catalog = self.catalog.load_full();
        if generation == 0 || catalog.generation != generation {
            return Err("catalog_changed");
        }
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.partition_id == id)
            .ok_or("partition_not_found")?;
        if entry.owner.instance_id != self.instance_id || entry.owner_epoch != epoch {
            return Err("owner_changed");
        }
        let hosted = self.partitions.load_full();
        let partition = hosted.get(&id).ok_or("partition_not_hosted")?;
        let snapshot = partition.snapshot();
        if snapshot.ownership_epoch != epoch
            || snapshot.range.start.as_deref().unwrap_or_default() != entry.range.start
            || snapshot.range.end != entry.range.end
            || partition.tree_id() != entry.artifact.tree_id
            || snapshot.stream_name != entry.artifact.stream_name
        {
            return Err("writer_changed");
        }
        let observed_at = self.monotonic_ms();
        let admitted = self.admitting.load(Ordering::Acquire);
        let live_grant = self.authority.has_live_grant(generation, observed_at);
        let journal = partition
            .observe_journal(stream_generation, stream_offset)
            .map_err(|_| "stream_observation_changed")?;
        let tree = storage::tree(partition.observe_tree());
        let result = json!({
            "partition_id":format!("{:016x}{:016x}", id.high, id.low),
            "instance_id":self.instance_id.to_string(), "catalog_generation":generation.to_string(),
            "owner_epoch":epoch.to_string(), "tree_id":partition.tree_id().to_string(),
            "stream_id":format!("{:016x}{:016x}", snapshot.stream_name.high, snapshot.stream_name.low),
            "lifecycle":format!("{:?}", snapshot.lifecycle),
            "admitting":admitted, "live_grant":live_grant,
            "journal_durable_seq":snapshot.journal_durable_seq.to_string(),
            "journal_durable_offset":snapshot.journal_durable_offset.to_string(),
            "applied_seq":snapshot.applied_seq.to_string(),
            "observed_at_monotonic_ms":observed_at.to_string(),
            "sampling":"independent counters", "data_pages_read":0,
            "tree":tree,
            "journal":journal.as_ref().map(storage::journal),
        });
        let current = partition.snapshot();
        if !Arc::ptr_eq(&catalog, &self.catalog.load_full())
            || !Arc::ptr_eq(&hosted, &self.partitions.load_full())
            || current.ownership_epoch != epoch
            || current.range != snapshot.range
        {
            return Err("observation_changed");
        }
        Ok(result)
    }
}
