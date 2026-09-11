// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV persistence layer for chunk metadata.
//!
//! `ChunkStore` reads/writes `Chunk` proto records to CROWDB KV via the
//! routed KV group. Serialization uses prost (no Rust type duplication,
//! design §3.8). The chunk ID (24 bytes) is the KV key.

pub mod reservation;

use std::sync::Arc;

use tracing::warn;

use crowdb_kv_client::{CrowdbKvClient, GetOutcome, ReadMode, ScanOutcome};
use crowdb_protocol::chunkdb::rpc::Chunk;
use crowdb_protocol::common::ChunkId;

use crate::routing::{route, BindingCache, MigrationState, Route};

/// Storage error.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("chunk not found")]
    ChunkNotFound,
    #[error("chunk already exists")]
    ChunkAlreadyExists,
    #[error("chunk changed concurrently")]
    Conflict,
    #[error("routing error: {0}")]
    Route(#[from] crate::routing::RouteError),
    #[error("kv client error: {0}")]
    Kv(String),
    #[error("serialization error: {0}")]
    Serde(String),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, StoreError>;

/// Chunk metadata store — persists `Chunk` records to CROWDB KV.
pub struct ChunkStore {
    pub(super) kv: Arc<CrowdbKvClient>,
    pub(super) bindings: BindingCache,
}

impl ChunkStore {
    #[must_use]
    pub fn new(kv: Arc<CrowdbKvClient>, bindings: BindingCache) -> Self {
        Self { kv, bindings }
    }

    /// Publish one logical chunk transition with KV revision CAS. New chunks
    /// use create-if-absent; updates must advance `modify_ts` exactly once.
    /// An ambiguous response is reconciled by reading the current record.
    pub async fn put_chunk(&self, chunk: &Chunk) -> Result<()> {
        let id = chunk.id.as_ref().expect("chunk has id");
        let r = route(&self.bindings, id)?;
        let key = chunk_key(id);
        let value = encode_chunk(chunk);

        self.put_chunk_at(r.kv_store_id, r.kv_group_id, &key, &value, chunk)
            .await?;
        if r.migration_state == MigrationState::Copying || r.migration_state == MigrationState::Cutover {
            // The new route is authoritative. Mirror the same guarded logical
            // transition to the old route during migration.
            if let (Some(old_store), Some(old_group)) = (r.old_kv_store_id, r.old_kv_group_id) {
                if let Err(e) = self.put_chunk_at(old_store, old_group, &key, &value, chunk).await {
                    warn!(error = %e, "dual-write: old group write failed (new group has data)");
                }
            }
        }
        Ok(())
    }

    async fn put_chunk_at(
        &self,
        store_id: u64,
        group_id: u64,
        key: &[u8],
        value: &[u8],
        chunk: &Chunk,
    ) -> Result<()> {
        let observed = self
            .kv
            .get(store_id, group_id, key, ReadMode::Linearizable, None)
            .await
            .map_err(|error| StoreError::Kv(error.to_string()))?;
        let expected_revision = match observed {
            GetOutcome::NotFound => 0,
            GetOutcome::Found {
                value: current,
                revision,
            } => {
                let current = decode_chunk(&current)?;
                if current == *chunk {
                    return Ok(());
                }
                if chunk.modify_ts != current.modify_ts.saturating_add(1) {
                    return Err(StoreError::Conflict);
                }
                revision
            }
        };
        match self
            .kv
            .put_cas(store_id, group_id, key, value, expected_revision)
            .await
        {
            Ok(_) => Ok(()),
            Err(
                crowdb_kv_client::Error::CasFailed { .. }
                | crowdb_kv_client::Error::CasBusy
                | crowdb_kv_client::Error::OutcomeUnknown,
            ) => {
                match self
                    .kv
                    .get(store_id, group_id, key, ReadMode::Linearizable, None)
                    .await
                {
                    Ok(GetOutcome::Found { value, .. }) if decode_chunk(&value)? == *chunk => Ok(()),
                    Ok(_) => Err(StoreError::Conflict),
                    Err(error) => Err(StoreError::Kv(error.to_string())),
                }
            }
            Err(error) => Err(StoreError::Kv(error.to_string())),
        }
    }

    /// Read a chunk by ID.
    pub async fn get_chunk(&self, id: &ChunkId) -> Result<Chunk> {
        let r = route(&self.bindings, id)?;
        let key = chunk_key(id);

        // Try new group first.
        if let Some(data) = self.get_chunk_raw(&r, &key).await? {
            return decode_chunk(&data);
        }

        // During migration, fall back to old group.
        if r.migration_state == MigrationState::Copying || r.migration_state == MigrationState::Cutover {
            if let (Some(old_store), Some(old_group)) = (r.old_kv_store_id, r.old_kv_group_id) {
                let old_route = Route {
                    kv_store_id: old_store,
                    kv_group_id: old_group,
                    migration_state: MigrationState::NotMigrating,
                    old_kv_store_id: None,
                    old_kv_group_id: None,
                };
                if let Some(data) = self.get_chunk_raw(&old_route, &key).await? {
                    return decode_chunk(&data);
                }
            }
        }

        Err(StoreError::ChunkNotFound)
    }

    /// Delete a chunk by ID.
    pub async fn delete_chunk(&self, id: &ChunkId) -> Result<()> {
        let r = route(&self.bindings, id)?;
        let key = chunk_key(id);

        // Delete from new group.
        self.kv
            .delete(r.kv_store_id, r.kv_group_id, &key, None)
            .await
            .map_err(|e| StoreError::Kv(e.to_string()))?;

        // During migration, also delete from old group.
        if r.migration_state == MigrationState::Copying || r.migration_state == MigrationState::Cutover {
            if let (Some(old_store), Some(old_group)) = (r.old_kv_store_id, r.old_kv_group_id) {
                let _ = self.kv.delete(old_store, old_group, &key, None).await;
            }
        }

        Ok(())
    }

    /// List chunks with pagination. Scans the routed KV group's
    /// keyspace starting from `start_after`.
    pub async fn list_chunks(&self, start_after: Option<&ChunkId>, max_keys: u32) -> Result<Vec<Chunk>> {
        let table = self.bindings.snapshot();
        if table.is_empty() {
            return Err(StoreError::Route(crate::routing::RouteError::NoBinding));
        }

        let prefix = b"/chunk/";
        let start = start_after.map(chunk_key).unwrap_or_default();
        let start_key: &[u8] = if start.is_empty() { &[] } else { &start };

        // Scan all bindings — chunks may be spread across multiple KV
        // groups. Each binding gets up to `max_keys` results; the
        // caller merges them.
        let mut chunks = Vec::new();
        for binding in table.bindings() {
            let outcome: ScanOutcome = self
                .kv
                .scan(
                    binding.kv_store_id,
                    binding.kv_group_id,
                    prefix,
                    start_key,
                    &[],
                    max_keys,
                    ReadMode::Linearizable,
                    None,
                    false,
                    None,
                )
                .await
                .map_err(|e| StoreError::Kv(e.to_string()))?;

            for (_key, value) in outcome.items {
                match decode_chunk(&value) {
                    Ok(chunk) => chunks.push(chunk),
                    Err(e) => {
                        warn!(error = %e, "list_chunks: failed to decode chunk, skipping");
                    }
                }
            }
        }
        chunks.sort_unstable_by_key(|chunk| chunk.id.map(|id| (id.high, id.low)));
        chunks.dedup_by_key(|chunk| chunk.id);
        chunks.truncate(usize::try_from(max_keys).unwrap_or(usize::MAX));
        Ok(chunks)
    }

    /// Raw get from a specific route.
    async fn get_chunk_raw(&self, r: &Route, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self
            .kv
            .get(r.kv_store_id, r.kv_group_id, key, ReadMode::Linearizable, None)
            .await
        {
            Ok(GetOutcome::Found { value, .. }) => Ok(Some(value.to_vec())),
            Ok(GetOutcome::NotFound) => Ok(None),
            Err(e) => {
                warn!(error = %e, "get_chunk_raw: kv get failed");
                Err(StoreError::Kv(e.to_string()))
            }
        }
    }
}

/// Build the KV key for a chunk ID: `/chunk/<16-byte-id>`.
pub(super) fn chunk_key(id: &ChunkId) -> Vec<u8> {
    let mut key = Vec::with_capacity(23);
    key.extend_from_slice(b"/chunk/");
    key.extend_from_slice(&id.high.to_be_bytes());
    key.extend_from_slice(&id.low.to_be_bytes());
    key
}

/// Encode a `Chunk` to bytes (bincode).
pub(super) fn encode_chunk(chunk: &Chunk) -> Vec<u8> {
    bincode::serialize(chunk).expect("Chunk serialization")
}

/// Decode a `Chunk` from bytes (bincode).
fn decode_chunk(data: &[u8]) -> Result<Chunk> {
    bincode::deserialize(data).map_err(|e| StoreError::Serde(e.to_string()))
}
