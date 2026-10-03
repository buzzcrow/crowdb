// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed-layout bitmap publication and consistent group-0 reads.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_protocol::chunk_slot::{
    ChunkSlotBinding, ChunkSlotBootstrap, ChunkSlotMap, ChunkSlotMapHead, ChunkSlotOwner, ChunkStorageGroup,
};
use crowdb_protocol::key::{
    ChunkServiceSlotsKey, ChunkSlotMapHeadKey, ChunkStorageSlotsKey, ChunkdbRangeBindingKey, TextKey,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{BatchOp, CrowdbKvClient, Error, GetOutcome, ReadMode, Result};

/// Reads each layer independently; ordinary chunk clients only need service slots.
pub struct ChunkSlotMapClient {
    kv: Arc<CrowdbKvClient>,
}

impl ChunkSlotMapClient {
    #[must_use]
    pub fn new(kv: Arc<CrowdbKvClient>) -> Self {
        Self { kv }
    }

    /// Initialize an explicitly configured layout after checking all selected
    /// groups are available. Existing assignments must match exactly.
    ///
    /// # Errors
    /// Returns validation, availability, conflicting-layout or publication errors.
    pub async fn initialize_layout(&self, bootstrap: &ChunkSlotBootstrap) -> Result<()> {
        let service = bootstrap
            .service_map()
            .map_err(|error| invalid("chunk slots", &error.to_string()))?;
        let storage = bootstrap
            .storage_map()
            .map_err(|error| invalid("chunk slots", &error.to_string()))?;
        for group in &bootstrap.storage_groups {
            self.kv
                .get(
                    group.store_id,
                    group.group_id,
                    b"/chunkdb/readiness",
                    ReadMode::Linearizable,
                    None,
                )
                .await?;
        }
        self.initialize_storage(&storage).await?;
        self.initialize_service(&service).await
    }

    /// Read a complete service map, without consulting the storage map.
    ///
    /// # Errors
    /// Rejects missing, partial, legacy, corrupt or concurrently changed layouts.
    pub async fn read_service(&self) -> Result<ChunkSlotMap<u64>> {
        self.read().await
    }

    /// Read a complete storage map, rejecting group-zero destinations.
    ///
    /// # Errors
    /// Rejects missing, partial, legacy, corrupt or concurrently changed layouts.
    pub async fn read_storage(&self) -> Result<ChunkSlotMap<ChunkStorageGroup>> {
        self.read().await
    }

    /// Publish a complete initial service assignment once. Retry is idempotent.
    ///
    /// # Errors
    /// Rejects legacy records, conflicting initialization or unsupported reassignment.
    pub async fn initialize_service(&self, map: &ChunkSlotMap<u64>) -> Result<()> {
        self.initialize(map).await
    }

    /// Publish explicitly selected storage groups; never infer eligibility from discovery.
    /// The caller must provision these groups before allowing chunk allocation.
    ///
    /// # Errors
    /// Rejects legacy records, conflicting initialization or unsupported remapping.
    pub async fn initialize_storage(&self, map: &ChunkSlotMap<ChunkStorageGroup>) -> Result<()> {
        self.initialize(map).await
    }

    async fn read<O: MapOwner>(&self) -> Result<ChunkSlotMap<O>> {
        self.reject_legacy().await?;
        let key = O::head().to_path();
        let (head, revision) = self
            .read_head::<O>()
            .await?
            .ok_or_else(|| invalid(&key, "slot map is not initialized"))?;
        let records = self.scan_prefix(&O::prefix()).await?;
        let mut bindings = Vec::with_capacity(records.len());
        for (record_key, value) in records {
            let binding: ChunkSlotBinding<O> = decode(&record_key, &value)?;
            if binding.owner.key().as_bytes() != record_key.as_ref() {
                return Err(invalid(&key, "slot binding key/owner mismatch"));
            }
            bindings.push(binding);
        }
        let (after, after_revision) = self
            .read_head::<O>()
            .await?
            .ok_or_else(|| invalid(&key, "slot map head disappeared"))?;
        if head != after || revision != after_revision {
            return Err(invalid(&key, "slot map changed while reading"));
        }
        ChunkSlotMap::new(head, bindings).map_err(|error| invalid(&key, &error.to_string()))
    }

    async fn initialize<O: MapOwner>(&self, map: &ChunkSlotMap<O>) -> Result<()> {
        self.reject_legacy().await?;
        if self.read_head::<O>().await?.is_some() {
            return self.check_initialized(map).await;
        }
        self.reject_legacy_chunk_state().await?;
        let key = O::head().to_path();
        if !self.scan_prefix(&O::prefix()).await?.is_empty() {
            return Err(invalid(&key, "orphan slot bindings require explicit recovery"));
        }
        let mut ops = Vec::with_capacity(map.bindings().len() + 1);
        for binding in map.bindings() {
            ops.push(put(binding.owner.key(), binding)?);
        }
        ops.push(put(key.clone(), map.head())?);
        match self.kv.batch_write_cas(0, 0, &ops, key.as_bytes(), 0).await {
            Ok(_) => Ok(()),
            Err(Error::OutcomeUnknown | Error::CasFailed { .. } | Error::CasBusy) => {
                self.check_initialized(map).await
            }
            Err(error) => Err(error),
        }
    }

    async fn check_initialized<O: MapOwner>(&self, expected: &ChunkSlotMap<O>) -> Result<()> {
        let actual = self.read::<O>().await?;
        if actual.head() != expected.head()
            || actual
                .bindings()
                .iter()
                .any(|entry| !expected.bindings().contains(entry))
        {
            return Err(invalid(
                &O::head().to_path(),
                "slot map is fixed; migration is required",
            ));
        }
        Ok(())
    }

    async fn read_head<O: MapOwner>(&self) -> Result<Option<(ChunkSlotMapHead, u64)>> {
        let key = O::head().to_path();
        match self
            .kv
            .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, revision } => Ok(Some((decode(key.as_bytes(), &value)?, revision))),
        }
    }

    async fn reject_legacy(&self) -> Result<()> {
        let prefix = ChunkdbRangeBindingKey::prefix_all();
        let page = self
            .kv
            .scan_bounded_at(0, 0, prefix.as_bytes(), &[], &[], 1, true, None, 0)
            .await?;
        if !page.items.is_empty() || page.truncated || page.timed_out {
            return Err(invalid(
                &prefix,
                "legacy range layout requires explicit conversion",
            ));
        }
        Ok(())
    }

    async fn reject_legacy_chunk_state(&self) -> Result<()> {
        use crowdb_protocol::key::{
            ChunkTaskKey, FinalizeChunkTaskKey, LeasedChunkTaskKey, ReadyChunkTaskKey,
        };
        for prefix in [
            b"/chunk/".to_vec(),
            b"/reservation/".to_vec(),
            // Retired task indexes must not be mistaken for an empty layout.
            vec![0xC0, 0, 0x0E],
            vec![0xC0, 0, 0x0F],
            vec![0xC0, 0, 0x10],
            ChunkTaskKey::prefix_all(),
            FinalizeChunkTaskKey::prefix_all(),
            ReadyChunkTaskKey::prefix_all(),
            LeasedChunkTaskKey::prefix_all(),
        ] {
            let page = self
                .kv
                .scan_bounded_at(0, 0, &prefix, &[], &[], 1, true, None, 0)
                .await?;
            if !page.items.is_empty() || page.truncated || page.timed_out {
                return Err(invalid(
                    "chunk slots",
                    "legacy per-chunk state in group 0 requires explicit conversion",
                ));
            }
        }
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(Bytes, Bytes)>> {
        let mut records = Vec::new();
        let mut start = Bytes::new();
        let mut cutoff = 0;
        loop {
            let page = self
                .kv
                .scan_bounded_at(0, 0, prefix.as_bytes(), &start, &[], 256, false, None, cutoff)
                .await?;
            if page.timed_out || (page.truncated && page.items.is_empty()) {
                return Err(invalid(prefix, "slot map scan made incomplete progress"));
            }
            cutoff = page.scan_cutoff;
            if let Some((last, _)) = page.items.last() {
                start = last.clone();
            }
            records.extend(page.items);
            if !page.truncated {
                return Ok(records);
            }
        }
    }
}

trait MapOwner: ChunkSlotOwner + Serialize + DeserializeOwned {
    fn head() -> ChunkSlotMapHeadKey;
    fn prefix() -> String;
    fn key(self) -> String;
}

impl MapOwner for u64 {
    fn head() -> ChunkSlotMapHeadKey {
        ChunkSlotMapHeadKey::Service
    }
    fn prefix() -> String {
        ChunkServiceSlotsKey::prefix_all()
    }
    fn key(self) -> String {
        ChunkServiceSlotsKey { instance_id: self }.to_path()
    }
}

impl MapOwner for ChunkStorageGroup {
    fn head() -> ChunkSlotMapHeadKey {
        ChunkSlotMapHeadKey::Storage
    }
    fn prefix() -> String {
        ChunkStorageSlotsKey::prefix_all()
    }
    fn key(self) -> String {
        ChunkStorageSlotsKey {
            store_id: self.store_id,
            group_id: self.group_id,
        }
        .to_path()
    }
}

fn invalid(key: &str, reason: &str) -> Error {
    Error::SysdataDecode {
        key: key.into(),
        reason: reason.into(),
    }
}

fn decode<T: DeserializeOwned>(key: &[u8], bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| invalid(&String::from_utf8_lossy(key), &error.to_string()))
}

fn put<T: Serialize>(key: String, value: &T) -> Result<BatchOp> {
    let value = serde_json::to_vec(value).map_err(|error| invalid(&key, &error.to_string()))?;
    Ok(BatchOp::Put {
        key: Bytes::from(key),
        value: Bytes::from(value),
    })
}
