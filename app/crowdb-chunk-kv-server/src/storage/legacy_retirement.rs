// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit stopped-legacy-format retirement. Unknown historical owners retain their receipts.
use super::ChunkKvStorage;
use crowdb_chunk_client::{ChunkAllocator, DiskWriter};
use crowdb_paxos_kv_client::{GetOutcome, PaxosKvClient, ReadMode, StreamProcessAuthority};
use crowdb_tree_ffi::CtError;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Instant};
mod allocation;
mod proof;
pub(crate) mod record;
use record::{Namespace, Phase, NAMESPACE_KEY, ROOT_PREFIX};

/// A durable reason why a known legacy receipt is still physically retained.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LegacyRetirementPending {
    LegacyRoot {
        tree: u64,
    },
    MissingOwnerProof {
        tree: u64,
        generation: u64,
    },
    LiveProcess {
        tree: u64,
        generation: u64,
        instance: u64,
        epoch: u64,
    },
    GenerationPin {
        tree: u64,
        generation: u64,
    },
    MissingAllocation {
        high: u64,
        low: u64,
    },
    AllocationChanged {
        high: u64,
        low: u64,
    },
}
/// Work in this invocation; `complete` covers persisted, known manifest receipts only.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LegacyRetirementProgress {
    pub examined: u32,
    pub reclaimed_chunks: u32,
    pub reclaimed_bytes: u64,
    pub complete: bool,
    pub pending: Option<LegacyRetirementPending>,
}

impl ChunkKvStorage {
    /// Permanently stop legacy-format publication in this metadata namespace and
    /// retire known receipts only after all registered legacy readers are excluded.
    /// No writer-quiesced flag substitutes for authoritative process exclusion.
    /// Historical allocations lacking a retained manifest are outside this census.
    ///
    /// # Errors
    /// Returns fencing, metadata, or physical-IO errors. The durable cursor and
    /// allocation claim survive unknown replies; the next call resumes them.
    pub async fn collect_legacy_storage(
        &self,
        metadata_group_id: u64,
        limit: u32,
    ) -> Result<LegacyRetirementProgress, CtError> {
        self.legacy_collector(metadata_group_id)?.step(limit).await
    }

    pub(crate) async fn resume_legacy_storage(
        &self,
        metadata_group_id: u64,
        limit: u32,
    ) -> Result<Option<LegacyRetirementProgress>, CtError> {
        self.legacy_collector(metadata_group_id)?.resume(limit).await
    }

    fn legacy_collector(&self, metadata_group_id: u64) -> Result<Collector, CtError> {
        let process = self.owner_process.load_full().ok_or(CtError::NotSupported)?;
        let (allocator, disks) = self.chunk_io.storage_parts();
        Ok(Collector {
            kv: self.kv.clone(),
            store: self.metadata_store_id,
            group: metadata_group_id,
            process,
            allocator,
            disks,
        })
    }
}

pub(super) struct Collector {
    pub kv: Arc<PaxosKvClient>,
    pub store: u64,
    pub group: u64,
    pub process: Arc<StreamProcessAuthority>,
    pub allocator: Arc<dyn ChunkAllocator>,
    pub disks: Arc<dyn DiskWriter>,
}
impl Collector {
    async fn resume(&self, limit: u32) -> Result<Option<LegacyRetirementProgress>, CtError> {
        if self.get(NAMESPACE_KEY).await?.is_none() {
            return Ok(None);
        }
        self.step(limit).await.map(Some)
    }

    async fn get(&self, key: &[u8]) -> Result<Option<(bytes::Bytes, u64)>, CtError> {
        match self
            .kv
            .get(self.store, self.group, key, ReadMode::Linearizable, None)
            .await
            .map_err(|_| CtError::Unavailable)?
        {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, revision } => Ok(Some((value, revision))),
        }
    }
    async fn put(&self, key: &[u8], bytes: &[u8], revision: u64) -> Result<(), CtError> {
        self.kv
            .put_cas(self.store, self.group, key, bytes, revision)
            .await
            .map(|_| ())
            .map_err(|_| CtError::Unavailable)
    }
    pub(super) async fn step(&self, limit: u32) -> Result<LegacyRetirementProgress, CtError> {
        if self.group == 0 || limit == 0 || limit > 64 {
            return Err(CtError::InvalidArgument);
        }
        self.process
            .capture(Instant::now())
            .map_err(|_| CtError::Unavailable)?;
        let (mut state, revision) = if let Some((bytes, revision)) = self.get(NAMESPACE_KEY).await? {
            (record::decode::<Namespace>(&bytes)?, revision)
        } else {
            self.put(NAMESPACE_KEY, &record::encode(&Namespace::default())?, 0)
                .await?;
            let (bytes, revision) = self.get(NAMESPACE_KEY).await?.ok_or(CtError::Unavailable)?;
            (record::decode::<Namespace>(&bytes)?, revision)
        };
        if state.version != 1 {
            return Err(CtError::Corruption);
        }
        state.pending = None;
        let mut progress = LegacyRetirementProgress::default();
        while progress.examined < limit && state.phase != Phase::Complete {
            let page = self
                .kv
                .scan(
                    self.store,
                    self.group,
                    ROOT_PREFIX,
                    &state.after,
                    &[],
                    1,
                    ReadMode::Linearizable,
                    None,
                    true,
                    None,
                )
                .await
                .map_err(|_| CtError::Unavailable)?;
            if page.timed_out {
                return Err(CtError::Unavailable);
            }
            let Some((key, _)) = page.items.into_iter().next() else {
                state.phase = if state.phase == Phase::Proving {
                    Phase::Retiring
                } else {
                    Phase::Complete
                };
                state.after.clear();
                state.pack_index = 0;
                continue;
            };
            progress.examined += 1;
            let finished = if state.phase == Phase::Proving {
                self.prove_key(&key, &mut state).await?
            } else {
                self.retire_key(&key, &mut state, &mut progress).await?
            };
            if state.pending.is_some() {
                break;
            }
            if finished {
                state.after = key.to_vec();
                state.pack_index = 0;
                state.proof_after.clear();
                state.pins_done = false;
                state.owners_done = false;
            }
        }
        self.process
            .capture(Instant::now())
            .map_err(|_| CtError::Unavailable)?;
        self.put(NAMESPACE_KEY, &record::encode(&state)?, revision)
            .await?;
        progress.pending = state.pending;
        progress.complete = state.phase == Phase::Complete;
        Ok(progress)
    }
}

/// Run the production collector against protocol fixture seams.
/// # Errors
/// Returns the same proof, metadata and physical IO failures as the host entry.
#[cfg(feature = "test-util")]
pub async fn collect_legacy_storage_for_tests(
    kv: Arc<PaxosKvClient>,
    process: Arc<StreamProcessAuthority>,
    allocator: Arc<dyn ChunkAllocator>,
    disks: Arc<dyn DiskWriter>,
    limit: u32,
) -> Result<LegacyRetirementProgress, CtError> {
    Collector {
        kv,
        store: 0,
        group: 1,
        process,
        allocator,
        disks,
    }
    .step(limit)
    .await
}

/// Run the host's resume-only path without constructing native RPC routes.
/// # Errors
/// Returns the production resume result.
#[cfg(feature = "test-util")]
pub async fn resume_legacy_storage_for_tests(
    kv: Arc<PaxosKvClient>,
    process: Arc<StreamProcessAuthority>,
    allocator: Arc<dyn ChunkAllocator>,
    disks: Arc<dyn DiskWriter>,
    limit: u32,
) -> Result<Option<LegacyRetirementProgress>, CtError> {
    Collector {
        kv,
        store: 0,
        group: 1,
        process,
        allocator,
        disks,
    }
    .resume(limit)
    .await
}
