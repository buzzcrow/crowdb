// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable tree root authority and generation publication.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, GetOutcome, PaxosKvClient, ReadMode};
use crowdb_tree_ffi::chunk::stage::{StageId, StageRecord};
use crowdb_tree_ffi::{RootCatalogObject, RootCatalogStore};

mod legacy_migration;
mod legacy_tracking;
mod prepared;
mod protection;
mod retention;
mod staging;
mod transaction;

use super::StorageRuntimeError;
use crate::{ChunkKvRangeCatalogPublisher, Group0ControlStore};
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState};

pub(super) struct KvRootCatalogStore {
    kv: Arc<PaxosKvClient>,
    runtime: tokio::runtime::Handle,
    store_id: u64,
    group_id: u64,
    tree_id: u64,
    owner_epoch: u64,
    process: Option<Arc<crowdb_paxos_kv_client::StreamProcessAuthority>>,
    writer_incarnation: u64,
    writer_quiesced: AtomicBool,
    recovered: ArcSwapOption<protection::RecoveryReceipt>,
    publication_authorized: AtomicBool,
    prepared_assignment: Option<ChunkKvRangeCatalogEntry>,
    published_objects: ArcSwap<HashMap<Vec<u8>, Vec<u8>>>,
}

impl KvRootCatalogStore {
    pub(super) async fn claim(
        kv: Arc<PaxosKvClient>,
        store_id: u64,
        group_id: u64,
        tree_id: u64,
        owner_epoch: u64,
    ) -> Result<Self, StorageRuntimeError> {
        if group_id == 0 || tree_id == 0 || owner_epoch == 0 {
            return Err(StorageRuntimeError::Tree(
                "root catalog group, tree, and owner epoch must be nonzero".into(),
            ));
        }
        let key = catalog_key(tree_id, b"authority", 0);
        loop {
            let (current_epoch, generation, revision) = match kv
                .get(store_id, group_id, &key, ReadMode::Linearizable, None)
                .await
                .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?
            {
                GetOutcome::NotFound => (0, 0, 0),
                GetOutcome::Found { value, revision } => {
                    let (epoch, generation) = decode_authority(&value)
                        .ok_or_else(|| StorageRuntimeError::Tree("invalid root authority record".into()))?;
                    (epoch, generation, revision)
                }
            };
            if current_epoch > owner_epoch {
                return Err(StorageRuntimeError::Tree(format!(
                    "tree root owner epoch is stale: tree_id={tree_id}, current_epoch={current_epoch}, requested_epoch={owner_epoch}"
                )));
            }
            if current_epoch == owner_epoch {
                break;
            }
            match kv
                .put_cas(
                    store_id,
                    group_id,
                    &key,
                    &encode_authority(owner_epoch, generation),
                    revision,
                )
                .await
            {
                Ok(_) => break,
                Err(
                    crowdb_paxos_kv_client::Error::CasFailed { .. } | crowdb_paxos_kv_client::Error::CasBusy,
                ) => {}
                Err(error) => return Err(StorageRuntimeError::Tree(error.to_string())),
            }
        }
        Ok(Self {
            kv,
            runtime: tokio::runtime::Handle::current(),
            store_id,
            group_id,
            tree_id,
            owner_epoch,
            process: None,
            writer_incarnation: 0,
            writer_quiesced: AtomicBool::new(false),
            recovered: ArcSwapOption::empty(),
            publication_authorized: AtomicBool::new(true),
            prepared_assignment: None,
            published_objects: ArcSwap::from_pointee(HashMap::new()),
        })
    }

    pub(super) async fn for_assignment(
        kv: Arc<PaxosKvClient>,
        store_id: u64,
        group_id: u64,
        entry: &ChunkKvRangeCatalogEntry,
    ) -> Result<Self, StorageRuntimeError> {
        if matches!(
            entry.state,
            ChunkKvRangeCatalogPartitionState::Prepared | ChunkKvRangeCatalogPartitionState::TargetCatchingUp
        ) {
            let catalog = Self::prepare(kv, store_id, group_id, entry)?;
            if entry.state == ChunkKvRangeCatalogPartitionState::TargetCatchingUp {
                catalog.claim_published(true).await?;
            }
            Ok(catalog)
        } else {
            Self::claim(kv, store_id, group_id, entry.artifact.tree_id, entry.owner_epoch).await
        }
    }

    pub(super) fn prepare(
        kv: Arc<PaxosKvClient>,
        store_id: u64,
        group_id: u64,
        entry: &ChunkKvRangeCatalogEntry,
    ) -> Result<Self, StorageRuntimeError> {
        if group_id == 0 || entry.artifact.tree_id == 0 || entry.owner_epoch == 0 {
            return Err(StorageRuntimeError::Tree("invalid prepared root identity".into()));
        }
        Ok(Self {
            kv,
            runtime: tokio::runtime::Handle::current(),
            store_id,
            group_id,
            tree_id: entry.artifact.tree_id,
            owner_epoch: entry.owner_epoch,
            process: None,
            writer_incarnation: 0,
            writer_quiesced: AtomicBool::new(false),
            recovered: ArcSwapOption::empty(),
            publication_authorized: AtomicBool::new(false),
            prepared_assignment: Some(entry.clone()),
            published_objects: ArcSwap::from_pointee(HashMap::new()),
        })
    }

    async fn authorize_publication(&self) -> Result<(), crowdb_tree_ffi::CtError> {
        self.claim_published(false)
            .await
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?;
        self.register_writer().await?;
        self.persist_completed_recovery().await
    }

    pub(super) async fn claim_published(&self, allow_catching_up: bool) -> Result<(), StorageRuntimeError> {
        if self.publication_authorized.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(prepared) = &self.prepared_assignment else {
            return Ok(());
        };
        let publisher =
            ChunkKvRangeCatalogPublisher::new(Arc::new(Group0ControlStore::from_client(self.kv.clone())));
        let (_, pages) = publisher
            .load_current()
            .await
            .map_err(|error| StorageRuntimeError::Tree(error.to_string()))?
            .ok_or_else(|| StorageRuntimeError::Tree("range catalog is absent".into()))?;
        let assigned = pages.iter().flat_map(|page| &page.entries).any(|entry| {
            entry.partition_id == prepared.partition_id
                && entry.range == prepared.range
                && entry.owner.instance_id == prepared.owner.instance_id
                && entry.owner_epoch == prepared.owner_epoch
                && entry.artifact.tree_id == prepared.artifact.tree_id
                && entry.artifact.stream_name == prepared.artifact.stream_name
                && same_publication_dependency(entry, prepared)
                && same_recovery_base(entry, prepared)
                && (entry.state == ChunkKvRangeCatalogPartitionState::Serving
                    || (allow_catching_up
                        && entry.state == ChunkKvRangeCatalogPartitionState::TargetCatchingUp))
        });
        if !assigned {
            return Err(StorageRuntimeError::Tree(
                "prepared target is not the published tree owner".into(),
            ));
        }
        Self::claim(
            self.kv.clone(),
            self.store_id,
            self.group_id,
            self.tree_id,
            self.owner_epoch,
        )
        .await
        .map(|_| {
            if !allow_catching_up {
                self.publication_authorized.store(true, Ordering::Release);
            }
        })
    }

    fn wait<F: std::future::Future>(&self, future: F) -> F::Output {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    async fn get(&self, key: &[u8]) -> Result<Option<(Bytes, u64)>, crowdb_paxos_kv_client::Error> {
        self.kv
            .get(self.store_id, self.group_id, key, ReadMode::Linearizable, None)
            .await
            .map(|outcome| match outcome {
                GetOutcome::Found { value, revision } => Some((value, revision)),
                GetOutcome::NotFound => None,
            })
    }

    fn remember_published(&self, key: &[u8], value: &[u8]) {
        self.published_objects.rcu(|current| {
            let mut next = HashMap::clone(current);
            next.insert(key.to_vec(), value.to_vec());
            next
        });
    }
}

impl RootCatalogStore for KvRootCatalogStore {
    fn prepare_legacy_migration(
        &self,
        tree: u64,
        generation: u64,
        manifest: &[u8],
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        self.retain_legacy_migration(tree, generation, manifest)
    }
    fn load_legacy_migration(
        &self,
        tree: u64,
        generation: u64,
    ) -> Result<Option<Vec<u8>>, crowdb_tree_ffi::CtError> {
        self.load_migration_receipt(tree, generation)
    }
    fn stage_protection(&self) -> Option<&dyn crowdb_tree_ffi::chunk::protection::StageProtectionCatalog> {
        Some(self)
    }
    fn supports_staging(&self) -> bool {
        true
    }
    fn load_stage(&self, tree_id: u64, id: StageId) -> Result<Option<StageRecord>, crowdb_tree_ffi::CtError> {
        self.stage_load(tree_id, id)
    }
    fn compare_stage(
        &self,
        tree_id: u64,
        owner_epoch: u64,
        expected: Option<&StageRecord>,
        next: Option<&StageRecord>,
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        self.stage_compare(tree_id, owner_epoch, expected, next)
    }
    fn list_stages(
        &self,
        tree_id: u64,
        after: Option<StageId>,
        limit: u32,
    ) -> Result<Vec<StageRecord>, crowdb_tree_ffi::CtError> {
        self.stage_list(tree_id, after, limit)
    }
    fn load(
        &self,
        tree_id: u64,
        object: RootCatalogObject,
    ) -> Result<Option<Vec<u8>>, crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id && !matches!(object, RootCatalogObject::ReferenceSegment(_)) {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        let key = object_key(tree_id, object);
        // Exact generations can be reclaimed by another catalog instance.
        // A cached descriptor must not resurrect an absent durable root.
        if matches!(object, RootCatalogObject::ReferenceSegment(_)) {
            if let Some(value) = self.published_objects.load().get(&key) {
                return Ok(Some(value.clone()));
            }
        }
        let value = self
            .wait(self.get(&key))
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?;
        if value.is_none() && object == RootCatalogObject::CurrentManifest {
            let authority_key = catalog_key(tree_id, b"authority", 0);
            if let Some((authority, _)) = self
                .wait(self.get(&authority_key))
                .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?
            {
                let (_, generation) =
                    decode_authority(&authority).ok_or(crowdb_tree_ffi::CtError::Corruption)?;
                if generation != 0 {
                    return Err(crowdb_tree_ffi::CtError::Corruption);
                }
            }
        }
        if let Some((bytes, _)) = &value {
            if !matches!(object, RootCatalogObject::ReferenceSegment(_)) {
                self.wait(self.track_legacy_reader(bytes))?;
            }
        }
        Ok(value.map(|(bytes, _)| bytes.to_vec()))
    }

    fn store(
        &self,
        tree_id: u64,
        object: RootCatalogObject,
        data: &[u8],
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id || !matches!(object, RootCatalogObject::ReferenceSegment(_)) {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        let key = object_key(tree_id, object);
        self.wait(self.kv.put(self.store_id, self.group_id, &key, data, None))
            .map(|_| ())
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?;
        self.remember_published(&key, data);
        Ok(())
    }

    fn publish(
        &self,
        tree_id: u64,
        expected_generation: u64,
        owner_epoch: u64,
        generation: u64,
        manifest: &[u8],
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id || owner_epoch != self.owner_epoch || generation != expected_generation + 1
        {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        self.wait(self.authorize_publication())?;
        let authority_key = catalog_key(tree_id, b"authority", 0);
        let Some((authority, revision)) = self
            .wait(self.get(&authority_key))
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?
        else {
            return Err(crowdb_tree_ffi::CtError::Unavailable);
        };
        self.wait(self.check_registered_writer())?;
        if decode_authority(&authority) != Some((owner_epoch, expected_generation)) {
            return Err(crowdb_tree_ffi::CtError::Unavailable);
        }
        let mut ops = vec![
            BatchOp::Put {
                key: Bytes::from(authority_key.clone()),
                value: Bytes::copy_from_slice(&encode_authority(owner_epoch, generation)),
            },
            BatchOp::Put {
                key: Bytes::from(catalog_key(tree_id, b"current", 0)),
                value: Bytes::copy_from_slice(manifest),
            },
            BatchOp::Put {
                key: Bytes::from(catalog_key(tree_id, b"manifest", generation)),
                value: Bytes::copy_from_slice(manifest),
            },
        ];
        ops.extend(self.wait(self.legacy_publication_ops(generation, manifest))?);
        self.wait(
            self.kv
                .batch_write_cas(self.store_id, self.group_id, &ops, &authority_key, revision),
        )
        .map(|_| ())
        .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?;
        self.remember_published(&catalog_key(tree_id, b"current", 0), manifest);
        self.remember_published(&catalog_key(tree_id, b"manifest", generation), manifest);
        Ok(())
    }

    fn allocate_reference_segment_id(&self, tree_id: u64) -> Result<u64, crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        let key = catalog_key(tree_id, b"next-reference", 0);
        loop {
            let (current, revision) = match self
                .wait(self.get(&key))
                .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?
            {
                Some((value, revision)) => (
                    decode_u64(&value).ok_or(crowdb_tree_ffi::CtError::Corruption)?,
                    revision,
                ),
                None => (1, 0),
            };
            let next = current
                .checked_add(1)
                .ok_or(crowdb_tree_ffi::CtError::ResourceExhausted)?;
            match self.wait(self.kv.put_cas(
                self.store_id,
                self.group_id,
                &key,
                &next.to_be_bytes(),
                revision,
            )) {
                Ok(_) => return Ok(current),
                Err(
                    crowdb_paxos_kv_client::Error::CasFailed { .. } | crowdb_paxos_kv_client::Error::CasBusy,
                ) => {}
                Err(_) => return Err(crowdb_tree_ffi::CtError::Unavailable),
            }
        }
    }

    fn discard_reference_segments(&self, tree_id: u64, object_ids: &[u64]) -> u64 {
        if tree_id != self.tree_id || object_ids.is_empty() {
            return 0;
        }
        let ops = object_ids
            .iter()
            .map(|object_id| BatchOp::Delete {
                key: Bytes::from(catalog_key(tree_id, b"reference", *object_id)),
            })
            .collect::<Vec<_>>();
        self.wait(self.kv.batch_write(self.store_id, self.group_id, &ops))
            .map_or(0, |_| object_ids.len() as u64)
    }

    fn reclaim_before(&self, tree_id: u64, generation: u64) -> u64 {
        self.reclaim_generations(tree_id, generation)
    }
    fn pin_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
        generation: u64,
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        self.retain_generation(tree_id, transition_high, transition_low, generation)
    }
    fn unpin_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        self.release_generation(tree_id, transition_high, transition_low)
    }
}

fn catalog_key(tree_id: u64, kind: &[u8], object_id: u64) -> Vec<u8> {
    let mut key = b"\0crowdb/chunk-kv/root/v1/".to_vec();
    key.extend_from_slice(&tree_id.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(kind);
    key.push(b'/');
    key.extend_from_slice(&object_id.to_be_bytes());
    key
}

fn catalog_pin_prefix(tree_id: u64) -> Vec<u8> {
    let mut key = b"\0crowdb/chunk-kv/root/v1/".to_vec();
    key.extend_from_slice(&tree_id.to_be_bytes());
    key.extend_from_slice(b"/pin/");
    key
}

fn catalog_pin_key(tree_id: u64, transition_high: u64, transition_low: u64) -> Vec<u8> {
    let mut key = catalog_pin_prefix(tree_id);
    key.extend_from_slice(&transition_high.to_be_bytes());
    key.extend_from_slice(&transition_low.to_be_bytes());
    key
}

fn object_key(tree_id: u64, object: RootCatalogObject) -> Vec<u8> {
    match object {
        RootCatalogObject::CurrentManifest => catalog_key(tree_id, b"current", 0),
        RootCatalogObject::Manifest(generation) => catalog_key(tree_id, b"manifest", generation),
        RootCatalogObject::ReferenceSegment(object_id) => catalog_key(tree_id, b"reference", object_id),
    }
}

fn encode_authority(owner_epoch: u64, generation: u64) -> [u8; 16] {
    let mut value = [0; 16];
    value[..8].copy_from_slice(&owner_epoch.to_be_bytes());
    value[8..].copy_from_slice(&generation.to_be_bytes());
    value
}

fn decode_authority(value: &[u8]) -> Option<(u64, u64)> {
    (value.len() == 16).then(|| (decode_u64(&value[..8]).unwrap(), decode_u64(&value[8..]).unwrap()))
}

fn decode_u64(value: &[u8]) -> Option<u64> {
    value.try_into().ok().map(u64::from_be_bytes)
}

fn same_publication_dependency(
    entry: &ChunkKvRangeCatalogEntry,
    prepared: &ChunkKvRangeCatalogEntry,
) -> bool {
    entry.transition_id == prepared.transition_id
        || (entry.state == ChunkKvRangeCatalogPartitionState::Serving
            && entry.transition_id.is_none()
            && entry.artifact.tail_overlay.is_none()
            && prepared.artifact.tail_overlay.is_none())
}

fn same_recovery_base(entry: &ChunkKvRangeCatalogEntry, prepared: &ChunkKvRangeCatalogEntry) -> bool {
    match (&entry.artifact.tail_overlay, &prepared.artifact.tail_overlay) {
        (None, None) => true,
        (Some(current), Some(base)) => {
            current.source_partition_id == base.source_partition_id
                && current.source_epoch == base.source_epoch
                && current.source_stream_name == base.source_stream_name
                && current.source_stream_generation == base.source_stream_generation
                && current.replay_offset == base.replay_offset
                && current.base_root_manifest_generation == base.base_root_manifest_generation
                && current.base_tree_manifest == base.base_tree_manifest
                && current.base_applied_seq == base.base_applied_seq
                && current.cutover_seq >= base.cutover_seq
                && current.cutover_offset >= base.cutover_offset
        }
        _ => false,
    }
}
