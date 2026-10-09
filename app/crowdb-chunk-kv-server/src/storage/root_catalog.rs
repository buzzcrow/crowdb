// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable tree root authority and generation publication.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use crowdb_kv_client::{BatchOp, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_tree_ffi::{RootCatalogObject, RootCatalogStore};

use super::StorageRuntimeError;
use crate::{ChunkKvRangeCatalogPublisher, Group0ControlStore};
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState};

pub(super) struct KvRootCatalogStore {
    kv: Arc<CrowdbKvClient>,
    runtime: tokio::runtime::Handle,
    store_id: u64,
    group_id: u64,
    tree_id: u64,
    owner_epoch: u64,
    publication_authorized: AtomicBool,
    prepared_assignment: Option<ChunkKvRangeCatalogEntry>,
    published_objects: ArcSwap<HashMap<Vec<u8>, Vec<u8>>>,
}

impl KvRootCatalogStore {
    pub(super) async fn claim(
        kv: Arc<CrowdbKvClient>,
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
                Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::CasBusy) => {}
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
            publication_authorized: AtomicBool::new(true),
            prepared_assignment: None,
            published_objects: ArcSwap::from_pointee(HashMap::new()),
        })
    }

    pub(super) async fn for_assignment(
        kv: Arc<CrowdbKvClient>,
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
        kv: Arc<CrowdbKvClient>,
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
            publication_authorized: AtomicBool::new(false),
            prepared_assignment: Some(entry.clone()),
            published_objects: ArcSwap::from_pointee(HashMap::new()),
        })
    }

    async fn authorize_publication(&self) -> Result<(), crowdb_tree_ffi::CtError> {
        self.claim_published(false)
            .await
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)
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
                && entry.owner == prepared.owner
                && entry.owner_epoch == prepared.owner_epoch
                && entry.artifact.tree_id == prepared.artifact.tree_id
                && entry.artifact.stream_name == prepared.artifact.stream_name
                && entry.transition_id == prepared.transition_id
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

    async fn get(&self, key: &[u8]) -> Result<Option<(Bytes, u64)>, crowdb_kv_client::Error> {
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
    fn load(
        &self,
        tree_id: u64,
        object: RootCatalogObject,
    ) -> Result<Option<Vec<u8>>, crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        let key = object_key(tree_id, object);
        if let Some(value) = self.published_objects.load().get(&key) {
            return Ok(Some(value.clone()));
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
        if decode_authority(&authority) != Some((owner_epoch, expected_generation)) {
            return Err(crowdb_tree_ffi::CtError::Unavailable);
        }
        let ops = [
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
                Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::CasBusy) => {}
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
        if tree_id != self.tree_id || generation <= 2 {
            return 0;
        }
        let pin_prefix = catalog_pin_prefix(tree_id);
        let pinned_generation = match self.wait(self.kv.scan(
            self.store_id,
            self.group_id,
            &pin_prefix,
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )) {
            Ok(outcome) => {
                let mut oldest = None;
                for (_, value) in outcome.items {
                    let Some(pin) = decode_u64(&value) else {
                        return 0;
                    };
                    oldest = Some(oldest.map_or(pin, |current: u64| current.min(pin)));
                }
                oldest
            }
            Err(_) => return 0,
        };
        let generation = pinned_generation.map_or(generation, |pin| generation.min(pin));
        if generation <= 2 {
            return 0;
        }
        let floor_key = catalog_key(tree_id, b"reclaim-floor", 0);
        let (floor, revision) = match self.wait(self.get(&floor_key)) {
            Ok(Some((value, revision))) => match decode_u64(&value) {
                Some(floor) => (floor, revision),
                None => return 0,
            },
            Ok(None) => (1, 0),
            Err(_) => return 0,
        };
        let end = generation.saturating_sub(1).min(floor.saturating_add(128));
        if floor >= end {
            return 0;
        }
        let mut ops = (floor..end)
            .map(|old_generation| BatchOp::Delete {
                key: Bytes::from(catalog_key(tree_id, b"manifest", old_generation)),
            })
            .collect::<Vec<_>>();
        ops.push(BatchOp::Put {
            key: Bytes::from(floor_key.clone()),
            value: Bytes::copy_from_slice(&end.to_be_bytes()),
        });
        self.wait(
            self.kv
                .batch_write_cas(self.store_id, self.group_id, &ops, &floor_key, revision),
        )
        .map_or(0, |_| end - floor)
    }

    fn pin_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
        generation: u64,
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id || (transition_high == 0 && transition_low == 0) || generation == 0 {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        if self
            .wait(self.get(&catalog_key(tree_id, b"manifest", generation)))
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?
            .is_none()
        {
            return Err(crowdb_tree_ffi::CtError::NotFound);
        }
        let key = catalog_pin_key(tree_id, transition_high, transition_low);
        for _ in 0..3 {
            if let Some((value, _)) = self
                .wait(self.get(&key))
                .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)?
            {
                return if decode_u64(&value) == Some(generation) {
                    Ok(())
                } else {
                    Err(crowdb_tree_ffi::CtError::InvalidArgument)
                };
            }
            match self.wait(
                self.kv
                    .put_cas(self.store_id, self.group_id, &key, &generation.to_be_bytes(), 0),
            ) {
                Ok(_) => return Ok(()),
                Err(crowdb_kv_client::Error::CasFailed { .. } | crowdb_kv_client::Error::CasBusy) => {}
                Err(_) => return Err(crowdb_tree_ffi::CtError::Unavailable),
            }
        }
        Err(crowdb_tree_ffi::CtError::Unavailable)
    }

    fn unpin_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
    ) -> Result<(), crowdb_tree_ffi::CtError> {
        if tree_id != self.tree_id || (transition_high == 0 && transition_low == 0) {
            return Err(crowdb_tree_ffi::CtError::InvalidArgument);
        }
        let key = catalog_pin_key(tree_id, transition_high, transition_low);
        self.wait(self.kv.delete(self.store_id, self.group_id, &key, None))
            .map(|_| ())
            .map_err(|_| crowdb_tree_ffi::CtError::Unavailable)
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

fn same_recovery_base(entry: &ChunkKvRangeCatalogEntry, prepared: &ChunkKvRangeCatalogEntry) -> bool {
    match (&entry.artifact.tail_overlay, &prepared.artifact.tail_overlay) {
        (None, None) => true,
        (Some(current), Some(base)) => {
            current.source_partition_id == base.source_partition_id
                && current.source_epoch == base.source_epoch
                && current.source_stream_name == base.source_stream_name
                && current.source_stream_manifest_generation == base.source_stream_manifest_generation
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
