// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Complete routing and execution authority published by one cohort transaction.

use std::collections::HashMap;

mod epochs;

use crowdb_protocol::chunk_slot::{
    ChunkSlot, ChunkSlotAuthority, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotMap, ChunkSlotMapHead,
};
use crowdb_protocol::key::{ChunkServiceAuthorityKey, ChunkSlotMapHeadKey, TextKey};

use super::{invalid, put, ChunkSlotMapClient, MapOwner};
use crate::{BatchOp, Result};

/// Immutable complete generation. A route is usable only with its paired authority.
#[derive(Clone, Debug)]
pub struct ChunkServiceSnapshot {
    service: ChunkSlotMap<u64>,
    authority: ChunkSlotMap<ChunkSlotAuthority>,
}

impl ChunkServiceSnapshot {
    #[must_use]
    pub fn service(&self) -> &ChunkSlotMap<u64> {
        &self.service
    }

    #[must_use]
    pub fn authority(&self) -> &ChunkSlotMap<ChunkSlotAuthority> {
        &self.authority
    }

    fn validate(service: ChunkSlotMap<u64>, authority: ChunkSlotMap<ChunkSlotAuthority>) -> Result<Self> {
        if service.head().generation != authority.head().generation
            || ChunkSlot::all().any(|slot| service.owner(slot) != authority.owner(slot).instance_id())
        {
            return Err(invalid("service snapshot", "routing and authority disagree"));
        }
        Ok(Self { service, authority })
    }
}

impl MapOwner for ChunkSlotAuthority {
    fn head() -> ChunkSlotMapHeadKey {
        ChunkSlotMapHeadKey::Authority
    }

    fn prefix() -> String {
        ChunkServiceAuthorityKey::prefix_all()
    }

    fn key(self) -> String {
        ChunkServiceAuthorityKey { authority: self }.to_path()
    }
}

impl ChunkSlotMapClient {
    /// Read dynamic authority when explicitly initialized; fixed maps have no epoch head.
    ///
    /// # Errors
    /// Rejects corrupt or partial dynamic snapshots without falling back to fixed routing.
    pub async fn read_dynamic_service_snapshot(&self) -> Result<Option<ChunkServiceSnapshot>> {
        if self.read_head::<ChunkSlotAuthority>().await?.is_some() {
            self.read_service_snapshot().await.map(Some)
        } else if self.scan_prefix(&ChunkSlotAuthority::prefix()).await?.is_empty() {
            Ok(None)
        } else {
            Err(invalid(
                "service snapshot",
                "authority bindings have no publication head",
            ))
        }
    }
    /// Read both complete maps under an unchanged service-head revision.
    ///
    /// # Errors
    /// Rejects missing authority, mixed generations, invalid coverage or concurrent publication.
    pub async fn read_service_snapshot(&self) -> Result<ChunkServiceSnapshot> {
        for _ in 0..4 {
            match self.read_service_snapshot_once().await {
                Err(crate::Error::CasFailed { .. }) => {}
                result => return result,
            }
        }
        Err(crate::Error::CasBusy)
    }

    async fn read_service_snapshot_once(&self) -> Result<ChunkServiceSnapshot> {
        let before = self.read_head::<u64>().await?;
        let service = self.read_service().await?;
        let authority = self.read::<ChunkSlotAuthority>().await?;
        let after = self.read_head::<u64>().await?;
        if before != after || before.as_ref().map(|(head, _)| head) != Some(service.head()) {
            return Err(crate::Error::CasFailed {
                current_revision: after.map_or(0, |(_, revision)| revision),
            });
        }
        ChunkServiceSnapshot::validate(service, authority)
    }
}

pub(super) fn assemble<O: MapOwner>(
    generation: u64,
    entries: HashMap<O, ChunkSlotBitmap>,
) -> Result<ChunkSlotMap<O>> {
    let mut bindings: Vec<_> = entries
        .into_iter()
        .map(|(owner, slots)| ChunkSlotBinding {
            generation,
            owner,
            slots,
        })
        .collect();
    bindings.sort_by_key(|binding| binding.owner.key());
    let head = ChunkSlotMapHead {
        layout_version: crowdb_protocol::chunk_slot::CHUNK_SLOT_LAYOUT_VERSION,
        generation,
        owner_count: u32::try_from(bindings.len()).map_err(|_| invalid("handoff", "too many owners"))?,
    };
    ChunkSlotMap::new(head, bindings).map_err(|error| invalid("handoff", &error.to_string()))
}

pub(super) fn append_map<O: MapOwner>(ops: &mut Vec<BatchOp>, map: &ChunkSlotMap<O>) -> Result<()> {
    for binding in map.bindings() {
        ops.push(put(binding.owner.key(), binding)?);
    }
    ops.push(put(O::head().to_path(), map.head())?);
    Ok(())
}

pub(super) fn replace_map<O: MapOwner>(
    ops: &mut Vec<BatchOp>,
    old: &ChunkSlotMap<O>,
    next: &ChunkSlotMap<O>,
) -> Result<()> {
    for binding in old.bindings() {
        if !next.bindings().iter().any(|entry| entry.owner == binding.owner) {
            ops.push(BatchOp::Delete {
                key: binding.owner.key().into(),
            });
        }
    }
    append_map(ops, next)
}
