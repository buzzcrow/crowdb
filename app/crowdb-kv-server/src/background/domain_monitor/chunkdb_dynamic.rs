// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Service-only balancing through complete group-zero epoch publication.

use super::{DomainMonitorDriver, DomainMonitorFuture};

mod owners;
mod snapshot;
use crate::group0_control_plane::Group0ControlPlane;
use crowdb_kv::cluster::group_operations::KvGroupMutation;
use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use crowdb_protocol::chunk_slot::{
    ChunkSlot, ChunkSlotAuthority, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotMap,
};
use crowdb_protocol::key::{ChunkServiceAuthorityKey, ChunkServiceSlotsKey, ChunkSlotMapHeadKey, TextKey};
use owners::live_owners;
use snapshot::load_maps;
use std::collections::{BTreeMap, HashMap};

/// Each tick moves at most 64 slots; a one-slot count difference is already balanced.
#[derive(Default)]
pub struct ChunkdbDynamicMonitorDriver;

impl DomainMonitorDriver for ChunkdbDynamicMonitorDriver {
    fn domain(&self) -> &'static str {
        "chunkdb"
    }
    fn driver_version(&self) -> u32 {
        3
    }
    fn tick<'a>(
        &'a self,
        control: &'a Group0ControlPlane,
        descriptor: &'a DomainMonitorDescriptor,
    ) -> DomainMonitorFuture<'a> {
        Box::pin(async move {
            if descriptor.driver_version != 3
                || descriptor.failure_policy != DomainFailurePolicy::AutomaticSharedStorage
                || descriptor.balance_policy != "dynamic-service-slots-v1"
            {
                return Err("chunkdb dynamic monitor requires explicit dynamic service policy".into());
            }
            balance(control, descriptor).await
        })
    }
}

async fn balance(control: &Group0ControlPlane, descriptor: &DomainMonitorDescriptor) -> Result<(), String> {
    let (revision, service, authority) = load_maps(control).await?;
    let (mut live, eligible) = live_owners(control, descriptor, &authority).await?;
    let moves = plan_moves(&authority, &mut live, &eligible)?;
    if moves.is_empty() {
        return Ok(());
    }
    publish(control, revision, &service, &authority, &moves).await
}

fn plan_moves(
    authority: &ChunkSlotMap<ChunkSlotAuthority>,
    live: &mut BTreeMap<u64, usize>,
    eligible: &HashMap<u64, bool>,
) -> Result<Vec<(ChunkSlot, u64)>, String> {
    let mut moves = Vec::with_capacity(64);
    for slot in ChunkSlot::all() {
        if moves.len() == 64 {
            break;
        }
        let owner = authority.owner(slot).instance_id();
        let (&target, &minimum) = live
            .iter()
            .min_by_key(|(owner, count)| (**count, **owner))
            .ok_or("no live target")?;
        let should_move = live.get(&owner).map_or_else(
            || eligible.get(&owner).copied().unwrap_or(false),
            |count| *count > minimum + 1,
        );
        if owner != target && should_move {
            if let Some(count) = live.get_mut(&owner) {
                *count -= 1;
            }
            *live.get_mut(&target).ok_or("live target disappeared")? += 1;
            moves.push((slot, target));
        }
    }
    Ok(moves)
}

async fn publish(
    control: &Group0ControlPlane,
    revision: u64,
    service: &ChunkSlotMap<u64>,
    authority: &ChunkSlotMap<ChunkSlotAuthority>,
    moves: &[(ChunkSlot, u64)],
) -> Result<(), String> {
    let path = ChunkSlotMapHeadKey::Service.to_path();
    let next = authority.reassign(moves).map_err(|error| error.to_string())?;
    let mut routes: BTreeMap<_, ChunkSlotBitmap> = service
        .bindings()
        .iter()
        .map(|binding| (binding.owner, ChunkSlotBitmap::default()))
        .collect();
    for slot in ChunkSlot::all() {
        routes
            .entry(next.owner(slot).instance_id())
            .or_default()
            .insert(slot);
    }
    let mut mutations =
        Vec::with_capacity(authority.bindings().len() + next.bindings().len() + routes.len() + 2);
    for binding in authority.bindings() {
        if !next.bindings().iter().any(|entry| entry.owner == binding.owner) {
            mutations.push(KvGroupMutation::Delete {
                key: (ChunkServiceAuthorityKey {
                    authority: binding.owner,
                })
                .to_path()
                .into(),
            });
        }
    }
    for binding in next.bindings() {
        mutations.push(put(
            (ChunkServiceAuthorityKey {
                authority: binding.owner,
            })
            .to_path(),
            binding,
        )?);
    }
    let mut service_head = service.head().clone();
    service_head.generation = next.head().generation;
    service_head.owner_count = u32::try_from(routes.len()).map_err(|error| error.to_string())?;
    for (owner, slots) in routes {
        mutations.push(put(
            (ChunkServiceSlotsKey { instance_id: owner }).to_path(),
            &ChunkSlotBinding {
                generation: service_head.generation,
                owner,
                slots,
            },
        )?);
    }
    mutations.push(put(path.clone(), &service_head)?);
    mutations.push(put(ChunkSlotMapHeadKey::Authority.to_path(), next.head())?);
    control
        .compare_and_batch(&mutations, path.into(), revision)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    serde_json::from_slice(bytes).map_err(|error| error.to_string())
}
fn put<T: serde::Serialize>(key: String, value: &T) -> Result<KvGroupMutation, String> {
    Ok(KvGroupMutation::Put {
        key: key.into(),
        value: serde_json::to_vec(value)
            .map_err(|error| error.to_string())?
            .into(),
    })
}
