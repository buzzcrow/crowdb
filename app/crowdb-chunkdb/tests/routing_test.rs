// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb::routing::{route, BindingCache, BindingTable, RouteError};
use crowdb_protocol::chunk_slot::{
    ChunkSlot, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotBootstrap, ChunkSlotMap, ChunkStorageGroup,
};
use crowdb_protocol::common::ChunkId;

fn layout() -> ChunkSlotBootstrap {
    ChunkSlotBootstrap {
        service_instances: vec![11, 12, 13],
        storage_groups: (1..=3)
            .map(|group_id| ChunkStorageGroup {
                store_id: 0,
                group_id,
            })
            .collect(),
    }
}

#[test]
fn fixed_storage_map_routes_every_slot_and_owning_chunk() {
    let map = layout().storage_map().unwrap();
    let cache = BindingCache::new();
    cache.replace(BindingTable::new(map.clone())).unwrap();
    assert_eq!(cache.snapshot().bindings().len(), 3);
    for slot in ChunkSlot::all() {
        assert_eq!(
            cache.route_slot(slot).unwrap().kv_group_id,
            map.owner(slot).group_id
        );
    }
    for low in 0..4096 {
        let id = ChunkId { high: 1 << 56, low };
        let destination = route(&cache, &id).unwrap();
        assert_eq!(destination.kv_store_id, 0);
        assert_eq!(
            destination.kv_group_id,
            map.owner(ChunkSlot::for_chunk(&id)).group_id
        );
        assert_ne!(destination.kv_group_id, 0);
    }
}

#[test]
fn empty_cache_and_storage_remap_fail_closed() {
    let cache = BindingCache::new();
    let id = ChunkId { high: 1, low: 1 };
    assert!(matches!(route(&cache, &id), Err(RouteError::NoBinding)));
    let initial = layout().storage_map().unwrap();
    cache.replace(BindingTable::new(initial.clone())).unwrap();
    cache.replace(BindingTable::new(initial.clone())).unwrap();
    let mut different = layout();
    different.storage_groups.reverse();
    assert!(matches!(
        cache.replace(BindingTable::new(different.storage_map().unwrap())),
        Err(RouteError::FixedLayout)
    ));
    for slot in ChunkSlot::all() {
        assert_eq!(
            cache.route_slot(slot).unwrap().kv_group_id,
            initial.owner(slot).group_id
        );
    }
}

#[test]
fn service_guards_admit_only_owned_slots_with_bounded_quotas() {
    let map = layout().service_map().unwrap();
    let guards: Vec<_> = [11, 12, 13]
        .map(|instance_id| {
            let guard = RangeGuard::new();
            guard.install(&map, instance_id).unwrap();
            (instance_id, guard)
        })
        .into_iter()
        .collect();
    assert_eq!(
        guards
            .iter()
            .map(|(_, guard)| guard.owned_bucket_count())
            .sum::<u64>(),
        1024
    );
    assert!(
        guards
            .iter()
            .map(|(_, guard)| guard.quota_share(1001))
            .sum::<u64>()
            <= 1001
    );
    for low in 0..8192 {
        let id = ChunkId { high: 1 << 56, low };
        let owner = map.owner(ChunkSlot::for_chunk(&id));
        for (instance_id, guard) in &guards {
            assert_eq!(guard.check(&id).is_ok(), *instance_id == owner);
        }
    }
}

#[test]
fn uninitialized_and_zero_slot_owners_never_allow_all() {
    let guard = RangeGuard::new();
    let id = ChunkId { high: 1, low: 1 };
    assert!(!guard.is_ready());
    assert!(guard.check(&id).is_err());
    let initial = layout().service_map().unwrap();
    let mut bindings = initial.bindings().to_vec();
    bindings.push(ChunkSlotBinding {
        generation: 1,
        owner: 14,
        slots: ChunkSlotBitmap::default(),
    });
    let mut head = initial.head().clone();
    head.owner_count += 1;
    let map = ChunkSlotMap::new(head, bindings).unwrap();
    guard.install(&map, 14).unwrap();
    assert!(guard.is_empty());
    assert_eq!(guard.quota_share(u64::MAX), 0);
    assert!(guard.check(&id).is_err());
    assert!(guard.install(&map, 11).is_err());
    assert!(RangeGuard::new().install(&map, 99).is_err());
}
