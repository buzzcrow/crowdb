// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_slot::{
    ChunkSlot, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotError, ChunkSlotMap, ChunkSlotMapHead,
    ChunkStorageGroup, CHUNK_SLOT_LAYOUT_VERSION,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::key::{ChunkServiceSlotsKey, ChunkSlotMapHeadKey, ChunkStorageSlotsKey, TextKey};

fn head(count: u32) -> ChunkSlotMapHead {
    ChunkSlotMapHead {
        layout_version: CHUNK_SLOT_LAYOUT_VERSION,
        generation: 7,
        owner_count: count,
    }
}

fn bindings() -> Vec<ChunkSlotBinding<u64>> {
    (1..=3)
        .map(|owner| ChunkSlotBinding {
            generation: 7,
            owner,
            slots: ChunkSlot::all()
                .filter(|s| u64::from(s.value()) % 3 + 1 == owner)
                .collect(),
        })
        .collect()
}

#[test]
fn bitmap_uses_exact_wire_size_and_lsb_bit_order() {
    let slots = [0, 7, 8, 1023].map(|s| ChunkSlot::try_from(s).unwrap());
    let bitmap: ChunkSlotBitmap = slots.into_iter().collect();
    let bytes = bitmap.as_bytes();
    assert_eq!(bytes.len(), 128);
    assert_eq!((bytes[0], bytes[1], bytes[127]), (129, 1, 128));
    assert_eq!(bitmap.slots().collect::<Vec<_>>(), slots);
    let encoded = serde_json::to_vec(&bitmap).unwrap();
    assert_eq!(
        serde_json::from_slice::<ChunkSlotBitmap>(&encoded).unwrap(),
        bitmap
    );
    for length in [0, 127, 129, 8192] {
        let value = serde_json::to_vec(&vec![0u8; length]).unwrap();
        assert!(serde_json::from_slice::<ChunkSlotBitmap>(&value).is_err());
    }
    assert!(ChunkSlot::try_from(1024).is_err());
    assert!(serde_json::from_str::<ChunkSlot>("65535").is_err());
}

#[test]
fn maps_cover_disjoint_slots_with_independent_owners_and_generations() {
    let mut service = bindings();
    service.push(ChunkSlotBinding {
        generation: 7,
        owner: 4,
        slots: ChunkSlotBitmap::default(),
    });
    let service = ChunkSlotMap::new(head(4), service).unwrap();
    assert!(service.bindings()[3].slots.is_empty());
    let storage: Vec<_> = (1..=3)
        .map(|group_id| ChunkSlotBinding {
            generation: 11,
            owner: ChunkStorageGroup {
                store_id: 0,
                group_id,
            },
            slots: ChunkSlot::all()
                .filter(|s| u64::from(s.value() / 16) % 3 + 1 == group_id)
                .collect(),
        })
        .collect();
    let storage = ChunkSlotMap::new(
        ChunkSlotMapHead {
            generation: 11,
            ..head(3)
        },
        storage,
    )
    .unwrap();
    assert_eq!(service.bindings().len() + storage.bindings().len(), 7);
    for slot in ChunkSlot::all() {
        assert_eq!(service.owner(slot), u64::from(slot.value()) % 3 + 1);
        assert_eq!(storage.owner(slot).group_id, u64::from(slot.value() / 16) % 3 + 1);
    }
}

#[test]
fn reject_incomplete_ambiguous_or_mixed_maps() {
    let mut entries = bindings();
    entries[0].slots = ChunkSlotBitmap::default();
    assert!(matches!(
        ChunkSlotMap::new(head(3), entries),
        Err(ChunkSlotError::Missing(0))
    ));
    let mut entries = bindings();
    entries[1].slots.insert(ChunkSlot::try_from(0).unwrap());
    assert!(matches!(
        ChunkSlotMap::new(head(3), entries),
        Err(ChunkSlotError::Overlap(0))
    ));
    let mut entries = bindings();
    entries[1].generation += 1;
    assert!(matches!(
        ChunkSlotMap::new(head(3), entries),
        Err(ChunkSlotError::MixedGeneration)
    ));
    let mut entries = bindings();
    entries[1].owner = entries[0].owner;
    assert!(matches!(
        ChunkSlotMap::new(head(3), entries),
        Err(ChunkSlotError::InvalidOwner)
    ));
    for invalid in [
        ChunkSlotMapHead {
            layout_version: 0,
            ..head(3)
        },
        ChunkSlotMapHead {
            generation: 0,
            ..head(3)
        },
        head(2),
    ] {
        assert!(matches!(
            ChunkSlotMap::new(invalid, bindings()),
            Err(ChunkSlotError::InvalidHead)
        ));
    }
    let entries = vec![ChunkSlotBinding {
        generation: 7,
        owner: ChunkStorageGroup {
            store_id: 1,
            group_id: 0,
        },
        slots: ChunkSlot::all().collect(),
    }];
    assert!(matches!(
        ChunkSlotMap::new(head(1), entries),
        Err(ChunkSlotError::InvalidOwner)
    ));
}

#[test]
fn hash_has_fixed_canonical_input_independent_of_maps() {
    let id = ChunkId {
        high: 0x0102_0304_0506_0708,
        low: 0x1112_1314_1516_1718,
    };
    let expected =
        xxhash_rust::xxh64::xxh64(&[1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 19, 20, 21, 22, 23, 24], 0) % 1024;
    assert_eq!(u64::from(ChunkSlot::for_chunk(&id).value()), expected);
    let mut seen = [false; 1024];
    for low in 0..32_768 {
        seen[usize::from(ChunkSlot::for_chunk(&ChunkId { high: 1 << 56, low }).value())] = true;
    }
    assert!(seen.into_iter().all(|visited| visited));
}

#[test]
fn owner_keys_and_heads_use_separate_namespaces() {
    let service = ChunkServiceSlotsKey { instance_id: 42 };
    let storage = ChunkStorageSlotsKey {
        store_id: 0,
        group_id: 3,
    };
    assert_eq!(service.to_path(), "/chunkdb/slot_service/42");
    assert_eq!(storage.to_path(), "/chunkdb/slot_storage/0/3");
    assert_eq!(
        ChunkServiceSlotsKey::from_path(&service.to_path()).unwrap(),
        service
    );
    assert_eq!(
        ChunkStorageSlotsKey::from_path(&storage.to_path()).unwrap(),
        storage
    );
    for key in [ChunkSlotMapHeadKey::Service, ChunkSlotMapHeadKey::Storage] {
        assert_eq!(ChunkSlotMapHeadKey::from_path(&key.to_path()).unwrap(), key);
        assert!(!key.to_path().starts_with(&ChunkServiceSlotsKey::prefix_all()));
        assert!(!key.to_path().starts_with(&ChunkStorageSlotsKey::prefix_all()));
    }
    for path in [
        "/chunkdb/slot_service/",
        "/chunkdb/slot_service/1/2",
        "/chunkdb/range_bind/1",
    ] {
        assert!(ChunkServiceSlotsKey::from_path(path).is_err());
    }
}
