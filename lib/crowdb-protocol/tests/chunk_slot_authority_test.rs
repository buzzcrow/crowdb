// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_slot::{ChunkServiceIncarnation, ChunkSlot, ChunkSlotAuthority};
use crowdb_protocol::key::{ChunkSlotFenceKey, TextKey};
use crowdb_protocol::owner_fence::{is_owner_fence_key, valid_owner_fence, valid_owner_fence_scope};

#[test]
fn fence_keys_are_canonical_and_cover_only_the_slot_space() {
    let authority =
        ChunkSlotAuthority::new(4, ChunkServiceIncarnation::try_from([1; 16]).unwrap(), 7).unwrap();
    for slot in ChunkSlot::all() {
        let key = ChunkSlotFenceKey { slot };
        assert_eq!(ChunkSlotFenceKey::from_path(&key.to_path()).unwrap(), key);
        assert!(valid_owner_fence(
            key.to_path().as_bytes(),
            &authority.to_fence_value()
        ));
    }
    for suffix in ["", "01", "+1", "-1", "1024", "65536", "1/2", "1/", " 1", "x"] {
        let path = format!("/chunkdb/ownership-fence/{suffix}");
        assert!(is_owner_fence_key(path.as_bytes()));
        assert!(ChunkSlotFenceKey::from_path(&path).is_err(), "{path}");
        assert!(!valid_owner_fence(path.as_bytes(), &authority.to_fence_value()));
    }
    assert!(!is_owner_fence_key(b"/chunkdb/slot_service/4"));
    assert!(!valid_owner_fence_scope(b"/chunkdb/ownership-fence/0", 0));
    assert!(valid_owner_fence_scope(b"/chunkdb/ownership-fence/0", 1));
    assert!(valid_owner_fence_scope(b"/diskdb/ownership-fence/1/1/1", 0));
    assert!(valid_owner_fence(b"/diskdb/ownership-fence/1/1/1", b"disk-owner"));
    assert!(!valid_owner_fence(b"/diskdb/ownership-fence/1/1/1", b""));
}

#[test]
fn fence_comparison_retains_incarnation_and_slot_generation() {
    let first = ChunkServiceIncarnation::try_from([1; 16]).unwrap();
    let second = ChunkServiceIncarnation::try_from([2; 16]).unwrap();
    let owner = ChunkSlotAuthority::new(4, first, 7).unwrap();
    let bytes = owner.to_fence_value();
    assert_eq!(bytes[0], 1);
    assert_eq!(&bytes[1..9], &4_u64.to_be_bytes());
    assert_eq!(&bytes[9..25], &[1; 16]);
    assert_eq!(&bytes[25..33], &7_u64.to_be_bytes());
    assert_eq!(ChunkSlotAuthority::from_fence_value(&bytes).unwrap(), owner);
    assert_ne!(
        ChunkSlotAuthority::new(4, second, 7).unwrap().to_fence_value(),
        bytes
    );
    assert_ne!(
        ChunkSlotAuthority::new(4, first, 8).unwrap().to_fence_value(),
        bytes
    );
    assert_ne!(
        ChunkSlotAuthority::new(5, first, 7).unwrap().to_fence_value(),
        bytes
    );
    assert_eq!(
        serde_json::from_slice::<ChunkSlotAuthority>(&serde_json::to_vec(&owner).unwrap()).unwrap(),
        owner
    );
}

#[test]
fn malformed_or_zero_authority_cannot_become_a_fence() {
    let incarnation = ChunkServiceIncarnation::try_from([1; 16]).unwrap();
    assert!(ChunkServiceIncarnation::try_from([0; 16]).is_err());
    assert!(ChunkSlotAuthority::new(0, incarnation, 1).is_err());
    assert!(ChunkSlotAuthority::new(1, incarnation, 0).is_err());
    let owner = ChunkSlotAuthority::new(4, incarnation, 7).unwrap();
    let bytes = owner.to_fence_value();
    for range in [0..1, 1..9, 9..25, 25..33] {
        let mut invalid = bytes;
        invalid[range].fill(0);
        assert!(ChunkSlotAuthority::from_fence_value(&invalid).is_err());
        assert!(!valid_owner_fence(b"/chunkdb/ownership-fence/0", &invalid));
    }
    let mut future = bytes;
    future[0] = 2;
    assert!(ChunkSlotAuthority::from_fence_value(&future).is_err());
    for invalid in [&bytes[..32], b"old-owner".as_slice()] {
        assert!(ChunkSlotAuthority::from_fence_value(invalid).is_err());
    }
    let mut json = serde_json::to_value(owner).unwrap();
    for field in ["instance_id", "generation"] {
        let saved = json[field].clone();
        json[field] = serde_json::json!(0);
        assert!(serde_json::from_value::<ChunkSlotAuthority>(json.clone()).is_err());
        json[field] = saved;
    }
    json["incarnation"] = serde_json::json!(vec![0_u8; 16]);
    assert!(serde_json::from_value::<ChunkSlotAuthority>(json).is_err());
}
