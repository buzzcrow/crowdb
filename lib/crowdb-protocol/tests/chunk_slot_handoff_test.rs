// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_slot::{
    ChunkServiceHandoff, ChunkServiceHandoffPhase, ChunkServiceIncarnation, ChunkSlot, ChunkSlotAuthority,
    ChunkSlotFenceReceipt,
};
use crowdb_protocol::key::{ChunkServiceHandoffKey, TextKey};
#[path = "common/service_handoff.rs"]
mod service_handoff;
use service_handoff::TestServiceHandoff;

#[test]
fn incomplete_fences_cannot_publish_and_replay_cannot_roll_back() {
    let mut plan = ChunkServiceHandoff::prepare(5, 1, TestServiceHandoff::transfers()).unwrap();
    let initial = plan.clone();
    assert!(plan.record_activation().is_err());
    assert!(plan
        .record_fence(ChunkSlotFenceReceipt {
            slot: ChunkSlot::try_from(0).unwrap(),
            revision: 3
        })
        .is_err());
    plan.begin_fencing().unwrap();
    for (index, slot) in [0, 1023].into_iter().enumerate() {
        assert!(plan.record_publication(6).is_err());
        let receipt = ChunkSlotFenceReceipt {
            slot: ChunkSlot::try_from(slot).unwrap(),
            revision: u64::try_from(index).unwrap() + 3,
        };
        plan.record_fence(receipt).unwrap();
        plan.record_fence(receipt).unwrap();
        assert!(plan
            .record_fence(ChunkSlotFenceReceipt {
                revision: receipt.revision + 1,
                ..receipt
            })
            .is_err());
        let recovered: ChunkServiceHandoff =
            serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
        assert_eq!(recovered, plan);
    }
    assert!(plan.record_publication(7).is_err());
    plan.record_publication(6).unwrap();
    let published = plan.clone();
    assert!(plan.begin_fencing().is_err());
    plan.record_activation().unwrap();
    plan.record_activation().unwrap();
    plan.record_publication(6).unwrap();
    assert_eq!(plan.record().phase, ChunkServiceHandoffPhase::Activate);
    assert!(plan.can_follow(&initial));
    assert!(plan.can_follow(&published));
    assert!(!published.can_follow(&plan));
}

#[test]
fn corrupt_cohorts_and_forged_incomplete_publication_are_rejected() {
    let valid = ChunkServiceHandoff::prepare(5, 1, TestServiceHandoff::transfers()).unwrap();
    for phase in [
        ChunkServiceHandoffPhase::Publish,
        ChunkServiceHandoffPhase::Activate,
    ] {
        let mut record = valid.record().clone();
        record.phase = phase;
        assert!(
            serde_json::from_slice::<ChunkServiceHandoff>(&serde_json::to_vec(&record).unwrap()).is_err()
        );
    }
    for generation in [0, u64::MAX] {
        assert!(ChunkServiceHandoff::prepare(generation, 1, TestServiceHandoff::transfers()).is_err());
    }
    assert!(ChunkServiceHandoff::prepare(5, 0, TestServiceHandoff::transfers()).is_err());
    assert!(ChunkServiceHandoff::prepare(5, 1, vec![]).is_err());
    let mut duplicate = TestServiceHandoff::transfers();
    duplicate.push(duplicate[0]);
    assert!(ChunkServiceHandoff::prepare(5, 1, duplicate).is_err());
    let mut invalid = TestServiceHandoff::transfers();
    invalid[0].storage.group_id = 0;
    assert!(ChunkServiceHandoff::prepare(5, 1, invalid).is_err());
    let mut invalid = TestServiceHandoff::transfers();
    invalid[0].target = invalid[0].previous.unwrap();
    assert!(ChunkServiceHandoff::prepare(5, 1, invalid).is_err());
    let mut record = valid.record().clone();
    record.phase = ChunkServiceHandoffPhase::Fence;
    record.fences = vec![ChunkSlotFenceReceipt {
        slot: ChunkSlot::try_from(1).unwrap(),
        revision: 1,
    }];
    assert!(ChunkServiceHandoff::try_from(record).is_err());
}

#[test]
fn bootstrap_and_record_keys_have_explicit_contracts() {
    let mut entries = TestServiceHandoff::transfers();
    entries[0].previous = None;
    assert!(ChunkServiceHandoff::prepare(5, 1, entries.clone()).is_err());
    entries[0].target =
        ChunkSlotAuthority::new(2, ChunkServiceIncarnation::try_from([2; 16]).unwrap(), 1).unwrap();
    let plan = ChunkServiceHandoff::prepare(5, 1, entries).unwrap();
    assert_eq!(plan.publication_generation(), 6);
    let key = ChunkServiceHandoffKey;
    assert_eq!(key.to_path(), "/chunkdb/slot_handoff/service");
    assert_eq!(ChunkServiceHandoffKey::from_path(&key.to_path()).unwrap(), key);
    for path in [
        "/chunkdb/slot_handoff/",
        "/chunkdb/slot_handoff/storage",
        "/chunkdb/slot_handoff/service/1",
    ] {
        assert!(ChunkServiceHandoffKey::from_path(path).is_err());
    }
}
