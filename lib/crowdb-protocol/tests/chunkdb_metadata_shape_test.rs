// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunkdb::rpc::{ChunkStrip, StripReservationGroup, StripReservationState};

#[test]
fn placement_metadata_requires_explicit_policy_and_repair_state() {
    let strip = ChunkStrip::default();
    let value = serde_json::to_value(&strip).unwrap();
    assert_eq!(
        serde_json::from_value::<ChunkStrip>(value.clone()).unwrap(),
        strip
    );
    for field in ["placement_priority", "placement_repair_required"] {
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<ChunkStrip>(missing).is_err(), "{field}");
    }
}

#[test]
fn consumed_reservation_metadata_requires_explicit_planned_progress() {
    let group = StripReservationGroup {
        strips: vec![ChunkStrip::default()],
        states: vec![StripReservationState::Consumed as i32],
        planned_cursors: vec![1024],
        planned_closed_sequences: vec![u32::MAX],
        ..Default::default()
    };
    let value = serde_json::to_value(&group).unwrap();
    assert_eq!(
        serde_json::from_value::<StripReservationGroup>(value.clone()).unwrap(),
        group
    );
    for field in ["planned_cursors", "planned_closed_sequences"] {
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<StripReservationGroup>(missing).is_err(),
            "{field}"
        );
    }
}
