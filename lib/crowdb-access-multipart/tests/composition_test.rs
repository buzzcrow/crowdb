// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_multipart::{ComposeError, MultipartComposer};
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;

fn location(part: u64, logical_length: u64) -> Location {
    Location {
        chunk_id: Some(ChunkId { high: 1, low: part }),
        offset: 100,
        length: logical_length + 34,
        logical_offset: 0,
        logical_length,
    }
}

#[test]
fn completion_composes_locations_and_saved_md5_in_selected_order() {
    let mut composer = MultipartComposer::new(12);
    composer.push(7, [0x11; 16], &[location(2, 7)]).unwrap();
    composer.push(5, [0x22; 16], &[location(1, 5)]).unwrap();
    let object = composer.finish().unwrap();
    assert_eq!(object.length, 12);
    assert_eq!(object.locations[0].logical_offset, 0);
    assert_eq!(object.locations[1].logical_offset, 7);
    assert_eq!(object.locations[0].chunk_id.as_ref().unwrap().low, 2);
    assert_eq!(object.locations[1].chunk_id.as_ref().unwrap().low, 1);
    assert_eq!(object.etag, "b4ab393b73e0e71830bf2bf0e63c4d91-2");
}

#[test]
fn malformed_part_does_not_advance_composition() {
    let mut composer = MultipartComposer::new(12);
    let mut invalid = location(1, 5);
    invalid.logical_offset = 1;
    assert_eq!(
        composer.push(5, [0x11; 16], &[invalid]),
        Err(ComposeError::InvalidLocations)
    );
    composer.push(5, [0x22; 16], &[location(2, 5)]).unwrap();
    let object = composer.finish().unwrap();
    assert_eq!(object.length, 5);
    assert_eq!(object.locations[0].logical_offset, 0);
    assert!(object.etag.ends_with("-1"));
}

#[test]
fn length_and_offset_overflow_are_rejected() {
    let mut composer = MultipartComposer::new(u64::MAX);
    let first = Location {
        chunk_id: Some(ChunkId { high: 1, low: 1 }),
        offset: 100,
        length: 1,
        logical_offset: 0,
        logical_length: u64::MAX - 1,
    };
    composer.push(u64::MAX - 1, [0; 16], &[first]).unwrap();
    assert_eq!(
        composer.push(2, [0; 16], &[location(2, 2)]),
        Err(ComposeError::OffsetOverflow)
    );
    assert_eq!(composer.finish().unwrap().length, u64::MAX - 1);
}

#[test]
fn empty_selection_is_rejected() {
    assert!(matches!(
        MultipartComposer::new(1).finish(),
        Err(ComposeError::Empty)
    ));
}
