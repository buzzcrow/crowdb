// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_stream::{
    resolve_extent, validate_manifest, ActiveChunkDescriptor, StreamExtentPage, StreamExtentPageFence,
    StreamManifest, StreamName,
};
use crowdb_protocol::common::ChunkId;

fn name() -> StreamName {
    StreamName { high: 1, low: 2 }
}

fn extent_page() -> StreamExtentPage {
    StreamExtentPage {
        stream_name: name(),
        writer_epoch: 7,
        generation: 3,
        page_index: 0,
        chunk_ids: vec![ChunkId { high: 0, low: 10 }, ChunkId { high: 0, low: 11 }],
        logical_offsets: vec![0, 10, 25],
        physical_offsets: vec![4, 8],
    }
}

#[test]
fn resolves_first_interior_and_boundary_offsets() {
    let page = extent_page();
    let first = resolve_extent(&page, 0).unwrap();
    assert_eq!(first.chunk_id.low, 10);
    assert_eq!(first.physical_offset, 4);
    assert_eq!(first.available, 10);

    let interior = resolve_extent(&page, 6).unwrap();
    assert_eq!(interior.physical_offset, 10);
    assert_eq!(interior.available, 4);

    let boundary = resolve_extent(&page, 10).unwrap();
    assert_eq!(boundary.chunk_id.low, 11);
    assert_eq!(boundary.physical_offset, 8);
    assert_eq!(boundary.available, 15);
}

#[test]
fn rejects_malformed_extent_geometry() {
    let mut page = extent_page();
    page.logical_offsets = vec![0, 10];
    assert!(resolve_extent(&page, 0).is_err());

    let mut page = extent_page();
    page.logical_offsets = vec![0, 10, 10];
    assert!(resolve_extent(&page, 10).is_err());

    let mut page = extent_page();
    page.physical_offsets[1] = u64::MAX;
    assert!(resolve_extent(&page, 10).is_err());
}

#[test]
fn manifest_tail_includes_only_acknowledged_active_bytes() {
    let page = extent_page();
    let manifest = StreamManifest {
        stream_name: name(),
        metadata_group_id: 7,
        writer_epoch: 7,
        generation: 3,
        trim_offset: 5,
        sealed_tail: 25,
        active: Some(ActiveChunkDescriptor {
            chunk_id: ChunkId { high: 0, low: 12 },
            physical_start: 10,
            logical_start: 25,
            acknowledged_cursor: 18,
            capacity: 100,
        }),
        extent_pages: vec![StreamExtentPageFence {
            page_index: 0,
            first_logical: 0,
            end_logical: 25,
        }],
        previous_generation: Some(2),
        closed: false,
    };
    assert_eq!(validate_manifest(&manifest, &[page]).unwrap(), 33);
}
