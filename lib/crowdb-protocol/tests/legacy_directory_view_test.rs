// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    decode_stream_extent_page_record, decode_stream_manifest_record, encode_stream_manifest_record,
    ActiveChunkDescriptor, LegacyExtentPageView, LegacyManifestView, LegacyStreamExtentPage,
    LegacyStreamManifest, StreamExtentPageFence, StreamName, StreamRecordEncoding,
};
use crowdb_protocol::{chunkdb::rpc::ChunkType, common::ChunkId};

#[test]
fn bounded_migration_heads_fence_legacy_decoders_and_preserve_exact_source_identity() {
    use crowdb_protocol::chunk_stream::{
        decode_stream_head_record, encode_stream_migration_head, legacy_stream_source_id, StreamHeadRecord,
        StreamMigrationHead, StreamRootIdentity,
    };
    use crowdb_protocol::key::{BinaryKey, StreamLegacySourceKey};
    let legacy = LegacyStreamManifest {
        metadata_group_id: 1,
        writer_epoch: 3,
        generation: 7,
        ..LegacyStreamManifest::default()
    };
    for bytes in [
        bincode::serialize(&legacy).unwrap(),
        encode_stream_manifest_record(&legacy).unwrap(),
    ] {
        let marker = StreamMigrationHead {
            stream_name: legacy.stream_name,
            purpose: legacy.purpose,
            metadata_group_id: 1,
            writer_epoch: 4,
            owner_session: [9; 16],
            generation: 8,
            legacy: StreamRootIdentity {
                writer_epoch: 3,
                generation: 7,
            },
            source: legacy_stream_source_id(&bytes),
        };
        marker.validate_source(&bytes).unwrap();
        let encoded = encode_stream_migration_head(&marker).unwrap();
        assert_eq!(&encoded[..8], b"CSTR\x03\x00\x01\x00");
        assert!(encoded.len() < 192);
        assert_eq!(
            decode_stream_head_record(&encoded).unwrap(),
            StreamHeadRecord::Migrating(marker.clone())
        );
        assert!(decode_stream_manifest_record(&encoded).is_err());
        assert!(LegacyManifestView::decode(&encoded).is_err());
        let key = StreamLegacySourceKey {
            stream_name: marker.stream_name,
            source: marker.source,
        };
        assert_eq!(StreamLegacySourceKey::from_bytes(&key.to_bytes()).unwrap(), key);
        assert_eq!(key.to_bytes().len(), 51);
        let mut corrupt = bytes.clone();
        corrupt[0] ^= 1;
        assert!(marker.validate_source(&corrupt).is_err());
        let mut bad = marker.clone();
        bad.owner_session = [0; 16];
        assert!(encode_stream_migration_head(&bad).is_err());
        bad = marker.clone();
        bad.writer_epoch = 2;
        assert!(encode_stream_migration_head(&bad).is_err());
        bad = marker.clone();
        bad.generation += 1;
        assert!(encode_stream_migration_head(&bad).is_err());
        let mut future = encoded;
        future[4] = 4;
        assert!(decode_stream_head_record(&future).is_err());
    }
}

#[test]
fn borrowed_views_match_independently_retained_legacy_records() {
    for line in include_str!("data/stream-legacy-records.txt")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let (kind, hex) = line.split_once(' ').unwrap();
        let bytes: Vec<_> = hex
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        if kind == "manifest" {
            let retained = decode_stream_manifest_record(&bytes).unwrap().value;
            let view = LegacyManifestView::decode(&bytes).unwrap();
            assert_eq!(view.stream_name, retained.stream_name);
            assert_eq!(view.purpose, retained.purpose);
            assert_eq!(view.metadata_group_id, retained.metadata_group_id);
            assert_eq!(view.writer_epoch, retained.writer_epoch);
            assert_eq!(view.generation, retained.generation);
            assert_eq!(view.trim_offset, retained.trim_offset);
            assert_eq!(view.sealed_tail, retained.sealed_tail);
            assert_eq!(view.active, retained.active);
            assert_eq!(view.previous_generation, retained.previous_generation);
            assert_eq!(view.closed, retained.closed);
            assert_eq!(view.fence_count(), retained.extent_pages.len());
            for (index, fence) in retained.extent_pages.iter().enumerate() {
                assert_eq!(&view.fence(index).unwrap(), fence);
            }
            assert!(view.fence(usize::MAX).is_err());
            let encoded = encode_stream_manifest_record(&retained).unwrap();
            assert_eq!(
                LegacyManifestView::decode(&encoded).unwrap().generation,
                retained.generation
            );
        } else {
            let retained = decode_stream_extent_page_record(&bytes, StreamRecordEncoding::Legacy)
                .unwrap()
                .value;
            let view = LegacyExtentPageView::from_raw(&bytes).unwrap();
            assert_eq!(view.stream_name, retained.stream_name);
            assert_eq!(view.writer_epoch, retained.writer_epoch);
            assert_eq!(view.generation, retained.generation);
            assert_eq!(view.page_index, retained.page_index);
            assert_eq!(view.extent_count(), retained.chunk_ids.len());
            for index in 0..view.extent_count() {
                let location = view.location(index).unwrap();
                assert_eq!(location.location.chunk_id, retained.chunk_ids[index]);
                assert_eq!(location.logical_start, retained.logical_offsets[index]);
                assert_eq!(
                    location.end_logical().unwrap(),
                    retained.logical_offsets[index + 1]
                );
                assert_eq!(location.location.frame_offset, retained.physical_offsets[index]);
                assert_eq!(
                    location.end_physical().unwrap(),
                    retained.physical_offsets[index] + u64::from(retained.frame_lengths[index])
                );
            }
        }
    }
}

#[test]
fn legacy_head_optional_fields_and_large_fence_arrays_need_no_owned_history() {
    let manifest = LegacyStreamManifest {
        metadata_group_id: 1,
        writer_epoch: 9,
        generation: 13,
        sealed_tail: 100_000,
        previous_generation: Some(12),
        closed: false,
        active: Some(ActiveChunkDescriptor {
            chunk_id: ChunkId {
                high: (ChunkType::Stream as u64) << 56,
                low: 73,
            },
            physical_start: 47,
            logical_start: 100_000,
            acknowledged_cursor: 47,
            capacity: 1024 * 1024,
        }),
        extent_pages: (0..100_000)
            .map(|index| StreamExtentPageFence {
                page_index: index + 1000,
                first_logical: index,
                end_logical: index + 1,
            })
            .collect(),
        ..LegacyStreamManifest::default()
    };
    let bytes = bincode::serialize(&manifest).unwrap();
    let view = LegacyManifestView::decode(&bytes).unwrap();
    assert_eq!(view.fence_count(), 100_000);
    assert_eq!(view.active, manifest.active);
    assert_eq!(view.previous_generation, Some(12));
    assert_eq!(view.fence(99_999).unwrap(), manifest.extent_pages[99_999]);
    assert!(std::mem::size_of::<LegacyManifestView<'_>>() <= 192);
    let mut invalid = bytes.clone();
    invalid.push(0);
    assert!(LegacyManifestView::decode(&invalid).is_err());
    let mut envelope = encode_stream_manifest_record(&manifest).unwrap();
    envelope[4] = 2;
    assert!(LegacyManifestView::decode(&envelope).is_err());
}

#[test]
fn legacy_column_lengths_flags_and_single_frame_geometry_fail_closed() {
    let name = StreamName {
        high: u64::from_le_bytes(*b"CSTR\x02\x00\x02\x00"),
        low: 41,
    };
    let page = LegacyStreamExtentPage {
        stream_name: name,
        writer_epoch: 2,
        generation: 3,
        page_index: 71,
        chunk_ids: vec![ChunkId {
            high: (ChunkType::Stream as u64) << 56,
            low: 73,
        }],
        logical_offsets: vec![0, 3],
        physical_offsets: vec![4096],
        frame_lengths: vec![37],
    };
    let raw = bincode::serialize(&page).unwrap();
    assert_eq!(LegacyExtentPageView::from_raw(&raw).unwrap().stream_name, name);
    for count_offset in [40, 64, 88, 104] {
        let mut invalid = raw.clone();
        invalid[count_offset..count_offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(LegacyExtentPageView::from_raw(&invalid).is_err());
    }
    for end in 0..raw.len() {
        assert!(LegacyExtentPageView::from_raw(&raw[..end]).is_err());
    }
    let mut extra = raw;
    extra.push(0);
    assert!(LegacyExtentPageView::from_raw(&extra).is_err());
    let mut bad = page.clone();
    bad.logical_offsets[1] = 0;
    let bytes = bincode::serialize(&bad).unwrap();
    assert!(LegacyExtentPageView::from_raw(&bytes)
        .unwrap()
        .location(0)
        .is_err());
    bad = page;
    bad.frame_lengths[0] += 1;
    let bytes = bincode::serialize(&bad).unwrap();
    let view = LegacyExtentPageView::from_raw(&bytes).unwrap();
    assert!(view.location(0).is_err());
    assert!(view.location(usize::MAX).is_err());
    let mut head = bincode::serialize(&LegacyStreamManifest::default()).unwrap();
    head[60] = 2;
    assert!(LegacyManifestView::decode(&head).is_err());
}
