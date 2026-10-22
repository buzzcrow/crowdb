// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    decode_stream_migration_guard, encode_stream_directory_page, encode_stream_migration_guard,
    legacy_stream_source_id, StreamDirectoryLeaf, StreamDirectoryPage, StreamExtentLocation,
    StreamFrameFormat, StreamMigrationGuard, StreamMigrationHead, StreamMigrationPhase,
    StreamMigrationProgress, StreamPublicationId, StreamRootIdentity,
};
use crowdb_protocol::frame::ChunkLocation;
use crowdb_protocol::key::{BinaryKey, StreamMigrationProgressKey};
use sha2::Digest;

use super::*;

#[path = "../common/migration_progress.rs"]
mod fixture;

#[test]
fn conversion_progress_preserves_the_original_root_when_a_durable_tail_is_added() {
    let source = fixture::TestMigrationProgress::source();
    let chunk_id = source.active.as_ref().unwrap().chunk_id;
    let bytes = bincode::serialize(&source).unwrap();
    let marker = StreamMigrationHead {
        stream_name: source.stream_name,
        purpose: source.purpose,
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
    let mut leaf = StreamDirectoryLeaf {
        stream_name: source.stream_name,
        purpose: source.purpose,
        locations: vec![StreamExtentLocation {
            logical_start: 0,
            location: ChunkLocation {
                chunk_id,
                frame_offset: 0,
                logical_length: 3,
            },
            format: StreamFrameFormat::LegacyV1,
        }],
    };
    let (old_root, _) = encode_stream_directory_page(&StreamDirectoryPage::Leaf(leaf.clone())).unwrap();
    let mut progress = StreamMigrationProgress {
        marker: marker.clone(),
        sequence: 1,
        root: None,
        legacy_root: None,
        logical_tail: 0,
        phase: StreamMigrationPhase::History { fence: 0, extent: 0 },
    };
    let view = LegacyManifestView::decode(&bytes).unwrap();
    progress.validate_source(&view).unwrap();
    progress.root = Some(old_root);
    progress.legacy_root = Some(old_root);
    progress.logical_tail = 3;
    progress.phase = StreamMigrationPhase::Tail {
        physical: 37,
        sealed_end: 76,
    };
    progress.validate_source(&view).unwrap();
    assert_eq!(
        StreamMigrationProgress::decode(&progress.encode().unwrap()).unwrap(),
        progress
    );
    leaf.locations.push(StreamExtentLocation {
        logical_start: 3,
        location: ChunkLocation {
            chunk_id,
            frame_offset: 37,
            logical_length: 5,
        },
        format: StreamFrameFormat::LegacyV1,
    });
    let (next_root, _) = encode_stream_directory_page(&StreamDirectoryPage::Leaf(leaf)).unwrap();
    progress.sequence += 1;
    progress.logical_tail = 8;
    progress.root = Some(next_root);
    progress.phase = StreamMigrationPhase::Complete;
    progress.validate_source(&view).unwrap();
    let mut guard = StreamMigrationGuard {
        marker,
        sequence: progress.sequence,
        base: Some(old_root),
        root: Some(next_root),
        legacy_root: Some(old_root),
        pages: vec![next_root.id],
    };
    let (id, protected) = encode_stream_migration_guard(&guard).unwrap();
    assert_eq!(
        decode_stream_migration_guard(&protected, id, source.stream_name).unwrap(),
        guard
    );
    assert!(decode_stream_migration_guard(&protected, id, StreamName { high: 71, low: 73 }).is_err());
    let mut invented = protected;
    let count = invented.len() - 40;
    invented[count..count + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    let forged_id = StreamPublicationId(sha2::Sha256::digest(&invented).into());
    assert!(decode_stream_migration_guard(&invented, forged_id, source.stream_name).is_err());
    guard.pages.clear();
    assert!(encode_stream_migration_guard(&guard).is_err());
    progress.phase = StreamMigrationPhase::Tail {
        physical: 36,
        sealed_end: 76,
    };
    assert!(progress.validate_source(&view).is_err());
    progress.phase = StreamMigrationPhase::History { fence: 2, extent: 0 };
    assert!(progress.validate_source(&view).is_err());
}

#[test]
fn conversion_progress_keys_are_scoped_to_the_exact_legacy_source() {
    let key = StreamMigrationProgressKey {
        stream_name: StreamName { high: 7, low: 9 },
        source: crowdb_protocol::chunk_stream::StreamLegacySourceId([11; 32]),
    };
    assert_eq!(
        StreamMigrationProgressKey::from_bytes(&key.to_bytes()).unwrap(),
        key
    );
    assert_eq!(key.to_bytes().len(), 51);
}
