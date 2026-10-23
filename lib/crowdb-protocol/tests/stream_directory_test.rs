// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/stream_directory.rs"]
mod fixture;
#[path = "stream_directory_test/record.rs"]
mod record;
#[path = "stream_directory_test/span.rs"]
mod span;

use crowdb_protocol::chunk_stream::{
    decode_stream_directory_page, decode_stream_head_record, decode_stream_manifest_record,
    encode_stream_directory_head, encode_stream_directory_page, StreamActiveAttachment,
    StreamDirectoryBranch, StreamDirectoryError, StreamDirectoryPage, StreamDirectoryPageId, StreamName,
    StreamPurpose, StreamRecordError, StreamRootIdentity, MAX_STREAM_DIRECTORY_HEIGHT,
    MAX_STREAM_DIRECTORY_LOCATIONS,
};
use crowdb_protocol::frame::{FrameLayout, MAX_FRAME_BYTES, MAX_FRAME_GROUP_PAYLOAD_BYTES};
use crowdb_protocol::key::{BinaryKey, StreamDirectoryPageKey, StreamDirectoryRootKey, StreamExtentPageKey};
use fixture::TestDirectory;
use sha2::{Digest, Sha256};

#[test]
fn large_append_has_one_location_and_checked_minimal_frame_ranges() {
    let page = TestDirectory::leaf(0, 0, 4 * 1024 * 1024);
    let (reference, bytes) = encode_stream_directory_page(&page).unwrap();
    assert_eq!(reference.extent_count, 1);
    assert_eq!(reference.level, 0);
    assert!(bytes.len() < 128);
    let decoded =
        decode_stream_directory_page(&bytes, reference, TestDirectory::NAME, StreamPurpose::Stream).unwrap();
    assert_eq!(decoded, page);
    let StreamDirectoryPage::Leaf(leaf) = decoded else {
        panic!("leaf expected")
    };
    assert_eq!(leaf.locations.len(), 1);
    let extent = leaf.locations[0];
    let capacity = MAX_FRAME_GROUP_PAYLOAD_BYTES as u64;
    assert_eq!(
        crowdb_protocol::frame::FrameLayout::StreamGroupV2
            .physical_range(extent.location, capacity - 1..capacity + 2)
            .unwrap(),
        0..2 * MAX_FRAME_BYTES as u64
    );
    assert!(extent.end_physical().unwrap() > extent.location.logical_length);
}

#[test]
fn completed_page_identity_is_independent_of_head_epoch_and_generation() {
    let (left, left_bytes) = encode_stream_directory_page(&TestDirectory::leaf(0, 0, 3)).unwrap();
    let (right, _) = encode_stream_directory_page(&TestDirectory::leaf(3, 50, 4)).unwrap();
    let branch = StreamDirectoryPage::Branch(StreamDirectoryBranch {
        stream_name: TestDirectory::NAME,
        purpose: StreamPurpose::Stream,
        level: 1,
        children: vec![left, right],
    });
    let (root, bytes) = encode_stream_directory_page(&branch).unwrap();
    assert_eq!(root.extent_count, 2);
    assert_eq!(root.end_logical, 7);
    assert_ne!(left.id, root.id);
    let mut first = TestDirectory::head(left);
    first.generation = 2;
    first.previous = Some(StreamRootIdentity {
        writer_epoch: 9,
        generation: 1,
    });
    let mut next = TestDirectory::head(root);
    next.writer_epoch = 10;
    next.owner_session = [2; 16];
    next.generation = 3;
    next.previous = Some(first.identity());
    assert_eq!(
        encode_stream_directory_head(&first).unwrap().len(),
        encode_stream_directory_head(&next).unwrap().len()
    );
    let decoded =
        decode_stream_directory_page(&bytes, root, TestDirectory::NAME, StreamPurpose::Stream).unwrap();
    let StreamDirectoryPage::Branch(decoded) = decoded else {
        panic!("branch expected")
    };
    assert_eq!(decoded.children[0], left);
    assert_eq!(
        decode_stream_directory_page(&left_bytes, left, TestDirectory::NAME, StreamPurpose::Stream).unwrap(),
        TestDirectory::leaf(0, 0, 3)
    );
}

#[test]
fn corrupted_bytes_wrong_scope_and_fence_mismatches_fail_before_data_lookup() {
    let (reference, bytes) = encode_stream_directory_page(&TestDirectory::leaf(0, 0, 3)).unwrap();
    assert!(matches!(
        decode_stream_directory_page(
            &bytes,
            reference,
            StreamName { high: 8, low: 9 },
            StreamPurpose::Stream
        ),
        Err(StreamDirectoryError::WrongPurpose)
    ));
    assert!(
        decode_stream_directory_page(&bytes, reference, TestDirectory::NAME, StreamPurpose::Wal).is_err()
    );
    let mut wrong = reference;
    wrong.end_logical += 1;
    assert!(matches!(
        decode_stream_directory_page(&bytes, wrong, TestDirectory::NAME, StreamPurpose::Stream),
        Err(StreamDirectoryError::IdentityMismatch)
    ));
    let mut damaged = bytes;
    damaged[20] ^= 1;
    assert!(matches!(
        decode_stream_directory_page(&damaged, reference, TestDirectory::NAME, StreamPurpose::Stream),
        Err(StreamDirectoryError::IdentityMismatch)
    ));
}

#[test]
fn leaf_rejects_gaps_physical_overlap_wrong_purpose_and_excess_entries() {
    let StreamDirectoryPage::Leaf(mut leaf) = TestDirectory::leaf(0, 0, 3) else {
        unreachable!()
    };
    let mut second = leaf.locations[0];
    second.logical_start = 3;
    second.location.frame_offset = 50;
    leaf.locations.push(second);
    encode_stream_directory_page(&StreamDirectoryPage::Leaf(leaf.clone())).unwrap();
    leaf.locations[1].logical_start = 4;
    assert!(encode_stream_directory_page(&StreamDirectoryPage::Leaf(leaf.clone())).is_err());
    leaf.locations[1].logical_start = 3;
    leaf.locations[1].location.frame_offset = 49;
    assert!(encode_stream_directory_page(&StreamDirectoryPage::Leaf(leaf.clone())).is_err());
    leaf.locations[1].location.frame_offset = 50;
    leaf.locations.truncate(1);
    leaf.purpose = StreamPurpose::Wal;
    assert!(encode_stream_directory_page(&StreamDirectoryPage::Leaf(leaf.clone())).is_err());
    leaf.purpose = StreamPurpose::Stream;
    leaf.locations.resize(MAX_STREAM_DIRECTORY_LOCATIONS + 1, second);
    assert!(encode_stream_directory_page(&StreamDirectoryPage::Leaf(leaf)).is_err());
}

#[test]
fn branches_reject_wrong_heights_duplicate_children_and_coverage_gaps() {
    let (left, _) = encode_stream_directory_page(&TestDirectory::leaf(0, 0, 3)).unwrap();
    let (right, _) = encode_stream_directory_page(&TestDirectory::leaf(3, 50, 4)).unwrap();
    let mut branch = StreamDirectoryBranch {
        stream_name: TestDirectory::NAME,
        purpose: StreamPurpose::Stream,
        level: 1,
        children: vec![left, right],
    };
    branch.level = 2;
    assert!(encode_stream_directory_page(&StreamDirectoryPage::Branch(branch.clone())).is_err());
    branch.level = 1;
    branch.children[1].id = left.id;
    assert!(encode_stream_directory_page(&StreamDirectoryPage::Branch(branch.clone())).is_err());
    branch.children[1] = right;
    branch.children[1].first_logical += 1;
    assert!(encode_stream_directory_page(&StreamDirectoryPage::Branch(branch.clone())).is_err());
    branch.children[1] = right;
    branch.level = MAX_STREAM_DIRECTORY_HEIGHT + 1;
    assert!(encode_stream_directory_page(&StreamDirectoryPage::Branch(branch)).is_err());
}

#[test]
fn declared_directory_counts_are_bounded_before_reserving_entries() {
    let page = TestDirectory::leaf(0, 0, 3);
    let (mut reference, _) = encode_stream_directory_page(&page).unwrap();
    let mut body = bincode::serialize(&page).unwrap();
    body[24..32].copy_from_slice(&u64::MAX.to_le_bytes());
    let mut bytes = b"CSTR\x01\x00\x03\x00".to_vec();
    bytes.extend_from_slice(&u32::try_from(body.len()).unwrap().to_le_bytes());
    bytes.extend(body);
    reference.id = StreamDirectoryPageId(Sha256::digest(&bytes).into());
    assert!(matches!(
        decode_stream_directory_page(&bytes, reference, TestDirectory::NAME, StreamPurpose::Stream),
        Err(StreamDirectoryError::Codec(_))
    ));
}

#[test]
fn indexed_heads_have_explicit_versions_and_checkpoint_bounds() {
    let (root, _) = encode_stream_directory_page(&TestDirectory::leaf(0, 0, 3)).unwrap();
    let mut head = TestDirectory::head(root);
    head.active = Some(StreamActiveAttachment {
        chunk_id: TestDirectory::CHUNK,
        logical_start: 0,
        checkpoint_physical: 50,
        checkpoint_logical: 3,
    });
    let bytes = encode_stream_directory_head(&head).unwrap();
    assert_eq!(&bytes[..8], b"CSTR\x02\x00\x01\x00");
    assert_eq!(decode_stream_head_record(&bytes).unwrap(), head.clone());
    assert!(matches!(
        decode_stream_manifest_record(&bytes),
        Err(StreamRecordError::UnsupportedVersion(2))
    ));
    head.active.as_mut().unwrap().checkpoint_physical = 49;
    assert!(encode_stream_directory_head(&head).is_err());
    head.active.as_mut().unwrap().checkpoint_physical = 50;
    head.closed = true;
    assert!(encode_stream_directory_head(&head).is_err());
    head.closed = false;
    head.owner_session = [0; 16];
    assert!(encode_stream_directory_head(&head).is_err());
    head.owner_session = [1; 16];
    head.trim_offset = 4;
    assert!(encode_stream_directory_head(&head).is_err());
    head.trim_offset = 3;
    head.root = None;
    encode_stream_directory_head(&head).unwrap();
    head.previous = Some(StreamRootIdentity {
        writer_epoch: 9,
        generation: 1,
    });
    assert!(encode_stream_directory_head(&head).is_err());
}

#[test]
fn independent_page_keys_do_not_retag_contents_with_head_generations() {
    let (reference, _) = encode_stream_directory_page(&TestDirectory::leaf(0, 0, 3)).unwrap();
    let key = StreamDirectoryPageKey {
        stream_name: TestDirectory::NAME,
        page_id: reference.id,
    };
    assert_eq!(StreamDirectoryPageKey::from_bytes(&key.to_bytes()).unwrap(), key);
    assert_eq!(key.to_bytes().len(), 51);
    assert!(key
        .to_bytes()
        .starts_with(&StreamDirectoryPageKey::stream_prefix(TestDirectory::NAME)));
    assert!(StreamExtentPageKey::from_bytes(&key.to_bytes()).is_err());
    let root = StreamDirectoryRootKey {
        stream_name: TestDirectory::NAME,
        identity: StreamRootIdentity {
            writer_epoch: 9,
            generation: 1,
        },
    };
    assert_eq!(
        StreamDirectoryRootKey::from_bytes(&root.to_bytes()).unwrap(),
        root
    );
    let next = StreamDirectoryRootKey {
        identity: StreamRootIdentity {
            generation: 2,
            ..root.identity
        },
        ..root
    };
    assert!(root.to_bytes() < next.to_bytes());
    assert_eq!(FrameLayout::StreamGroupV2.physical_length(3).unwrap(), 50);
}
