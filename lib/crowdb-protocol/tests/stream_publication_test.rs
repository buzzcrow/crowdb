// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/stream_directory.rs"]
mod stream_directory;

use crowdb_protocol::chunk_stream::{
    decode_stream_publication_guard, encode_stream_directory_page, encode_stream_publication_guard,
    stream_publication_id, StreamDirectoryControl, StreamName, StreamPublicationGuard, StreamPublicationId,
    StreamPurpose, MAX_STREAM_PUBLICATION_PAGES,
};
use crowdb_protocol::key::{BinaryKey, StreamDirectoryControlKey, StreamPublicationGuardKey};
use stream_directory::TestDirectory;

#[test]
fn publication_protection_has_bounded_scope_identity_and_record_versions() {
    let (root, _) = encode_stream_directory_page(&TestDirectory::leaf(0, 0, 3)).unwrap();
    let head = TestDirectory::head(root);
    let guard = StreamPublicationGuard {
        head,
        base: None,
        pages: vec![root.id],
    };
    let id = stream_publication_id(&guard.head).unwrap();
    let bytes = encode_stream_publication_guard(&guard).unwrap();
    assert_eq!(
        decode_stream_publication_guard(&bytes, id, TestDirectory::NAME).unwrap(),
        guard
    );
    assert!(
        decode_stream_publication_guard(&bytes, StreamPublicationId([9; 32]), TestDirectory::NAME).is_err()
    );
    assert!(decode_stream_publication_guard(&bytes, id, StreamName::default()).is_err());
    for offset in [4, 6, 7, 8] {
        let mut invalid = bytes.clone();
        invalid[offset] ^= 32;
        assert!(decode_stream_publication_guard(&invalid, id, TestDirectory::NAME).is_err());
    }
    let mut invented = bytes.clone();
    let count = invented.len() - 40;
    invented[count..count + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(decode_stream_publication_guard(&invented, id, TestDirectory::NAME).is_err());
    let mut invalid = guard.clone();
    invalid.pages.clear();
    assert!(encode_stream_publication_guard(&invalid).is_err());
    invalid.pages = vec![root.id; MAX_STREAM_PUBLICATION_PAGES + 1];
    assert!(encode_stream_publication_guard(&invalid).is_err());
    invalid.pages = vec![root.id; 2];
    assert!(encode_stream_publication_guard(&invalid).is_err());
    let control = StreamDirectoryControl {
        stream_name: TestDirectory::NAME,
        purpose: StreamPurpose::Stream,
    };
    assert_eq!(
        StreamDirectoryControl::decode(&control.encode().unwrap(), control).unwrap(),
        control
    );
    let wrong = StreamDirectoryControl {
        purpose: StreamPurpose::Wal,
        ..control
    };
    assert!(StreamDirectoryControl::decode(&control.encode().unwrap(), wrong).is_err());
    let key = StreamPublicationGuardKey {
        stream_name: TestDirectory::NAME,
        publication_id: id,
    };
    assert_eq!(
        StreamPublicationGuardKey::from_bytes(&key.to_bytes()).unwrap(),
        key
    );
    let control_key = StreamDirectoryControlKey {
        stream_name: TestDirectory::NAME,
    };
    assert_eq!(
        StreamDirectoryControlKey::from_bytes(&control_key.to_bytes()).unwrap(),
        control_key
    );
    assert_ne!(&key.to_bytes()[..3], &control_key.to_bytes()[..3]);
}

#[test]
fn protection_changes_preserve_the_base_and_require_monotonic_publication() {
    let (root, _) = encode_stream_directory_page(&TestDirectory::leaf(0, 0, 3)).unwrap();
    let base = TestDirectory::head(root);
    let mut next = base.clone();
    next.generation += 1;
    next.previous = Some(base.identity());
    next.writer_epoch += 1;
    next.owner_session = [2; 16];
    let mut guard = StreamPublicationGuard {
        head: next,
        base: Some(base),
        pages: vec![],
    };
    guard.validate().unwrap();
    guard.head.generation += 1;
    assert!(guard.validate().is_err());
    guard.head.generation -= 1;
    guard.head.previous = None;
    assert!(guard.validate().is_err());
    guard.head.previous = guard
        .base
        .as_ref()
        .map(crowdb_protocol::chunk_stream::StreamDirectoryHead::identity);
    guard.base.as_mut().unwrap().purpose = StreamPurpose::Wal;
    assert!(guard.validate().is_err());
}
