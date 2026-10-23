// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/stream_directory.rs"]
mod stream_directory;

use crowdb_protocol::chunk_stream::{
    decode_stream_root_pin, encode_stream_directory_page, encode_stream_root_pin, StreamDirectoryError,
    StreamRootPin, StreamRootPinId,
};
use crowdb_protocol::key::{BinaryKey, StreamRootPinKey};
use stream_directory::TestDirectory as Test;

fn pin() -> StreamRootPin {
    let (root, _) = encode_stream_directory_page(&Test::leaf(10, 0, 20)).unwrap();
    StreamRootPin {
        id: StreamRootPinId([7; 16]),
        head: Test::head(root),
        needed_offset: 15,
        captured_tail: 25,
        expires_at_ms: None,
    }
}

#[test]
fn durable_and_leased_pins_keep_exact_bounded_root_and_view() {
    for expiry in [None, Some(60_000)] {
        let mut pin = pin();
        pin.expires_at_ms = expiry;
        let bytes = encode_stream_root_pin(&pin).unwrap();
        assert!(bytes.len() < 512);
        assert_eq!(decode_stream_root_pin(&bytes, Test::NAME, pin.id).unwrap(), pin);
        let key = StreamRootPinKey {
            stream_name: Test::NAME,
            pin_id: pin.id,
        };
        assert_eq!(key.to_bytes().len(), 35);
        assert!(key
            .to_bytes()
            .starts_with(&StreamRootPinKey::stream_prefix(Test::NAME)));
        assert_eq!(StreamRootPinKey::from_bytes(&key.to_bytes()).unwrap(), key);
    }
}

#[test]
fn pins_reject_empty_id_invalid_view_and_missing_owner_scope() {
    let original = pin();
    let mut bad = original.clone();
    bad.id = StreamRootPinId::default();
    assert!(encode_stream_root_pin(&bad).is_err());
    for (needed_offset, captured_tail) in [(9, 25), (26, 25), (15, 31)] {
        let mut bad = original.clone();
        bad.needed_offset = needed_offset;
        bad.captured_tail = captured_tail;
        assert!(encode_stream_root_pin(&bad).is_err());
    }
    let mut bad = original.clone();
    bad.expires_at_ms = Some(0);
    assert!(encode_stream_root_pin(&bad).is_err());
    let mut bad = original;
    bad.head.owner_session = [0; 16];
    assert!(encode_stream_root_pin(&bad).is_err());
}

#[test]
fn pins_reject_unknown_envelopes_trailing_bytes_and_wrong_keys() {
    let pin = pin();
    let bytes = encode_stream_root_pin(&pin).unwrap();
    for offset in [4, 6, 7, 8] {
        let mut bad = bytes.clone();
        bad[offset] ^= 1;
        assert!(decode_stream_root_pin(&bad, Test::NAME, pin.id).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_stream_root_pin(&trailing, Test::NAME, pin.id).is_err());
    assert!(decode_stream_root_pin(&vec![0; 1025], Test::NAME, pin.id).is_err());
    assert!(matches!(
        decode_stream_root_pin(&bytes, Test::NAME, StreamRootPinId([8; 16])),
        Err(StreamDirectoryError::IdentityMismatch)
    ));
    let mut other = Test::NAME;
    other.low += 1;
    assert!(decode_stream_root_pin(&bytes, other, pin.id).is_err());
    let key = StreamRootPinKey {
        stream_name: Test::NAME,
        pin_id: pin.id,
    }
    .to_bytes();
    for end in [0, 18, 34] {
        assert!(StreamRootPinKey::from_bytes(&key[..end]).is_err());
    }
    let mut bad = key;
    bad.push(0);
    assert!(StreamRootPinKey::from_bytes(&bad).is_err());
}
