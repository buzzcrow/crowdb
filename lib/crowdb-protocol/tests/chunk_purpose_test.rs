// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{StreamBinding, StreamName, StreamPurpose};
use crowdb_protocol::chunkdb::rpc::ChunkType;

#[test]
fn retired_and_unknown_chunk_types_are_not_supported() {
    for value in [0, -1, 7, i32::MAX] {
        assert!(ChunkType::try_from(value).is_err());
    }
    for value in 1..=6 {
        assert_eq!(i32::from(ChunkType::try_from(value).unwrap()), value);
    }
}

#[test]
fn stream_binding_requires_a_durable_purpose() {
    let binding = StreamBinding::creating(StreamName::generate(), None, StreamPurpose::Wal);
    let mut json = serde_json::to_value(&binding).unwrap();
    assert_eq!(
        serde_json::from_value::<StreamBinding>(json.clone()).unwrap(),
        binding
    );
    json.as_object_mut().unwrap().remove("purpose");
    assert!(serde_json::from_value::<StreamBinding>(json).is_err());
}
