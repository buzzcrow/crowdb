// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunkdb::rpc::SealChunkRequest;
use crowdb_protocol::chunkdb_fb::{FBInt128, FBSealChunkRequest, FBSealChunkRequestArgs};
use flatbuffers::FlatBufferBuilder;

#[test]
fn seal_requests_keep_exact_bytes_including_empty_and_partial_kib_boundaries() {
    assert_eq!(FBSealChunkRequest::VT_SEAL_BYTES, 10);
    let id = FBInt128::new(2 << 56, 7);
    for bytes in [0, 4, 1025, u64::MAX] {
        let mut builder = FlatBufferBuilder::new();
        let request = FBSealChunkRequest::create(
            &mut builder,
            &FBSealChunkRequestArgs {
                id: 41,
                rpc_create_nano: 0,
                chunk_id: Some(&id),
                seal_bytes: bytes,
            },
        );
        builder.finish(request, None);
        let request = flatbuffers::root::<FBSealChunkRequest>(builder.finished_data()).unwrap();
        assert_eq!(request.id(), 41);
        assert_eq!(request.chunk_id(), Some(&id));
        assert_eq!(request.seal_bytes(), bytes);
    }
}

#[test]
fn typed_seal_requires_an_explicit_byte_count_without_a_missing_field_default() {
    assert!(serde_json::from_value::<SealChunkRequest>(serde_json::json!({"chunk_id": null})).is_err());
    let empty = serde_json::from_value::<SealChunkRequest>(serde_json::json!({
        "chunk_id": null, "seal_bytes": 0,
    }))
    .unwrap();
    assert_eq!(empty.seal_bytes, 0);
}
