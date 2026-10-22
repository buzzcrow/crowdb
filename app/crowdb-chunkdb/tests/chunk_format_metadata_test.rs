// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::storage::{decode_chunk_for_tests, encode_chunk_for_tests};
use crowdb_protocol::chunkdb::rpc::Chunk;

#[test]
fn format_metadata_envelope_rejects_unknown_and_malformed_records() {
    let chunk = Chunk::default();
    let bytes = encode_chunk_for_tests(&chunk).unwrap();
    assert_eq!(decode_chunk_for_tests(&bytes).unwrap(), chunk);
    for offset in [4, 6, 8] {
        let mut invalid = bytes.clone();
        invalid[offset] ^= 1;
        assert!(decode_chunk_for_tests(&invalid).is_err(), "byte {offset}");
    }
    for end in [4, 11, bytes.len() - 1] {
        assert!(decode_chunk_for_tests(&bytes[..end]).is_err(), "length {end}");
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_chunk_for_tests(&trailing).is_err());
    let mut trailing_body = trailing.clone();
    let length = u32::try_from(trailing_body.len() - 12).unwrap();
    trailing_body[8..12].copy_from_slice(&length.to_le_bytes());
    assert!(decode_chunk_for_tests(&trailing_body).is_err());
    assert!(decode_chunk_for_tests(&bincode::serialize(&chunk).unwrap()).is_err());
}
