// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{FrameLayout, StreamPurpose, TestDirectory as Test};
use crowdb_protocol::chunk_stream::{
    decode_stream_sealed_chunk_span, encode_stream_sealed_chunk_span, StreamSealedChunkSpan,
    MAX_STREAM_CHUNK_PHYSICAL_BYTES,
};
use crowdb_protocol::key::{BinaryKey, StreamRootPinKey, StreamSealedChunkSpanKey};

fn span() -> StreamSealedChunkSpan {
    StreamSealedChunkSpan {
        stream_name: Test::NAME,
        purpose: StreamPurpose::Stream,
        chunk_id: Test::CHUNK,
        logical_start: 10,
        logical_end: 30,
        physical_end: FrameLayout::StreamGroupV2.physical_length(20).unwrap(),
    }
}

#[test]
fn sealed_spans_keep_original_and_final_bounds_in_one_strict_current_record() {
    let span = span();
    let bytes = encode_stream_sealed_chunk_span(&span).unwrap();
    assert!(bytes.len() < 128);
    assert_eq!(
        decode_stream_sealed_chunk_span(&bytes, Test::NAME, Test::CHUNK).unwrap(),
        span
    );
    let key = StreamSealedChunkSpanKey {
        stream_name: Test::NAME,
        chunk_id: Test::CHUNK,
    };
    let encoded = key.to_bytes();
    assert_eq!(encoded.len(), 35);
    assert!(encoded.starts_with(&StreamSealedChunkSpanKey::stream_prefix(Test::NAME)));
    assert_eq!(StreamSealedChunkSpanKey::from_bytes(&encoded).unwrap(), key);
    assert!(StreamRootPinKey::from_bytes(&encoded).is_err());
    assert!(StreamSealedChunkSpanKey::from_bytes(&encoded[..34]).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(StreamSealedChunkSpanKey::from_bytes(&trailing).is_err());
}

#[test]
fn sealed_spans_reject_partial_impossible_or_wrong_purpose_bounds() {
    let span = span();
    for (logical_end, physical_end) in [
        (9, 0),
        (30, span.physical_end - 1),
        (10, 1),
        (30, MAX_STREAM_CHUNK_PHYSICAL_BYTES + 1),
    ] {
        assert!(encode_stream_sealed_chunk_span(&StreamSealedChunkSpan {
            logical_end,
            physical_end,
            ..span
        })
        .is_err());
    }
    assert!(encode_stream_sealed_chunk_span(&StreamSealedChunkSpan {
        purpose: StreamPurpose::Wal,
        ..span
    })
    .is_err());
    assert!(encode_stream_sealed_chunk_span(&StreamSealedChunkSpan {
        logical_end: 10,
        physical_end: 0,
        ..span
    })
    .is_ok());
    let bytes = encode_stream_sealed_chunk_span(&span).unwrap();
    for offset in [4, 6, 7, 8] {
        let mut bad = bytes.clone();
        bad[offset] ^= 1;
        assert!(decode_stream_sealed_chunk_span(&bad, Test::NAME, Test::CHUNK).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_stream_sealed_chunk_span(&trailing, Test::NAME, Test::CHUNK).is_err());
    assert!(decode_stream_sealed_chunk_span(&vec![0; 257], Test::NAME, Test::CHUNK).is_err());
    let mut wrong = Test::CHUNK;
    wrong.low += 1;
    assert!(decode_stream_sealed_chunk_span(&bytes, Test::NAME, wrong).is_err());
}
