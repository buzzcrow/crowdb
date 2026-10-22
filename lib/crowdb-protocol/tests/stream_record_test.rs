// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    decode_stream_extent_page_record, decode_stream_manifest_record, encode_stream_manifest_record,
    StreamRecordEncoding, StreamRecordError,
};

#[test]
fn metadata_envelopes_reject_wrong_versions_kinds_flags_and_lengths() {
    let bytes =
        encode_stream_manifest_record(&crowdb_protocol::chunk_stream::StreamManifest::default()).unwrap();
    assert!(matches!(
        decode_stream_extent_page_record(&bytes, StreamRecordEncoding::Version1),
        Err(StreamRecordError::WrongKind {
            expected: 2,
            actual: 1
        })
    ));
    for length in 4..bytes.len() {
        assert!(decode_stream_manifest_record(&bytes[..length]).is_err());
    }
    let mut unknown = bytes.clone();
    unknown[4] = 2;
    assert!(matches!(
        decode_stream_manifest_record(&unknown),
        Err(StreamRecordError::UnsupportedVersion(2))
    ));
    let mut flags = bytes.clone();
    flags[7] = 1;
    assert!(decode_stream_manifest_record(&flags).is_err());
    let mut extra = bytes;
    extra.push(0);
    assert!(decode_stream_manifest_record(&extra).is_err());
}
