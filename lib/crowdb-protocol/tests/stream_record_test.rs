// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    decode_stream_extent_page_record, decode_stream_manifest_record, encode_stream_extent_page_record,
    encode_stream_manifest_record, StreamRecordEncoding, StreamRecordError,
};

#[test]
fn retained_body_vectors_and_versioned_envelopes_preserve_their_fields() {
    for line in include_str!("data/stream-legacy-records.txt")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let (kind, hex) = line.split_once(' ').unwrap();
        let raw: Vec<_> = hex
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        if kind == "manifest" {
            let decoded = decode_stream_manifest_record(&raw).unwrap();
            assert_eq!(decoded.encoding, StreamRecordEncoding::Legacy);
            assert_eq!(decoded.value.stream_name.high, 7);
            assert_eq!(decoded.value.stream_name.low, 9);
            assert_eq!(decoded.value.writer_epoch, 2);
            assert_eq!(decoded.value.generation, 3);
            assert_eq!(bincode::serialize(&decoded.value).unwrap(), raw);
            let envelope = encode_stream_manifest_record(&decoded.value).unwrap();
            assert_eq!(&envelope[..8], b"CSTR\x01\x00\x01\x00");
            assert_eq!(&envelope[12..], raw);
            let decoded_again = decode_stream_manifest_record(&envelope).unwrap();
            assert_eq!(decoded_again.encoding, StreamRecordEncoding::Version1);
            assert_eq!(decoded_again.value, decoded.value);
        } else {
            let decoded = decode_stream_extent_page_record(&raw, StreamRecordEncoding::Legacy).unwrap();
            assert_eq!(decoded.encoding, StreamRecordEncoding::Legacy);
            assert_eq!(decoded.value.logical_offsets, [0, 3]);
            assert_eq!(decoded.value.physical_offsets, [4096]);
            assert_eq!(decoded.value.frame_lengths, [37]);
            assert_eq!(bincode::serialize(&decoded.value).unwrap(), raw);
            let envelope = encode_stream_extent_page_record(&decoded.value).unwrap();
            assert_eq!(&envelope[..8], b"CSTR\x01\x00\x02\x00");
            assert_eq!(&envelope[12..], raw);
            let decoded_again =
                decode_stream_extent_page_record(&envelope, StreamRecordEncoding::Version1).unwrap();
            assert_eq!(decoded_again.encoding, StreamRecordEncoding::Version1);
            assert_eq!(decoded_again.value, decoded.value);
        }
    }
}

#[test]
fn envelope_versions_kinds_flags_and_lengths_cannot_fall_back_to_legacy() {
    let bytes =
        encode_stream_manifest_record(&crowdb_protocol::chunk_stream::LegacyStreamManifest::default())
            .unwrap();
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

#[test]
fn legacy_and_enveloped_bodies_reject_trailing_bytes_and_invented_allocations() {
    let mut legacy = bincode::serialize(&crowdb_protocol::chunk_stream::StreamManifest::default()).unwrap();
    legacy.push(0);
    assert!(decode_stream_manifest_record(&legacy).is_err());
    let mut envelope =
        encode_stream_manifest_record(&crowdb_protocol::chunk_stream::LegacyStreamManifest::default())
            .unwrap();
    envelope.push(0);
    let length = u32::try_from(envelope.len() - 12).unwrap();
    envelope[8..12].copy_from_slice(&length.to_le_bytes());
    assert!(decode_stream_manifest_record(&envelope).is_err());
    // Raw extent-page header ends with the chunk-ID array length. The byte
    // budget must reject this invented length without consuming its elements.
    let mut page = bincode::serialize(&crowdb_protocol::chunk_stream::StreamExtentPage::default()).unwrap();
    page[40..48].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(decode_stream_extent_page_record(&page, StreamRecordEncoding::Legacy).is_err());
}

#[test]
fn arbitrary_legacy_stream_identity_cannot_be_misread_as_an_envelope() {
    let page = crowdb_protocol::chunk_stream::StreamExtentPage {
        stream_name: crowdb_protocol::chunk_stream::StreamName {
            high: u64::from_le_bytes(*b"CSTR\x02\x00\x02\x00"),
            low: 11,
        },
        ..Default::default()
    };
    let raw = bincode::serialize(&page).unwrap();
    assert_eq!(
        decode_stream_extent_page_record(&raw, StreamRecordEncoding::Legacy)
            .unwrap()
            .value,
        page
    );
    assert!(matches!(
        decode_stream_extent_page_record(&raw, StreamRecordEncoding::Version1),
        Err(StreamRecordError::UnsupportedVersion(2))
    ));
}
