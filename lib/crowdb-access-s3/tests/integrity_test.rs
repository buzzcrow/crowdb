// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::integrity::{validate_completed_digests, IntegrityError, SinglePartIntegrity};
use hyper::body::Bytes;

#[test]
fn multipart_marker_binds_composite_digest_and_part_count() {
    use crowdb_access_s3::integrity::{is_multipart_checksum, multipart_checksum_marker};

    let etag = "b4ab393b73e0e71830bf2bf0e63c4d91-2";
    let marker = multipart_checksum_marker(etag).unwrap();
    assert_eq!(marker.len(), 18);
    assert_eq!(&marker[16..], &[0, 2]);
    assert!(is_multipart_checksum(&marker, etag));
    assert!(!is_multipart_checksum(
        &marker,
        "b4ab393b73e0e71830bf2bf0e63c4d91-3"
    ));
    assert!(multipart_checksum_marker("b4ab393b73e0e71830bf2bf0e63c4d91-0").is_none());
}

#[test]
fn single_part_etag_is_independent_of_body_frame_boundaries() {
    let mut fragmented = SinglePartIntegrity::default();
    fragmented.update(&Bytes::from_static(b"hel"));
    fragmented.update(&Bytes::from_static(b"lo world"));
    let mut contiguous = SinglePartIntegrity::default();
    contiguous.update(&Bytes::from_static(b"hello world"));

    let fragmented = fragmented.finish();
    let contiguous = contiguous.finish();
    assert_eq!(fragmented, contiguous);
    assert_eq!(contiguous.0, "5eb63bbbe01eeed093cb22bb8f5acdc3");
}

#[test]
fn content_md5_is_checked_before_publication() {
    let mut integrity = SinglePartIntegrity::default();
    integrity.update(&hyper::body::Bytes::from_static(b"hello"));
    assert!(integrity
        .finish_validated(Some("XUFAKrxLKna5cZ2REBfFkg=="))
        .is_ok());

    let mut mismatch = SinglePartIntegrity::default();
    mismatch.update(&hyper::body::Bytes::from_static(b"other"));
    assert!(mismatch
        .finish_validated(Some("XUFAKrxLKna5cZ2REBfFkg=="))
        .is_err());
}

#[test]
fn signed_payload_sha256_is_checked_incrementally() {
    let mut valid = SinglePartIntegrity::new(true);
    valid.update(&Bytes::from_static(b"abc"));
    assert!(valid
        .finish_validated_checksums(
            None,
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        )
        .is_ok());

    let mut mismatch = SinglePartIntegrity::new(true);
    mismatch.update(&Bytes::from_static(b"abc"));
    assert_eq!(
        mismatch.finish_validated_checksums(
            None,
            Some("aa7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        ),
        Err(IntegrityError::PayloadMismatch)
    );
}

#[test]
fn completed_digest_worker_values_keep_s3_checksum_contract() {
    let md5 = [
        0x90, 0x01, 0x50, 0x98, 0x3c, 0xd2, 0x4f, 0xb0, 0xd6, 0x96, 0x3f, 0x7d, 0x28, 0xe1, 0x7f, 0x72,
    ];
    let sha256 = [
        0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23, 0xb0,
        0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
    ];
    let expected_sha = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    assert_eq!(
        validate_completed_digests(
            md5,
            Some(sha256),
            Some("kAFQmDzST7DWlj99KOF/cg=="),
            Some(expected_sha)
        ),
        Ok(("900150983cd24fb0d6963f7d28e17f72".into(), md5.to_vec()))
    );
    assert_eq!(
        validate_completed_digests(
            md5,
            Some(sha256),
            None,
            Some("aa7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        ),
        Err(IntegrityError::PayloadMismatch)
    );
}
