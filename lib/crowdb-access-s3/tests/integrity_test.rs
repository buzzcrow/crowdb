// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::integrity::{IntegrityError, SinglePartIntegrity};
use hyper::body::Bytes;

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
    let mut valid = SinglePartIntegrity::default();
    valid.update(&Bytes::from_static(b"abc"));
    assert!(valid
        .finish_validated_checksums(
            None,
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        )
        .is_ok());

    let mut mismatch = SinglePartIntegrity::default();
    mismatch.update(&Bytes::from_static(b"abc"));
    assert_eq!(
        mismatch.finish_validated_checksums(
            None,
            Some("aa7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        ),
        Err(IntegrityError::PayloadMismatch)
    );
}
