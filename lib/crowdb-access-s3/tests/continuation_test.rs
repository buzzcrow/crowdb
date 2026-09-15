// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::continuation::{ContinuationPosition, ContinuationTokenError, ContinuationTokenSigner};
use crowdb_access_s3::metadata::BucketId;

fn position() -> ContinuationPosition {
    ContinuationPosition {
        bucket_id: BucketId::new(7_u128.to_be_bytes()),
        prefix: b"binary\0prefix".to_vec(),
        delimiter: Some(b"/".to_vec()),
        last_key: b"binary\0prefix/a".to_vec(),
        expires_at_unix_seconds: 200,
    }
}

#[test]
fn token_round_trips_and_is_bound_to_request() {
    let signer = ContinuationTokenSigner::new(b"test-secret".to_vec()).unwrap();
    let expected = position();
    let token = signer.encode(&expected).unwrap();
    let actual = signer
        .decode_for_request(
            &token,
            expected.bucket_id,
            &expected.prefix,
            expected.delimiter.as_deref(),
            100,
        )
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        signer.decode_for_request(&token, expected.bucket_id, b"other", Some(b"/"), 100),
        Err(ContinuationTokenError::RequestMismatch)
    );
}

#[test]
fn token_rejects_tampering_and_expiry() {
    let signer = ContinuationTokenSigner::new(b"test-secret".to_vec()).unwrap();
    let expected = position();
    let mut token = signer.encode(&expected).unwrap().into_bytes();
    token[8] = if token[8] == b'A' { b'B' } else { b'A' };
    let token = String::from_utf8(token).unwrap();
    assert_eq!(
        signer.decode_for_request(
            &token,
            expected.bucket_id,
            &expected.prefix,
            expected.delimiter.as_deref(),
            100
        ),
        Err(ContinuationTokenError::Invalid)
    );
    let valid = signer.encode(&expected).unwrap();
    assert_eq!(
        signer.decode_for_request(
            &valid,
            expected.bucket_id,
            &expected.prefix,
            expected.delimiter.as_deref(),
            201
        ),
        Err(ContinuationTokenError::Expired)
    );
}
