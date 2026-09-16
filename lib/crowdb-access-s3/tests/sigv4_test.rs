// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::auth::{Credential, CredentialProvider, RawAuthRequest, SigV4Verifier};
use hmac::{Hmac, Mac};
use hyper::{Method, Request};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

struct Provider;

impl CredentialProvider for Provider {
    fn lookup(&self, access_key: &str) -> Option<Credential> {
        (access_key == "AKIDEXAMPLE").then(|| Credential {
            secret_key: b"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_vec(),
            session_token: None,
            enabled: true,
        })
    }
}

#[test]
fn verifies_header_signature_and_rejects_a_changed_uri() {
    let date = "20150830";
    let amz_date = "20150830T123600Z";
    let payload_hash = format!("{:x}", Sha256::digest([]));
    let canonical_headers =
        format!("host:localhost\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request =
        format!("GET\n/bucket/key\nx=1\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = "20150830/us-east-1/s3/aws4_request";
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{:x}",
        Sha256::digest(canonical_request.as_bytes())
    );
    let key = derive_key(b"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", date, "us-east-1");
    let signature = hex(&sign(&key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );
    let request = Request::builder()
        .method(Method::GET)
        .uri("/bucket/key?x=1")
        .header("host", "localhost")
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .header("authorization", authorization)
        .body(())
        .unwrap();
    let verifier = SigV4Verifier::new(Provider, "us-east-1".into(), 900);
    let now = u64::try_from(
        chrono::DateTime::parse_from_rfc3339("2015-08-30T12:36:00Z")
            .unwrap()
            .timestamp(),
    )
    .unwrap();
    assert!(verifier
        .verify(
            RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
            now,
        )
        .is_ok());

    let changed = "/bucket/other?x=1".parse().unwrap();
    assert!(verifier
        .verify(
            RawAuthRequest::from_parts(request.method(), &changed, request.headers()),
            now,
        )
        .is_err());
}

#[test]
fn verifies_presigned_request_and_rejects_expiry() {
    let date = "20150830";
    let amz_date = "20150830T123600Z";
    let scope = "20150830/us-east-1/s3/aws4_request";
    let canonical_query = concat!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&",
        "X-Amz-Credential=AKIDEXAMPLE%2F20150830%2Fus-east-1%2Fs3%2Faws4_request&",
        "X-Amz-Date=20150830T123600Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host"
    );
    let canonical_request =
        format!("GET\n/bucket/key\n{canonical_query}\nhost:localhost\n\nhost\nUNSIGNED-PAYLOAD");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{:x}",
        Sha256::digest(canonical_request.as_bytes())
    );
    let key = derive_key(b"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", date, "us-east-1");
    let signature = hex(&sign(&key, string_to_sign.as_bytes()));
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/bucket/key?{canonical_query}&X-Amz-Signature={signature}"
        ))
        .header("host", "localhost")
        .body(())
        .unwrap();
    let verifier = SigV4Verifier::new(Provider, "us-east-1".into(), 0);
    let timestamp = u64::try_from(
        chrono::DateTime::parse_from_rfc3339("2015-08-30T12:36:00Z")
            .unwrap()
            .timestamp(),
    )
    .unwrap();
    let raw = RawAuthRequest::from_parts(request.method(), request.uri(), request.headers());
    assert!(verifier.verify(raw, timestamp).is_ok());
    assert!(verifier.verify(raw, timestamp + 301).is_err());
}

fn derive_key(secret: &[u8], date: &str, region: &str) -> Vec<u8> {
    let mut initial = b"AWS4".to_vec();
    initial.extend_from_slice(secret);
    let date = sign(&initial, date.as_bytes());
    let region = sign(&date, region.as_bytes());
    let service = sign(&region, b"s3");
    sign(&service, b"aws4_request")
}

fn sign(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn hex(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}
