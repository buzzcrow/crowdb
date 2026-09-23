#![cfg(feature = "iceberg")]

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    FileCredentials, FileGrant, FileGrantIssuer, FileOperation, FileOperations,
};
use crowdb_access_iceberg::key::{CatalogId, OperationId, TableId};
use crowdb_access_s3::auth::RawAuthRequest;
use crowdb_access_server::iceberg::authenticate_file_request;
use hmac::{Hmac, Mac};
use hyper::{header::HeaderValue, Request};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

const NOW: u64 = 1_704_067_200_000;
const DATE: &str = "20240101T000000Z";
const HASH: &str = "UNSIGNED-PAYLOAD";

fn credentials() -> (FileGrantIssuer, FileCredentials) {
    let issuer = FileGrantIssuer::new([42; 32], 60_000).unwrap();
    let credentials = issuer
        .issue(FileGrant {
            context: CatalogContext {
                catalog: CatalogId::random(),
                activation_epoch: 1,
            },
            table: TableId::random(),
            principal: [7; 32],
            nonce: OperationId::random(),
            issued_ms: NOW,
            expires_ms: NOW + 60_000,
            operations: FileOperations::new(&[FileOperation::Get]).unwrap(),
            max_request_bytes: 1024,
            max_file_bytes: 4096,
        })
        .unwrap();
    (issuer, credentials)
}

fn mac(key: &[u8], value: &str) -> Vec<u8> {
    let mut signer = Hmac::<Sha256>::new_from_slice(key).unwrap();
    signer.update(value.as_bytes());
    signer.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut result, "{byte:02x}").unwrap();
    }
    result
}

fn signature(credentials: &FileCredentials, canonical: &str) -> String {
    let date = mac(
        format!("AWS4{}", credentials.secret_access_key()).as_bytes(),
        "20240101",
    );
    let region = mac(&date, "us-east-1");
    let service = mac(&region, "s3");
    let key = mac(&service, "aws4_request");
    hex(&mac(
        &key,
        &format!(
            "AWS4-HMAC-SHA256\n{DATE}\n20240101/us-east-1/s3/aws4_request\n{}",
            hex(&Sha256::digest(canonical))
        ),
    ))
}

fn signed(credentials: &FileCredentials, presigned: bool) -> Request<()> {
    let mut request = Request::builder()
        .method("GET")
        .uri("/bucket/key")
        .header("host", "localhost");
    if presigned {
        let query = format!("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}%2F20240101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date={DATE}&X-Amz-Expires=60&X-Amz-Security-Token={}&X-Amz-SignedHeaders=host", credentials.access_key_id(), credentials.session_token());
        let canonical = format!("GET\n/bucket/key\n{query}\nhost:localhost\n\nhost\n{HASH}");
        request = request.uri(format!(
            "/bucket/key?{query}&X-Amz-Signature={}",
            signature(credentials, &canonical)
        ));
    } else {
        let names = "host;x-amz-content-sha256;x-amz-date;x-amz-security-token";
        let canonical = format!("GET\n/bucket/key\n\nhost:localhost\nx-amz-content-sha256:{HASH}\nx-amz-date:{DATE}\nx-amz-security-token:{}\n\n{names}\n{HASH}", credentials.session_token());
        request = request.header("x-amz-content-sha256", HASH).header("x-amz-date", DATE)
            .header("x-amz-security-token", credentials.session_token())
            .header("authorization", format!("AWS4-HMAC-SHA256 Credential={}/20240101/us-east-1/s3/aws4_request, SignedHeaders={names}, Signature={}", credentials.access_key_id(), signature(credentials, &canonical)));
    }
    request.body(()).unwrap()
}

fn verify(issuer: &FileGrantIssuer, context: CatalogContext, request: &Request<()>, now: u64) -> bool {
    authenticate_file_request(
        issuer,
        context,
        RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
        "us-east-1",
        now,
    )
    .is_ok()
}

#[test]
fn native_file_auth_verifies_header_and_presigned_requests_with_exact_grant_expiry() {
    let (issuer, credentials) = credentials();
    let context = credentials.grant().context;
    for presigned in [false, true] {
        let request = signed(&credentials, presigned);
        let grant = authenticate_file_request(
            &issuer,
            context,
            RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
            "us-east-1",
            NOW,
        )
        .unwrap();
        assert_eq!(&grant, credentials.grant());
        assert!(!verify(&issuer, context, &request, NOW - 1));
        assert!(!verify(&issuer, context, &request, NOW + 60_000));
        assert!(!verify(
            &issuer,
            CatalogContext {
                activation_epoch: 2,
                ..context
            },
            &request,
            NOW
        ));
        assert!(!verify(
            &FileGrantIssuer::new([43; 32], 60_000).unwrap(),
            context,
            &request,
            NOW
        ));
    }
}

#[test]
fn native_file_tokens_are_not_bearer_credentials_and_signatures_bind_request_bytes() {
    let (issuer, credentials) = credentials();
    let context = credentials.grant().context;
    let mut request = signed(&credentials, false);
    *request.uri_mut() = "/bucket/other".parse().unwrap();
    assert!(!verify(&issuer, context, &request, NOW));
    let mut request = signed(&credentials, false);
    request.headers_mut().remove("authorization");
    assert!(!verify(&issuer, context, &request, NOW));
    let mut request = signed(&credentials, false);
    *request.method_mut() = hyper::Method::DELETE;
    assert!(!verify(&issuer, context, &request, NOW));
}

#[test]
fn ambiguous_and_oversized_authentication_fails_before_signature_work() {
    let (issuer, credentials) = credentials();
    let context = credentials.grant().context;
    for name in [
        "authorization",
        "host",
        "x-amz-date",
        "x-amz-content-sha256",
        "x-amz-security-token",
    ] {
        let mut request = signed(&credentials, false);
        let value = request.headers()[name].clone();
        request.headers_mut().append(name, value);
        assert!(!verify(&issuer, context, &request, NOW));
    }
    let mut request = signed(&credentials, false);
    request
        .headers_mut()
        .insert("extra", HeaderValue::from_str(&"x".repeat(16384)).unwrap());
    assert!(!verify(&issuer, context, &request, NOW));
    for suffix in [
        "&X-Amz-Expires=60",
        "&%58-Amz-Expires=60",
        "&X-Amz-Security-Token=wrong",
    ] {
        let mut request = signed(&credentials, true);
        *request.uri_mut() = format!("{}{suffix}", request.uri()).parse().unwrap();
        assert!(!verify(&issuer, context, &request, NOW));
    }
}
