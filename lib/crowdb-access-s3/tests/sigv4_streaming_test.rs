use crowdb_access_s3::auth::{Credential, CredentialProvider, RawAuthRequest, SigV4Verifier};
use hyper::Request;
use sha2::{Digest, Sha256};

struct TestCredentials;

impl CredentialProvider for TestCredentials {
    fn lookup(&self, access: &str) -> Option<Credential> {
        (access == "AKIAIOSFODNN7EXAMPLE").then(|| Credential {
            secret_key: b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_vec(),
            session_token: None,
            enabled: true,
        })
    }
}

#[test]
fn aws_streaming_reference_signatures_require_explicit_opt_in_and_verified_chain() {
    let request = Request::builder().method("PUT").uri("/examplebucket/chunkObject.txt")
        .header("host", "s3.amazonaws.com")
        .header("content-encoding", "aws-chunked")
        .header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
        .header("x-amz-date", "20130524T000000Z")
        .header("x-amz-decoded-content-length", "66560")
        .header("x-amz-storage-class", "REDUCED_REDUNDANCY")
        .header("x-amz-trailer", "x-amz-checksum-crc32c")
        .header("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer, Signature=106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e")
        .body(()).unwrap();
    let verifier = SigV4Verifier::new(TestCredentials, "us-east-1".into(), 900);
    let raw = RawAuthRequest::from_parts(request.method(), request.uri(), request.headers());
    assert!(verifier.verify(raw, 1_369_353_600).is_err());
    assert!(verifier.verify_streaming(raw, 1_369_353_600 + 901).is_err());
    let mut streaming = verifier.verify_streaming(raw, 1_369_353_600).unwrap();
    let chunks = [
        (
            65536,
            "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2",
        ),
        (
            1024,
            "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7",
        ),
        (
            0,
            "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992",
        ),
    ];
    for (length, signature) in chunks {
        let digest = Sha256::digest(vec![b'a'; length]).into();
        assert!(streaming.verify_chunk(digest, Some(&"0".repeat(64))).is_err());
        streaming.verify_chunk(digest, Some(signature)).unwrap();
    }
    assert!(streaming
        .verify_trailer(
            "x-amz-checksum-crc32c:changed\n",
            Some("d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435")
        )
        .is_err());
    streaming
        .verify_trailer(
            "x-amz-checksum-crc32c:sOO8/Q==\n",
            Some("d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435"),
        )
        .unwrap();
}
