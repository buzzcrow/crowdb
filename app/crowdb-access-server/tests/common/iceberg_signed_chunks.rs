use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_s3::auth::{
    Credential, CredentialProvider, RawAuthRequest, SigV4Verifier, StreamingPayloadVerifier,
};
use hmac::{Hmac, Mac};
use hyper::{HeaderMap, Request};
use sha2::{Digest, Sha256};
use std::fmt::Write;

pub struct TestAwsCredentials;

impl CredentialProvider for TestAwsCredentials {
    fn lookup(&self, access: &str) -> Option<Credential> {
        (access == "AKIAIOSFODNN7EXAMPLE").then(|| Credential {
            secret_key: b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_vec(),
            session_token: None,
            enabled: true,
        })
    }
}

pub fn signed_request() -> Request<()> {
    Request::builder().method("PUT").uri("/examplebucket/chunkObject.txt")
        .header("host", "s3.amazonaws.com")
        .header("content-encoding", "aws-chunked")
        .header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
        .header("x-amz-date", "20130524T000000Z")
        .header("x-amz-decoded-content-length", "66560")
        .header("x-amz-storage-class", "REDUCED_REDUNDANCY")
        .header("x-amz-trailer", "x-amz-checksum-crc32c")
        .header("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer, Signature=106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e")
        .body(()).unwrap()
}

pub fn fixture() -> (HeaderMap, StreamingPayloadVerifier, Vec<u8>) {
    let request = signed_request();
    let verifier = SigV4Verifier::new(TestAwsCredentials, "us-east-1".into(), 900);
    let raw = RawAuthRequest::from_parts(request.method(), request.uri(), request.headers());
    assert!(verifier.verify(raw, 1_369_353_600).is_err());
    let streaming = verifier.verify_streaming(raw, 1_369_353_600).unwrap();
    let mut bytes =
        b"10000;chunk-signature=b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2\r\n"
            .to_vec();
    bytes.extend(vec![b'a'; 65536]);
    bytes.extend_from_slice(
        b"\r\n400;chunk-signature=1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7\r\n",
    );
    bytes.extend(vec![b'a'; 1024]);
    bytes.extend_from_slice(b"\r\n0;chunk-signature=2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992\r\nx-amz-checksum-crc32c:sOO8/Q==\r\nx-amz-trailer-signature:d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435\r\n\r\n");
    (request.into_parts().0.headers, streaming, bytes)
}

pub fn other_fixture(unsigned: bool, empty: bool) -> (HeaderMap, StreamingPayloadVerifier, Vec<u8>) {
    let mut request = signed_request();
    let payload = if empty { b"".as_slice() } else { b"abc".as_slice() };
    let mode = if unsigned {
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    } else {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    };
    request
        .headers_mut()
        .insert("x-amz-content-sha256", mode.parse().unwrap());
    request.headers_mut().insert(
        "x-amz-decoded-content-length",
        payload.len().to_string().parse().unwrap(),
    );
    let mut names = "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class".to_owned();
    if unsigned {
        names.push_str(";x-amz-trailer");
        request
            .headers_mut()
            .insert("x-amz-trailer", "x-amz-checksum-sha256".parse().unwrap());
    } else {
        request.headers_mut().remove("x-amz-trailer");
    }
    let mut headers = String::new();
    for name in names.split(';') {
        writeln!(headers, "{name}:{}", request.headers()[name].to_str().unwrap()).unwrap();
    }
    let canonical = format!("PUT\n/examplebucket/chunkObject.txt\n\n{headers}\n{names}\n{mode}");
    let scope = "20130524/us-east-1/s3/aws4_request";
    let date = "20130524T000000Z";
    let mut key = b"AWS4wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_vec();
    for item in ["20130524", "us-east-1", "s3", "aws4_request"] {
        key = mac(&key, item);
    }
    let seed = hex(&mac(
        &key,
        &format!(
            "AWS4-HMAC-SHA256\n{date}\n{scope}\n{:x}",
            Sha256::digest(canonical)
        ),
    ));
    request.headers_mut().insert("authorization", format!("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/{scope}, SignedHeaders={names}, Signature={seed}").parse().unwrap());
    let mut previous = seed;
    let mut bytes = Vec::new();
    for data in std::iter::once(payload).chain((!payload.is_empty()).then_some(b"".as_slice())) {
        if !bytes.is_empty() {
            bytes.extend_from_slice(b"\r\n");
        }
        if unsigned {
            bytes.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
        } else {
            let signature = hex(&mac(
                &key,
                &format!(
                    "AWS4-HMAC-SHA256-PAYLOAD\n{date}\n{scope}\n{previous}\n{:x}\n{:x}",
                    Sha256::digest([]),
                    Sha256::digest(data)
                ),
            ));
            bytes.extend_from_slice(format!("{:x};chunk-signature={signature}\r\n", data.len()).as_bytes());
            previous = signature;
        }
        bytes.extend_from_slice(data);
    }
    if unsigned {
        bytes.extend_from_slice(
            format!(
                "x-amz-checksum-sha256:{}\r\n",
                STANDARD.encode(Sha256::digest(payload))
            )
            .as_bytes(),
        );
    }
    bytes.extend_from_slice(b"\r\n");
    let verifier = SigV4Verifier::new(TestAwsCredentials, "us-east-1".into(), 900)
        .verify_streaming(
            RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
            1_369_353_600,
        )
        .unwrap();
    (request.into_parts().0.headers, verifier, bytes)
}

fn mac(key: &[u8], message: &str) -> Vec<u8> {
    let mut signer = Hmac::<Sha256>::new_from_slice(key).unwrap();
    signer.update(message.as_bytes());
    signer.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::new();
    for byte in bytes {
        write!(output, "{byte:02x}").unwrap();
    }
    output
}
