#![cfg(feature = "iceberg")]

#[path = "common/iceberg_signed_chunks.rs"]
mod signed;

use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use crowdb_access_s3::auth::{RawAuthRequest, SigV4Verifier};
use crowdb_access_server::iceberg::{FileEncodingError, FileUploadBody};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Bytes, Frame};
use hyper::{header::HeaderValue, HeaderMap};

struct TestFrames(VecDeque<Bytes>);

impl Body for TestFrames {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Poll::Ready(self.0.pop_front().map(|bytes| Ok(Frame::data(bytes))))
    }
}

#[tokio::test]
async fn aws_published_signed_trailer_vector_survives_arbitrary_http_boundaries() {
    for width in [1, 7, 16 * 1024, 100_000] {
        let (headers, verifier, bytes) = signed::fixture();
        let input = TestFrames(bytes.chunks(width).map(Bytes::copy_from_slice).collect());
        let mut body = FileUploadBody::new(input, &headers, Some(verifier), 100_000).unwrap();
        assert_eq!(body.decoded_length(), Some(66560));
        let mut output = Vec::new();
        while let Some(frame) = body.frame().await {
            let bytes = frame.unwrap().into_data().unwrap();
            assert!(bytes.len() <= 64 * 1024);
            output.extend_from_slice(&bytes);
        }
        assert!(body.is_end_stream());
        assert_eq!(output, vec![b'a'; 66560]);
    }
}

#[tokio::test]
async fn corrupt_chunks_checksums_signatures_suffixes_and_truncation_fail_closed() {
    for variant in 0..7 {
        let (headers, verifier, mut bytes) = signed::fixture();
        match variant {
            0 => bytes[100] ^= 1,
            1 => bytes[25] = b'0',
            2 => {
                let offset = bytes.windows(8).position(|bytes| bytes == b"sOO8/Q==").unwrap();
                bytes[offset] = b't';
            }
            3 => {
                let offset = bytes
                    .windows(b"x-amz-trailer-signature:".len())
                    .position(|bytes| bytes == b"x-amz-trailer-signature:")
                    .unwrap();
                bytes[offset + b"x-amz-trailer-signature:".len()] = b'0';
            }
            4 => bytes.extend_from_slice(b"extra"),
            5 => {
                bytes.truncate(bytes.len() - 2);
            }
            6 => {
                bytes.truncate(100);
            }
            _ => unreachable!(),
        }
        let mut body =
            FileUploadBody::new(Full::new(Bytes::from(bytes)), &headers, Some(verifier), 100_000).unwrap();
        loop {
            match body.frame().await {
                Some(Ok(_)) => {}
                Some(Err(_)) => break,
                None => panic!("corrupt variant {variant} succeeded"),
            }
        }
        assert!(body.is_end_stream());
        assert!(body.frame().await.is_none());
    }
}

#[tokio::test]
async fn encoded_byte_budget_and_plain_checksum_headers_are_enforced() {
    let (headers, verifier, bytes) = signed::fixture();
    let body = FileUploadBody::new(Full::new(Bytes::from(bytes)), &headers, Some(verifier), 66560).unwrap();
    assert!(matches!(body.collect().await, Err(FileEncodingError::Length)));
    for (name, value) in [("crc32", "y/Q5Jg=="), ("crc32c", "4waSgw==")] {
        let mut headers = HeaderMap::new();
        headers.insert(
            format!("x-amz-checksum-{name}")
                .parse::<hyper::header::HeaderName>()
                .unwrap(),
            HeaderValue::from_static(value),
        );
        let body =
            FileUploadBody::new(Full::new(Bytes::from_static(b"123456789")), &headers, None, 100).unwrap();
        assert_eq!(body.collect().await.unwrap().to_bytes(), b"123456789".as_slice());
        let body =
            FileUploadBody::new(Full::new(Bytes::from_static(b"123456788")), &headers, None, 100).unwrap();
        assert!(matches!(body.collect().await, Err(FileEncodingError::Checksum)));
    }
}

#[test]
fn altered_streaming_seed_and_duplicate_framing_headers_are_rejected() {
    let verifier = SigV4Verifier::new(signed::TestAwsCredentials, "us-east-1".into(), 900);
    for field in [
        "x-amz-decoded-content-length",
        "x-amz-trailer",
        "content-encoding",
    ] {
        let mut request = signed::signed_request();
        request
            .headers_mut()
            .insert(field, HeaderValue::from_static("changed"));
        assert!(verifier
            .verify_streaming(
                RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
                1_369_353_600
            )
            .is_err());
        let mut request = signed::signed_request();
        let value = request.headers()[field].clone();
        request.headers_mut().append(field, value);
        assert!(verifier
            .verify_streaming(
                RawAuthRequest::from_parts(request.method(), request.uri(), request.headers()),
                1_369_353_600
            )
            .is_err());
    }
    let (mut headers, verifier, _) = signed::fixture();
    headers.append("x-amz-decoded-content-length", HeaderValue::from_static("66560"));
    assert!(FileUploadBody::new(Full::new(Bytes::new()), &headers, Some(verifier), 100_000).is_err());
}

#[tokio::test]
async fn signed_without_trailers_and_unsigned_trailers_require_complete_framing() {
    for unsigned in [false, true] {
        for empty in [false, true] {
            let (headers, verifier, bytes) = signed::other_fixture(unsigned, empty);
            let input = TestFrames(bytes.chunks(1).map(Bytes::copy_from_slice).collect());
            let body = FileUploadBody::new(input, &headers, Some(verifier), 1000).unwrap();
            assert_eq!(
                body.collect().await.unwrap().to_bytes().as_ref(),
                if empty { b"".as_slice() } else { b"abc".as_slice() }
            );
            let (headers, verifier, mut bytes) = signed::other_fixture(unsigned, empty);
            bytes.truncate(bytes.len() - 2);
            let body =
                FileUploadBody::new(Full::new(Bytes::from(bytes)), &headers, Some(verifier), 1000).unwrap();
            assert!(body.collect().await.is_err());
        }
    }
}
