// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_s3::metrics::S3Metrics;
use crowdb_access_server::s3::test_support::write_fallback_body_for_tests;
use crowdb_access_server::upload_flow::body_encoding::UploadBody;
use crowdb_chunk_client::{ChunkIoWriter, FeedStatus, IoError};
use crowdb_protocol::chunkdb::rpc::Location;
use hyper::body::{Body, Bytes, Frame};
use hyper::HeaderMap;
use md5::{Digest, Md5};
use sha2::Sha256;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

pub const TARGET: usize = 1024 * 1024;

pub struct TestFrames {
    pub frames: VecDeque<Result<Frame<Bytes>, std::io::Error>>,
    pub polls: Arc<AtomicUsize>,
}
impl TestFrames {
    pub fn new(frames: Vec<Bytes>) -> Self {
        Self {
            frames: frames.into_iter().map(|bytes| Ok(Frame::data(bytes))).collect(),
            polls: Arc::default(),
        }
    }
}
impl Body for TestFrames {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.frames.pop_front())
    }
}

pub fn body(frames: Vec<Bytes>, declared: Option<usize>) -> UploadBody<TestFrames> {
    let mut headers = HeaderMap::new();
    if let Some(length) = declared {
        headers.insert("content-length", length.to_string().parse().unwrap());
    }
    UploadBody::new(TestFrames::new(frames), &headers, None, 64 << 20).unwrap()
}

#[derive(Default)]
pub struct TestWriter {
    pub buffers: Vec<Bytes>,
    pub calls: Arc<AtomicUsize>,
    pub reject: bool,
    pub pause: bool,
    pub blocked: Arc<AtomicBool>,
    pub resume: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl ChunkIoWriter for TestWriter {
    async fn on_data(&mut self, buffer: Bytes) -> crowdb_chunk_client::Result<FeedStatus> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.reject {
            return Err(IoError::WriteFailed("injected write failure".into()));
        }
        self.buffers.push(buffer);
        if self.pause {
            self.pause = false;
            self.blocked.store(true, Ordering::Relaxed);
            Ok(FeedStatus::Pause)
        } else {
            Ok(FeedStatus::Continue)
        }
    }
    async fn on_finish(&mut self) -> crowdb_chunk_client::Result<Vec<Location>> {
        panic!("body handoff must not finish storage before integrity validation")
    }
    async fn on_error(&mut self) -> crowdb_chunk_client::Result<Vec<Location>> {
        panic!("the caller owns abort and physical cleanup")
    }
    fn require_data(&self) -> bool {
        !self.blocked.load(Ordering::Relaxed)
    }
    async fn wait_for_capacity(&mut self) {
        if self.blocked.load(Ordering::Relaxed) {
            self.resume.notified().await;
        }
    }
}

struct TestOwner {
    bytes: Vec<u8>,
    live: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for TestOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for TestOwner {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}
pub fn owned(length: usize, live: &Arc<AtomicUsize>) -> Bytes {
    live.fetch_add(1, Ordering::Relaxed);
    Bytes::from_owner(TestOwner {
        bytes: vec![37; length],
        live: Arc::clone(live),
    })
}

pub async fn checked(frames: Vec<Bytes>, declared: Option<usize>) -> TestWriter {
    let expected: Vec<_> = frames.iter().flat_map(|bytes| bytes.iter().copied()).collect();
    let md5: [u8; 16] = Md5::digest(&expected).into();
    let expected_md5 = STANDARD.encode(md5);
    let sha = hex::encode(Sha256::digest(&expected));
    let mut body = body(frames, declared);
    let mut writer = TestWriter::default();
    let metrics = S3Metrics::default();
    let result = write_fallback_body_for_tests(
        &mut body,
        &mut writer,
        2,
        Some(&expected_md5),
        Some(&sha),
        Some(&metrics),
    )
    .await
    .unwrap();
    assert_eq!(result, (hex::encode(md5), md5.to_vec()));
    assert_eq!(metrics.snapshot().checksum_bytes, expected.len() as u64);
    let actual: Vec<_> = writer
        .buffers
        .iter()
        .flat_map(|bytes| bytes.iter().copied())
        .collect();
    assert_eq!(actual, expected);
    writer
}
