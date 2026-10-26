// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    body::{body, owned, TestFrames, TestWriter},
    common::{admission, TARGET},
};
use crowdb_access_server::{
    iceberg::{write_iceberg_fallback_body_for_tests, FileS3ErrorCode},
    upload_flow::body_encoding::UploadBody,
};
use hyper::{
    body::{Bytes, Frame},
    HeaderMap,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test]
async fn pause_bounds_body_polling_and_keeps_feed_and_digest_groups() {
    let frames = TestFrames::new((0..5).map(|value| Bytes::from(vec![value; TARGET])).collect());
    let polls = frames.polls.clone();
    let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 64 << 20).unwrap();
    let admission = admission(64 << 20);
    let mut writer = TestWriter {
        pause: true,
        ..Default::default()
    };
    let (blocked, resume, calls) = (
        writer.blocked.clone(),
        writer.resume.clone(),
        writer.calls.clone(),
    );
    let mut transfer = Box::pin(write_iceberg_fallback_body_for_tests(
        &mut body,
        &mut writer,
        &admission,
        2,
        None,
    ));
    assert!(futures::poll!(&mut transfer).is_pending());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(blocked.load(Ordering::Relaxed));
    assert_eq!(polls.load(Ordering::Relaxed), 3);
    assert!(futures::poll!(&mut transfer).is_pending());
    assert_eq!(polls.load(Ordering::Relaxed), 3);
    blocked.store(false, Ordering::Relaxed);
    resume.notify_one();
    let (_, _, metrics) = transfer.await.unwrap();
    assert_eq!(writer.buffers.len(), 5);
    for (index, buffer) in writer.buffers.iter().enumerate() {
        assert_eq!(buffer.len(), TARGET);
        assert!(buffer.iter().all(|byte| usize::from(*byte) == index));
    }
    assert_eq!(metrics.digest_enqueues, 5);
    assert_eq!(metrics.writer_feeds, 5);
    assert!(metrics.write_flow_pauses > 0);
    assert_eq!(metrics.queued_buffers_peak, 2);
}

#[tokio::test]
async fn cancelling_a_paused_flow_releases_all_body_digest_and_writer_owners() {
    let live = Arc::new(AtomicUsize::new(0));
    let frames = TestFrames::new((0..5).map(|_| owned(TARGET, &live)).collect());
    let polls = frames.polls.clone();
    let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 64 << 20).unwrap();
    let admission = admission(64 << 20);
    let mut writer = TestWriter {
        pause: true,
        ..Default::default()
    };
    let mut transfer = Box::pin(write_iceberg_fallback_body_for_tests(
        &mut body,
        &mut writer,
        &admission,
        2,
        None,
    ));
    assert!(futures::poll!(&mut transfer).is_pending());
    assert_eq!(polls.load(Ordering::Relaxed), 3);
    drop(transfer);
    drop(writer);
    drop(body);
    tokio::time::timeout(Duration::from_secs(2), async {
        while live.load(Ordering::Relaxed) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(live.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn write_failure_stops_before_polling_another_target() {
    let frames = TestFrames::new((0..5).map(|_| Bytes::from(vec![37; TARGET])).collect());
    let polls = frames.polls.clone();
    let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 64 << 20).unwrap();
    let mut writer = TestWriter {
        reject: true,
        ..Default::default()
    };
    assert_eq!(
        write_iceberg_fallback_body_for_tests(&mut body, &mut writer, &admission(64 << 20), 1, None)
            .await
            .unwrap_err(),
        FileS3ErrorCode::SlowDown
    );
    assert_eq!(writer.calls.load(Ordering::Relaxed), 1);
    assert_eq!(polls.load(Ordering::Relaxed), 1);
    assert!(writer.buffers.is_empty());
}

#[tokio::test]
async fn admission_rejects_the_offending_frame_before_handoff() {
    let frames = TestFrames::new(vec![Bytes::from(vec![37; TARGET]), Bytes::from(vec![83; TARGET])]);
    let polls = frames.polls.clone();
    let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 64 << 20).unwrap();
    let mut writer = TestWriter::default();
    assert_eq!(
        write_iceberg_fallback_body_for_tests(&mut body, &mut writer, &admission(TARGET as u64), 2, None)
            .await
            .unwrap_err(),
        FileS3ErrorCode::EntityTooLarge
    );
    assert_eq!(polls.load(Ordering::Relaxed), 2);
    assert!(writer
        .buffers
        .iter()
        .all(|bytes| bytes.iter().all(|byte| *byte == 37)));
}

#[tokio::test]
async fn failed_or_non_data_terminal_frames_do_not_flush_the_pending_tail() {
    for trailers in [false, true] {
        let mut frames = TestFrames::new(vec![Bytes::from_static(b"tail")]);
        frames.frames.push_back(if trailers {
            Ok(Frame::trailers(HeaderMap::new()))
        } else {
            Err(std::io::Error::other("injected terminal failure"))
        });
        let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 100).unwrap();
        let mut writer = TestWriter::default();
        assert_eq!(
            write_iceberg_fallback_body_for_tests(&mut body, &mut writer, &admission(100), 2, None)
                .await
                .unwrap_err(),
            FileS3ErrorCode::InvalidRequest
        );
        assert!(writer.buffers.is_empty());
    }
}

#[tokio::test]
async fn a_late_body_error_preserves_the_full_feed_without_flushing_its_short_tail() {
    let full = Bytes::from(vec![37; TARGET]);
    let mut frames = TestFrames::new(vec![full.clone(), Bytes::from_static(b"tail")]);
    frames
        .frames
        .push_back(Err(std::io::Error::other("injected error after a full feed")));
    let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 64 << 20).unwrap();
    let mut writer = TestWriter::default();
    assert_eq!(
        write_iceberg_fallback_body_for_tests(&mut body, &mut writer, &admission(64 << 20), 1, None)
            .await
            .unwrap_err(),
        FileS3ErrorCode::InvalidRequest
    );
    assert_eq!(writer.buffers, vec![full.clone()]);
    assert_eq!(writer.buffers[0].as_ptr(), full.as_ptr());
}

#[tokio::test]
async fn length_digest_and_crc_failures_keep_the_existing_error_contracts() {
    for (declared, expected) in [
        (Some(2), FileS3ErrorCode::InvalidRequest),
        (Some(4), FileS3ErrorCode::EntityTooLarge),
    ] {
        let mut body = body(vec![Bytes::from_static(b"abc")], declared);
        let mut writer = TestWriter::default();
        assert_eq!(
            write_iceberg_fallback_body_for_tests(&mut body, &mut writer, &admission(100), 2, None)
                .await
                .unwrap_err(),
            expected
        );
        assert!(writer.buffers.is_empty());
    }
    for md5 in [false, true] {
        let mut headers = HeaderMap::new();
        headers.insert("content-length", "3".parse().unwrap());
        if md5 {
            headers.insert("content-md5", "1B2M2Y8AsgTpgAmY7PhCfg==".parse().unwrap());
        }
        let mut body = UploadBody::new(
            TestFrames::new(vec![Bytes::from_static(b"abc")]),
            &headers,
            None,
            100,
        )
        .unwrap();
        let mut writer = TestWriter::default();
        assert_eq!(
            write_iceberg_fallback_body_for_tests(
                &mut body,
                &mut writer,
                &admission(100),
                2,
                (!md5).then_some([0; 32])
            )
            .await
            .unwrap_err(),
            if md5 {
                FileS3ErrorCode::BadDigest
            } else {
                FileS3ErrorCode::InvalidRequest
            }
        );
        assert_eq!(writer.buffers, vec![Bytes::from_static(b"abc")]);
    }
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-checksum-crc32c", "4waSgw==".parse().unwrap());
    for valid in [false, true] {
        let bytes = Bytes::from_static(if valid { b"123456789" } else { b"123456788" });
        let mut body = UploadBody::new(TestFrames::new(vec![bytes]), &headers, None, 100).unwrap();
        let mut writer = TestWriter::default();
        let result =
            write_iceberg_fallback_body_for_tests(&mut body, &mut writer, &admission(100), 2, None).await;
        if valid {
            result.unwrap();
            assert_eq!(writer.buffers.len(), 1);
        } else {
            assert_eq!(result.unwrap_err(), FileS3ErrorCode::BadDigest);
            assert!(writer.buffers.is_empty());
        }
    }
}
