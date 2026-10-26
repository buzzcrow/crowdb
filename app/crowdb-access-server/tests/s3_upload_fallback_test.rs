// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/s3_fallback.rs"]
mod common;
use common::{body, checked, owned, TestFrames, TestWriter, TARGET};
use crowdb_access_s3::streaming::{PutErrorCode, PutOutcome};
use crowdb_access_server::s3::test_support::write_fallback_body_for_tests;
use crowdb_access_server::upload_flow::body_encoding::UploadBody;
use hyper::body::{Bytes, Frame};
use hyper::HeaderMap;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test]
async fn one_small_body_frame_retains_its_original_owner() {
    let original = Bytes::from(vec![37; 4096]);
    let writer = checked(vec![original.clone()], Some(original.len())).await;
    assert_eq!(writer.buffers.len(), 1);
    assert_eq!(writer.buffers[0], original);
    assert_eq!(
        writer.buffers[0].as_ptr(),
        original.as_ptr(),
        "the full target was copied"
    );
}

#[tokio::test]
async fn exact_targets_preserve_source_views_and_a_short_unknown_tail_still_coalesces() {
    for declared in [None, Some(2 * TARGET + 37)] {
        let original = Bytes::from(vec![83; 2 * TARGET + 37]);
        let writer = checked(vec![original.clone()], declared).await;
        assert_eq!(
            writer.buffers.iter().map(Bytes::len).collect::<Vec<_>>(),
            [TARGET, TARGET, 37]
        );
        assert_eq!(writer.buffers[0].as_ptr(), original.as_ptr());
        assert_eq!(writer.buffers[1].as_ptr(), original.slice(TARGET..).as_ptr());
        assert_ne!(writer.buffers[2].as_ptr(), original.slice(2 * TARGET..).as_ptr());
    }
}

#[tokio::test]
async fn tiny_empty_and_known_small_inputs_keep_their_original_feed_units() {
    for length in [0, 1, 4096, 64 * 1024, TARGET] {
        let original = Bytes::from(vec![73; length]);
        let writer = checked(vec![Bytes::new(), original.clone(), Bytes::new()], Some(length)).await;
        if length == 0 {
            assert!(writer.buffers.is_empty());
        } else {
            assert_eq!(writer.buffers.len(), 1);
            assert_eq!(writer.buffers[0].as_ptr(), original.as_ptr());
        }
    }
}

#[tokio::test]
async fn fragmented_input_keeps_target_grouping_instead_of_feeding_tiny_fragments() {
    let original = Bytes::from(vec![41; 2 * TARGET + 37]);
    let fragments: Vec<_> = (0..original.len())
        .step_by(997)
        .map(|start| original.slice(start..(start + 997).min(original.len())))
        .collect();
    let writer = checked(fragments, None).await;
    assert_eq!(
        writer.buffers.iter().map(Bytes::len).collect::<Vec<_>>(),
        [TARGET, TARGET, 37]
    );
    assert_ne!(writer.buffers[0].as_ptr(), original.as_ptr());
    assert_ne!(writer.buffers[1].as_ptr(), original.slice(TARGET..).as_ptr());
}

#[tokio::test]
async fn unaligned_fragments_coalesce_before_a_later_full_owner_is_forwarded() {
    let first = Bytes::from(vec![17; TARGET - 17]);
    let middle = Bytes::from(vec![29; 34]);
    let third = Bytes::from(vec![37; TARGET - 17]);
    let direct = Bytes::from(vec![59; TARGET]);
    let writer = checked(vec![first.clone(), middle, third, direct.clone()], None).await;
    assert_eq!(
        writer.buffers.iter().map(Bytes::len).collect::<Vec<_>>(),
        [TARGET, TARGET, TARGET]
    );
    assert_ne!(writer.buffers[0].as_ptr(), first.as_ptr());
    assert_eq!(writer.buffers[2].as_ptr(), direct.as_ptr());
}

#[tokio::test]
async fn pause_retains_the_same_bounded_body_polling_and_resumes_without_reordering() {
    let frames = TestFrames::new((0..5).map(|n| Bytes::from(vec![n; TARGET])).collect());
    let polls = frames.polls.clone();
    let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 64 << 20).unwrap();
    let mut writer = TestWriter {
        pause: true,
        ..Default::default()
    };
    let blocked = writer.blocked.clone();
    let resume = writer.resume.clone();
    let calls = writer.calls.clone();
    let mut transfer = Box::pin(write_fallback_body_for_tests(
        &mut body,
        &mut writer,
        2,
        None,
        None,
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
    transfer.await.unwrap();
    assert_eq!(writer.buffers.len(), 5);
    for (n, buffer) in writer.buffers.iter().enumerate() {
        assert_eq!(buffer.len(), TARGET);
        assert!(buffer.iter().all(|byte| usize::from(*byte) == n));
    }
}

#[tokio::test]
async fn cancelling_a_paused_handoff_releases_queued_digest_and_source_owners() {
    let live = Arc::new(AtomicUsize::new(0));
    let frames = TestFrames::new((0..5).map(|_| owned(TARGET, &live)).collect());
    let polls = frames.polls.clone();
    let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 64 << 20).unwrap();
    let mut writer = TestWriter {
        pause: true,
        ..Default::default()
    };
    let mut transfer = Box::pin(write_fallback_body_for_tests(
        &mut body,
        &mut writer,
        2,
        None,
        None,
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
async fn writer_failure_stops_body_polling_at_the_first_full_target() {
    let frames = TestFrames::new((0..5).map(|_| Bytes::from(vec![11; TARGET])).collect());
    let polls = frames.polls.clone();
    let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 64 << 20).unwrap();
    let mut writer = TestWriter {
        reject: true,
        ..Default::default()
    };
    assert!(matches!(
        write_fallback_body_for_tests(&mut body, &mut writer, 1, None, None, None).await,
        Err(PutOutcome::Error {
            code: PutErrorCode::ChunkWrite,
            ..
        })
    ));
    assert_eq!(writer.calls.load(Ordering::Relaxed), 1);
    assert_eq!(polls.load(Ordering::Relaxed), 1);
    assert!(writer.buffers.is_empty());
}

#[tokio::test]
async fn body_failure_and_non_data_frames_do_not_flush_an_incomplete_tail() {
    for trailer in [false, true] {
        let mut frames = TestFrames::new(vec![Bytes::from_static(b"tail")]);
        frames.frames.push_back(if trailer {
            Ok(Frame::trailers(HeaderMap::new()))
        } else {
            Err(std::io::Error::other("injected body failure"))
        });
        let mut body = UploadBody::new(frames, &HeaderMap::new(), None, 64 << 20).unwrap();
        let mut writer = TestWriter::default();
        assert!(matches!(
            write_fallback_body_for_tests(&mut body, &mut writer, 2, None, None, None).await,
            Err(PutOutcome::Error {
                code: PutErrorCode::BodyRead,
                ..
            })
        ));
        assert!(writer.buffers.is_empty());
    }
}

#[tokio::test]
async fn length_mismatches_and_actual_md5_sha256_crc_fail_closed() {
    for declared in [Some(2), Some(4)] {
        let mut body = body(vec![Bytes::from_static(b"abc")], declared);
        let mut writer = TestWriter::default();
        assert!(matches!(
            write_fallback_body_for_tests(&mut body, &mut writer, 2, None, None, None).await,
            Err(PutOutcome::Error {
                code: PutErrorCode::BodyRead,
                ..
            })
        ));
        assert!(writer.buffers.is_empty());
    }
    for md5 in [false, true] {
        let mut body = body(vec![Bytes::from_static(b"abc")], Some(3));
        let mut writer = TestWriter::default();
        let sha = "0".repeat(64);
        let result = write_fallback_body_for_tests(
            &mut body,
            &mut writer,
            2,
            md5.then_some("1B2M2Y8AsgTpgAmY7PhCfg=="),
            (!md5).then_some(sha.as_str()),
            None,
        )
        .await;
        assert!(matches!(result, Err(PutOutcome::Error { code, .. })
            if code == if md5 { PutErrorCode::BadDigest } else { PutErrorCode::PayloadMismatch }));
        assert_eq!(writer.buffers, vec![Bytes::from_static(b"abc")]);
    }
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-checksum-crc32c", "4waSgw==".parse().unwrap());
    for valid in [false, true] {
        let bytes = Bytes::from_static(if valid { b"123456789" } else { b"123456788" });
        let mut body = UploadBody::new(TestFrames::new(vec![bytes]), &headers, None, 100).unwrap();
        let mut writer = TestWriter::default();
        let result = write_fallback_body_for_tests(&mut body, &mut writer, 2, None, None, None).await;
        if valid {
            assert_eq!(result.unwrap().0, "25f9e794323b453885f5181f1b624d0b");
            assert_eq!(writer.buffers.len(), 1);
        } else {
            assert!(matches!(
                result,
                Err(PutOutcome::Error {
                    code: PutErrorCode::BadDigest,
                    ..
                })
            ));
            assert!(writer.buffers.is_empty());
        }
    }
}
