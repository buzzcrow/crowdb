// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn conflicting_valid_complete_contents_fail_instead_of_waiting_for_an_arbiter() {
    let left = Test::group(0, b"left");
    let right = Test::group(0, b"next");
    let capacity = left.len() + 14;
    let readers: Vec<Arc<dyn FrameWindowReader>> = vec![
        Arc::new(TestWindowReader::new(Test::backing(&left, capacity))),
        Arc::new(TestWindowReader::new(Test::backing(&right, capacity))),
    ];
    assert!(matches!(
        select_frame_prefix(
            &readers,
            Test::config(capacity),
            1024,
            Instant::now() + Duration::from_secs(5)
        )
        .await,
        Err(FrameObservationError::ConflictingPrefixes { .. })
    ));
}

#[tokio::test]
async fn unequal_valid_ends_still_require_identical_overlapping_contents() {
    let short = Test::group(0, b"left");
    let first = Test::group(0, b"next");
    let longer = [first.as_slice(), Test::group(first.len(), b"tail").as_slice()].concat();
    let capacity = longer.len() + 14;
    let readers: Vec<Arc<dyn FrameWindowReader>> = vec![
        Arc::new(TestWindowReader::new(Test::backing(&short, capacity))),
        Arc::new(TestWindowReader::new(Test::backing(&longer, capacity))),
    ];
    assert!(matches!(
        select_frame_prefix(
            &readers,
            Test::config(capacity),
            1024,
            Instant::now() + Duration::from_secs(5)
        )
        .await,
        Err(FrameObservationError::ConflictingPrefixes { .. })
    ));
}

#[tokio::test]
async fn no_healthy_observation_or_a_short_read_is_explicitly_unavailable() {
    let bytes = Test::group(0, b"a");
    let capacity = bytes.len() + 14;
    let mut failed = TestWindowReader::new(Test::backing(&bytes, capacity));
    failed.fail_at = Some(0);
    let mut short = TestWindowReader::new(Test::backing(&bytes, capacity));
    short.short_at = Some(0);
    let readers: Vec<Arc<dyn FrameWindowReader>> = vec![Arc::new(failed), Arc::new(short)];
    let result = select_frame_prefix(
        &readers,
        Test::config(capacity),
        1024,
        Instant::now() + Duration::from_secs(5),
    )
    .await;
    let Err(FrameObservationError::NoHealthyReplica { failures }) = result else {
        panic!("failed reads must not become empty candidates")
    };
    assert_eq!(failures.len(), 2);
}

#[tokio::test]
async fn replay_rejects_changed_contents_and_sink_failure() {
    let bytes = Test::group(0, b"a");
    let source = TestWindowReader::new(Test::backing(&bytes, bytes.len()));
    let deadline = Instant::now() + Duration::from_secs(5);
    let observed = observe_frame_prefix(&source, Test::config(bytes.len()), 1024, deadline)
        .await
        .unwrap();
    let changed = Test::group(0, b"b");
    let changed = TestWindowReader::new(Test::backing(&changed, changed.len()));
    let mut sink = TestExtentSink::default();
    assert!(matches!(
        replay_frame_prefix(&changed, &observed, deadline, &mut sink).await,
        Err(FrameObservationError::ChangedPrefix)
    ));
    let mut sink = TestExtentSink {
        fail: true,
        ..TestExtentSink::default()
    };
    assert!(matches!(
        replay_frame_prefix(&source, &observed, deadline, &mut sink).await,
        Err(FrameObservationError::Sink(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn mapping_io_and_past_deadlines_cannot_create_a_new_budget() {
    let bytes = Test::group(0, b"a");
    let source = TestWindowReader::new(Test::backing(&bytes, bytes.len()));
    let deadline = Instant::now() + Duration::from_millis(20);
    let observed = observe_frame_prefix(&source, Test::config(bytes.len()), 1024, deadline)
        .await
        .unwrap();
    let mut sink = TestExtentSink {
        delay: Duration::from_secs(1),
        ..TestExtentSink::default()
    };
    assert!(matches!(
        replay_frame_prefix(&source, &observed, deadline, &mut sink).await,
        Err(FrameObservationError::DeadlineExpired)
    ));
    let calls = source.calls();
    assert!(matches!(
        observe_frame_prefix(&source, Test::config(bytes.len()), 1024, deadline).await,
        Err(FrameObservationError::DeadlineExpired)
    ));
    assert_eq!(source.calls(), calls);
}
