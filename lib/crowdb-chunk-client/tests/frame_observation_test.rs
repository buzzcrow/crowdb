// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/frame_observation.rs"]
mod fixture;
#[path = "common/mirrored_frames.rs"]
mod mirrored_fixture;

use crowdb_chunk_client::{
    observe_frame_prefix, replay_frame_prefix, select_frame_prefix, FrameObservationError, FrameWindowReader,
};
use fixture::{TestExtentSink, TestObservation as Test, TestWindowReader};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

#[path = "frame_observation_test/faults.rs"]
mod faults;
#[path = "frame_observation_test/mirrored.rs"]
mod mirrored;

#[tokio::test]
async fn shortest_complete_replica_prefix_is_selected_and_replayed_without_history_buffers() {
    let first = Test::group(0, &vec![0x41; 1024]);
    let second = Test::group(first.len(), &vec![0x42; 100_000]);
    let longer = [first.as_slice(), second.as_slice()].concat();
    let capacity = longer.len() + 512;
    let short = Arc::new(TestWindowReader::new(Test::backing(&first, capacity)));
    let long = Arc::new(TestWindowReader::new(Test::backing(&longer, capacity)));
    let readers: Vec<Arc<dyn FrameWindowReader>> = vec![short.clone(), long.clone()];
    let deadline = Instant::now() + Duration::from_secs(5);
    let selected = select_frame_prefix(&readers, Test::config(capacity), 4096, deadline)
        .await
        .unwrap();
    assert_eq!(selected.selected_index(), 0);
    assert_eq!(selected.healthy_replicas(), 2);
    assert!(selected.failures().is_empty());
    assert_eq!(selected.observation().summary().physical_end, first.len() as u64);
    assert_eq!(selected.observation().summary().logical_end, 1024);
    let mut sink = TestExtentSink::default();
    let replay = replay_frame_prefix(short.as_ref(), selected.observation(), deadline, &mut sink)
        .await
        .unwrap();
    assert_eq!(sink.records.len(), 1);
    assert_eq!(sink.records[0].logical_length, 1024);
    assert_eq!(short.last_end(), first.len() as u64);
    assert!(short.largest() <= 4096 && long.largest() <= 4096);
    assert_eq!(replay.reads(), 1);
    assert_eq!(replay.read_bytes(), first.len() as u64);
}

#[tokio::test]
async fn a_mid_scan_failure_and_a_corrupt_replica_never_force_a_shorter_prefix() {
    let first = Test::group(0, &vec![0x41; 1024]);
    let second = Test::group(first.len(), &vec![0x42; 100_000]);
    let prefix = [first.as_slice(), second.as_slice()].concat();
    let capacity = prefix.len() + 512;
    let good = Arc::new(TestWindowReader::new(Test::backing(&prefix, capacity)));
    let mut failed = TestWindowReader::new(Test::backing(&prefix, capacity));
    failed.fail_at = Some(8192);
    let mut corrupt = prefix.clone();
    corrupt[first.len() + 27] ^= 1;
    let readers: Vec<Arc<dyn FrameWindowReader>> = vec![
        good,
        Arc::new(failed),
        Arc::new(TestWindowReader::new(Test::backing(&corrupt, capacity))),
    ];
    let selected = select_frame_prefix(
        &readers,
        Test::config(capacity),
        4096,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(selected.selected_index(), 0);
    assert_eq!(selected.healthy_replicas(), 1);
    assert_eq!(selected.failures().len(), 2);
    assert_eq!(selected.observation().summary().physical_end, prefix.len() as u64);
    assert_eq!(selected.observation().summary().logical_end, 101_024);
}

#[tokio::test]
async fn exact_capacity_replay_emits_bounded_location_batches_without_an_extra_read() {
    let mut bytes = Vec::new();
    for _ in 0..3000 {
        bytes.extend(Test::group(bytes.len(), b"a"));
    }
    let capacity = bytes.len();
    let source = TestWindowReader::new(Test::backing(&bytes, capacity));
    let deadline = Instant::now() + Duration::from_secs(5);
    let observed = observe_frame_prefix(&source, Test::config(capacity), 1024 * 1024, deadline)
        .await
        .unwrap();
    let mut sink = TestExtentSink::default();
    replay_frame_prefix(&source, &observed, deadline, &mut sink)
        .await
        .unwrap();
    assert_eq!(sink.records.len(), 3000);
    assert_eq!(sink.largest_batch, 1024);
    assert_eq!(source.calls(), 2);
    assert_eq!(source.last_end(), capacity as u64);
}

#[tokio::test(start_paused = true)]
async fn one_shared_io_deadline_bounds_concurrent_replicas_without_per_replica_resets() {
    let bytes = Test::group(0, b"a");
    let capacity = bytes.len() + 14;
    let mut quick = TestWindowReader::new(Test::backing(&bytes, capacity));
    quick.delay = Duration::from_millis(1);
    let mut slow = TestWindowReader::new(Test::backing(&bytes, capacity));
    slow.delay = Duration::from_secs(60);
    let readers: Vec<Arc<dyn FrameWindowReader>> = vec![Arc::new(quick), Arc::new(slow)];
    let start = Instant::now();
    let selected = select_frame_prefix(
        &readers,
        Test::config(capacity),
        1024,
        start + Duration::from_millis(20),
    )
    .await
    .unwrap();
    assert!(start.elapsed() < Duration::from_millis(20));
    assert_eq!(selected.healthy_replicas(), 1);
    assert_eq!(selected.failures().len(), 1);
    assert_eq!(selected.observation().summary().physical_end, bytes.len() as u64);
    let mut sink = TestExtentSink::default();
    replay_frame_prefix(
        readers[selected.selected_index()].as_ref(),
        selected.observation(),
        start + Duration::from_millis(20),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(sink.records.len(), 1);
    assert!(start.elapsed() < Duration::from_millis(20));
}

#[tokio::test(start_paused = true)]
async fn a_hung_mirror_leaves_time_to_compare_unequal_healthy_ends_and_replay() {
    let first = Test::group(0, b"first");
    let longer = [first.clone(), Test::group(first.len(), b"second")].concat();
    let capacity = longer.len() + 14;
    let mut readers: Vec<Arc<dyn FrameWindowReader>> = Vec::new();
    for (data, delay) in [
        (first.as_slice(), Duration::from_millis(1)),
        (longer.as_slice(), Duration::from_millis(1)),
        (longer.as_slice(), Duration::from_secs(60)),
    ] {
        let mut reader = TestWindowReader::new(Test::backing(data, capacity));
        reader.delay = delay;
        readers.push(Arc::new(reader));
    }
    let start = Instant::now();
    let deadline = start + Duration::from_millis(20);
    let selected = select_frame_prefix(&readers, Test::config(capacity), 1024, deadline)
        .await
        .unwrap();
    assert_eq!(selected.selected_index(), 0);
    assert_eq!(selected.healthy_replicas(), 2);
    assert_eq!(selected.failures().len(), 1);
    let mut sink = TestExtentSink::default();
    replay_frame_prefix(readers[0].as_ref(), selected.observation(), deadline, &mut sink)
        .await
        .unwrap();
    assert_eq!(sink.records.len(), 1);
    assert_eq!(sink.records[0].logical_length, 5);
    assert!(start.elapsed() < Duration::from_millis(20));
}
