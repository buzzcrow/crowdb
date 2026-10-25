// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_client::MirroredFrameReaders;
use crowdb_protocol::chunkdb::rpc::Strip;
use std::sync::atomic::Ordering;

use super::*;
use crate::mirrored_fixture::TestMirroredFrames as Layout;

#[tokio::test]
async fn pinned_layout_reads_grouped_data_beyond_old_cursor_across_frames_and_strips() {
    let layout = Layout::new();
    assert_eq!(layout.chunk.acknowledged_cursor, 0);
    let mut chunk = Arc::new(layout.chunk);
    let deadline = Instant::now() + Duration::from_secs(5);
    let readers =
        MirroredFrameReaders::new(Arc::clone(&chunk), Layout::NAME, layout.disks.clone(), deadline).unwrap();
    Arc::make_mut(&mut chunk).strips.clear();
    assert_eq!(readers.capacity(), layout.capacity);
    assert_eq!(readers.deadline(), deadline);
    let config = readers.scan_config(0, 700, 0, 64 * 1024 * 1024).unwrap();
    let selected = select_frame_prefix(readers.readers(), config, 65536, deadline)
        .await
        .unwrap();
    assert_eq!(selected.healthy_replicas(), 2);
    assert_eq!(selected.observation().summary().physical_end, layout.body_length);
    assert_eq!(selected.observation().summary().logical_end, 100_700);
    let mut sink = TestExtentSink::default();
    replay_frame_prefix(
        readers.readers()[selected.selected_index()].as_ref(),
        selected.observation(),
        deadline,
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(sink.records.len(), 1);
    assert_eq!(sink.records[0].physical_length, layout.body_length);
    assert_eq!(sink.records[0].logical_offset, 700);
    assert_eq!(sink.records[0].logical_length, 100_000);
}

#[tokio::test]
async fn unavailable_replica_is_excluded_without_reading_or_mutating_its_layout() {
    let mut layout = Layout::new();
    let Some(Strip::MirrorStrip(mirror)) = &layout.chunk.strips[0].strip else {
        panic!("mirror missing")
    };
    let unavailable = mirror.segments[0];
    layout.chunk.strips[0].unavailable_segments.push(unavailable);
    let deadline = Instant::now() + Duration::from_secs(5);
    let strips = layout.chunk.strips.len();
    let readers = MirroredFrameReaders::new(
        Arc::new(layout.chunk),
        Layout::NAME,
        layout.disks.clone(),
        deadline,
    )
    .unwrap();
    let selected = select_frame_prefix(
        readers.readers(),
        readers.scan_config(0, 0, 0, 64 * 1024 * 1024).unwrap(),
        65536,
        deadline,
    )
    .await
    .unwrap();
    assert_eq!(selected.selected_index(), 1);
    assert_eq!(selected.healthy_replicas(), 1);
    assert_eq!(layout.disks.calls.load(Ordering::Relaxed), strips);
}

#[tokio::test(start_paused = true)]
async fn expired_layout_clones_and_inflight_reads_cannot_outlive_their_authority() {
    let layout = Layout::new();
    layout.disks.delay_ms.store(40, Ordering::Relaxed);
    let deadline = Instant::now() + Duration::from_millis(20);
    let readers = MirroredFrameReaders::new(
        Arc::new(layout.chunk),
        Layout::NAME,
        layout.disks.clone(),
        deadline,
    )
    .unwrap();
    let reader = Arc::clone(&readers.readers()[0]);
    assert!(reader.read_window(0, 512).await.is_err());
    assert_eq!(Instant::now(), deadline);
    let calls = layout.disks.calls.load(Ordering::Relaxed);
    assert!(reader.read_window(0, 512).await.is_err());
    assert_eq!(layout.disks.calls.load(Ordering::Relaxed), calls);
}

#[tokio::test]
async fn scope_bounds_and_short_segment_reads_fail_without_fabricated_bytes() {
    let layout = Layout::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let readers = MirroredFrameReaders::new(
        Arc::new(layout.chunk),
        Layout::NAME,
        layout.disks.clone(),
        deadline,
    )
    .unwrap();
    let reader = &readers.readers()[0];
    assert!(reader.read_window(layout.capacity - 1, 2).await.is_err());
    assert!(reader.read_window(0, 1024 * 1024 + 1).await.is_err());
    assert_eq!(layout.disks.calls.load(Ordering::Relaxed), 0);
    layout.disks.short.store(true, Ordering::Relaxed);
    assert!(reader.read_window(0, 512).await.is_err());
    assert_eq!(layout.disks.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn wrong_owner_gapped_layout_and_missing_allocation_identity_fail_before_io() {
    let layout = Layout::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut wrong = layout.chunk.clone();
    wrong.owner_key.clear();
    assert!(
        MirroredFrameReaders::new(Arc::new(wrong), Layout::NAME, layout.disks.clone(), deadline).is_err()
    );
    let mut gap = layout.chunk.clone();
    gap.strips[1].chunk_offset += 1;
    assert!(MirroredFrameReaders::new(Arc::new(gap), Layout::NAME, layout.disks.clone(), deadline).is_err());
    let mut wrong = layout.chunk;
    let Some(Strip::MirrorStrip(mirror)) = &mut wrong.strips[0].strip else {
        panic!("mirror missing")
    };
    mirror.segments[0].allocation_ts = 0;
    assert!(
        MirroredFrameReaders::new(Arc::new(wrong), Layout::NAME, layout.disks.clone(), deadline).is_err()
    );
    assert_eq!(layout.disks.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn the_current_five_copy_configuration_is_observed_without_an_artificial_three_copy_limit() {
    use crowdb_protocol::common::DiskId;
    let mut layout = Layout::new();
    for strip in &mut layout.chunk.strips {
        let Some(Strip::MirrorStrip(mirror)) = &mut strip.strip else {
            panic!("mirror missing")
        };
        let original = mirror.segments[0];
        for high in 3..=5 {
            mirror.segments.push(crowdb_protocol::diskdb::rpc::Segment {
                disk_id: Some(DiskId { high, low: 1 }),
                ..original
            });
        }
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let readers =
        MirroredFrameReaders::new(Arc::new(layout.chunk), Layout::NAME, layout.disks, deadline).unwrap();
    let selected = select_frame_prefix(
        readers.readers(),
        readers.scan_config(0, 0, 0, 64 * 1024 * 1024).unwrap(),
        65536,
        deadline,
    )
    .await
    .unwrap();
    assert_eq!(selected.healthy_replicas(), 5);
    assert_eq!(selected.observation().summary().physical_end, layout.body_length);
}

#[tokio::test]
async fn colocated_replicas_require_disjoint_allocations() {
    let layout = Layout::new();
    for offset in [0, 1, 2] {
        let mut chunk = layout.chunk.clone();
        for strip in &mut chunk.strips {
            let Some(Strip::MirrorStrip(mirror)) = &mut strip.strip else {
                panic!("mirror missing")
            };
            mirror.segments[1].disk_id = mirror.segments[0].disk_id;
            mirror.segments[1].unit_offset = mirror.segments[0].unit_offset + offset;
        }
        let result = MirroredFrameReaders::new(
            Arc::new(chunk),
            Layout::NAME,
            layout.disks.clone(),
            Instant::now() + Duration::from_secs(5),
        );
        assert_eq!(result.is_ok(), offset == 2, "replica unit offset {offset}");
    }
    assert_eq!(layout.disks.calls.load(Ordering::Relaxed), 0);
}
