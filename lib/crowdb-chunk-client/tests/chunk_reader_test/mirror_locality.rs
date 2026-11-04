// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::mirror_locality_fixture::{fixture, read};

#[tokio::test]
async fn mirror_reads_prefer_the_requested_node_without_changing_metadata() {
    let (reader, disks, allocator, frame) = fixture(Some(3), None, None);
    read(&reader, &frame).await;
    assert_eq!(*disks.served.lock().unwrap(), vec![3]);
    assert_eq!(
        allocator.current.lock().unwrap().strips[0].strip,
        allocator.first.strips[0].strip
    );
}

#[tokio::test]
async fn unknown_node_or_incomplete_hints_preserve_every_supplied_replica() {
    for (preferred, missing) in [(None, None), (Some(99), None), (Some(3), Some(1))] {
        let (reader, disks, _, frame) = fixture(preferred, missing, None);
        read(&reader, &frame).await;
        assert_eq!(*disks.served.lock().unwrap(), vec![1]);
    }
}

#[tokio::test]
async fn corrupt_preferred_mirror_falls_back_within_its_rack_after_reporting() {
    let (reader, disks, allocator, frame) = fixture(Some(3), None, Some(3));
    read(&reader, &frame).await;
    assert_eq!(*disks.served.lock().unwrap(), vec![3, 2]);
    assert_eq!(
        allocator.current.lock().unwrap().strips[0].unavailable_segments,
        vec![segment(3)]
    );
}
