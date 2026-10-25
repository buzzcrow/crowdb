// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "../common/frame_revision.rs"]
mod fixture;

use bytes::Bytes;
use crowdb_protocol::frame::{FrameMagic, MAX_FRAME_BYTES, MAX_FRAME_V2_PAYLOAD_BYTES};
use fixture::TestFramedReader;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn ordinary_ranges_cross_frames_and_objects_without_payload_copies() {
    let old = Bytes::from(vec![0x35; 71]);
    let payload = Bytes::from(
        (0..2 * MAX_FRAME_V2_PAYLOAD_BYTES + 17)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>(),
    );
    let fixture = TestFramedReader::new(
        &[
            (FrameMagic::RepoSmallV2, old.clone()),
            (FrameMagic::RepoLargeV2, payload.clone()),
        ],
        1,
    );
    let start = old.len() as u64 + MAX_FRAME_V2_PAYLOAD_BYTES as u64 - 1;
    let end = start + 9;
    let actual = fixture
        .reader
        .read_range(&fixture.locations, start, end)
        .await
        .unwrap();
    assert_eq!(
        actual.concat(),
        payload[MAX_FRAME_V2_PAYLOAD_BYTES - 1..MAX_FRAME_V2_PAYLOAD_BYTES + 8]
    );
    let mut stream = fixture.reader.read_stream(&fixture.locations).unwrap();
    let mut actual = Vec::new();
    while let Some(part) = stream.next_chunk().await {
        actual.extend_from_slice(&part.unwrap());
    }
    assert_eq!(actual, [old.as_ref(), payload.as_ref()].concat());
    let metrics = fixture.reader.flow_metrics_snapshot();
    assert!(metrics.stream_windows >= 4);
    assert!(fixture.reads.load(Ordering::Relaxed) >= 4);
    assert!(fixture.queries() <= 3);
}

#[tokio::test]
async fn v2_small_frames_and_short_final_frames_work_under_one_byte_windows() {
    for length in [1, MAX_FRAME_V2_PAYLOAD_BYTES, MAX_FRAME_V2_PAYLOAD_BYTES + 1] {
        let payload = Bytes::from(vec![0x47; length]);
        let fixture = TestFramedReader::new(&[(FrameMagic::RepoSmallV2, payload.clone())], 1);
        let mut stream = fixture.reader.read_stream(&fixture.locations).unwrap();
        let mut actual = Vec::new();
        while let Some(part) = stream.next_chunk().await {
            actual.extend_from_slice(&part.unwrap());
        }
        assert_eq!(actual, payload);
        assert_eq!(fixture.queries(), 1);
    }
}

#[tokio::test]
async fn location_geometry_must_match_the_verified_revision() {
    let mut fixture = TestFramedReader::new(
        &[(FrameMagic::RepoLargeV2, Bytes::from_static(b"payload"))],
        MAX_FRAME_BYTES,
    );
    fixture.locations[0].length -= 1;
    assert!(fixture.reader.read_object(&fixture.locations).await.is_err());
    let mut fixture = TestFramedReader::new(
        &[(FrameMagic::RepoLargeV2, Bytes::from_static(b"payload"))],
        MAX_FRAME_BYTES,
    );
    fixture.locations[0].length += 1;
    assert!(fixture.reader.read_object(&fixture.locations).await.is_err());
}
