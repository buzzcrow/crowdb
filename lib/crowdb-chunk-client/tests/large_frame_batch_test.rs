// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/large_write_allocator.rs"]
mod allocator;
mod common;
#[path = "common/large_frame_batch.rs"]
mod frame_batch;

use std::time::Duration;

use bytes::Bytes;
use crowdb_chunk_client::ChunkIoWriter;
use crowdb_protocol::frame::{MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES};
use frame_batch::TestFrameBatch;

#[tokio::test]
async fn irregular_four_mib_input_counts_frame_copies_and_preserves_canonical_frames() {
    let length: usize = (4 << 20) + 17;
    let source = Bytes::from(
        (0..length)
            .map(|index| index.to_le_bytes()[0])
            .collect::<Vec<_>>(),
    );
    for (fragment_bytes, chunk_bytes) in [1 << 20, 17_003, MAX_FRAME_PAYLOAD_BYTES - 1]
        .into_iter()
        .flat_map(|fragment| [(fragment, 1 << 20), (fragment, 8 << 20)])
    {
        let fixture = TestFrameBatch::new();
        let mut writer = fixture.writer(chunk_bytes);
        let mut inputs = 0;
        for start in (0..length).step_by(fragment_bytes) {
            writer
                .on_data(source.slice(start..(start + fragment_bytes).min(length)))
                .await
                .unwrap();
            inputs += 1;
        }
        let locations = writer.on_finish().await.unwrap();
        let lengths = fixture.verify(&locations, &source);
        assert_eq!(lengths, [vec![MAX_FRAME_PAYLOAD_BYTES; 64], vec![2193]].concat());
        let tail_reencodes = u64::from(chunk_bytes == 1 << 20);
        assert_eq!(locations.len(), if tail_reencodes == 0 { 1 } else { 5 });
        let metrics = writer.buffer_metrics();
        assert_eq!(
            metrics.payload_copy_bytes,
            (2 * length) as u64 + tail_reencodes * 2193
        );
        assert_eq!(metrics.payload_copy_operations, inputs + 65 + tail_reencodes);
        assert_eq!(metrics.framed_owners, 0);
    }
}

#[tokio::test]
async fn empty_partial_and_exact_frame_boundaries_keep_their_copy_counts() {
    for length in [
        0,
        1,
        MAX_FRAME_PAYLOAD_BYTES - 1,
        MAX_FRAME_PAYLOAD_BYTES,
        MAX_FRAME_PAYLOAD_BYTES + 1,
    ] {
        let source = Bytes::from(vec![0x5a; length]);
        let fixture = TestFrameBatch::new();
        let mut writer = fixture.writer(1 << 20);
        for byte in source.chunks(19_919) {
            writer.on_data(Bytes::copy_from_slice(byte)).await.unwrap();
        }
        let locations = writer.on_finish().await.unwrap();
        let lengths = fixture.verify(&locations, &source);
        assert_eq!(lengths.len(), length.div_ceil(MAX_FRAME_PAYLOAD_BYTES));
        assert_eq!(writer.buffer_metrics().payload_copy_bytes, (2 * length) as u64);
    }
}

#[tokio::test]
async fn pending_storage_holds_the_encoded_batch_and_backpressures_the_writer() {
    let fixture = TestFrameBatch::new();
    fixture.disk.pause_next();
    let source = Bytes::from(vec![0x74; 2 * MAX_FRAME_PAYLOAD_BYTES + 17]);
    let expected = source.clone();
    let mut writer = fixture.writer(MAX_FRAME_BYTES as u64);
    let mut transfer = tokio::spawn(async move {
        writer.on_data(source).await.unwrap();
        let locations = writer.on_finish().await.unwrap();
        (locations, writer.buffer_metrics())
    });
    let held = tokio::time::timeout(Duration::from_secs(2), fixture.disk.wait_until_paused())
        .await
        .unwrap();
    assert_eq!(held.len(), 4096);
    let before = held.to_vec();
    assert!(tokio::time::timeout(Duration::from_millis(10), &mut transfer)
        .await
        .is_err());
    fixture.disk.resume();
    let (locations, metrics) = tokio::time::timeout(Duration::from_secs(2), transfer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held, before);
    assert_eq!(
        fixture.verify(&locations, &expected),
        [MAX_FRAME_PAYLOAD_BYTES, MAX_FRAME_PAYLOAD_BYTES, 17]
    );
    assert_eq!(metrics.payload_copy_bytes, (2 * expected.len()) as u64 + 17);
}
