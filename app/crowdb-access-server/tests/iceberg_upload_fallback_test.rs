// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/s3_fallback.rs"]
#[allow(dead_code)] // Shares Body, writer and owner fixtures without the S3 checksum wrapper.
mod body;
#[path = "common/iceberg_fallback.rs"]
mod common;
#[path = "iceberg_upload_fallback/failure.rs"]
mod failure;

use common::{checked, TARGET};
use hyper::body::Bytes;

#[tokio::test]
async fn known_small_and_one_complete_target_retain_original_owners() {
    for length in [1, 4096, TARGET - 1, TARGET] {
        let original = Bytes::from(vec![37; length]);
        let (writer, _) = checked(vec![original.clone()], Some(length)).await;
        assert_eq!(writer.buffers.len(), 1);
        assert_eq!(
            writer.buffers[0].as_ptr(),
            original.as_ptr(),
            "full target was copied: {length}"
        );
    }
}

#[tokio::test]
async fn unknown_target_boundaries_copy_only_incomplete_tail_views() {
    for length in [TARGET - 1, TARGET, TARGET + 1, 1024 * 1024] {
        let original = Bytes::from(vec![41; length]);
        let (writer, _) = checked(vec![original.clone()], None).await;
        let expected = if length <= TARGET {
            vec![length]
        } else {
            vec![TARGET, length - TARGET]
        };
        assert_eq!(
            writer.buffers.iter().map(Bytes::len).collect::<Vec<_>>(),
            expected
        );
        if length >= TARGET {
            assert_eq!(writer.buffers[0].as_ptr(), original.as_ptr());
        }
        if length != TARGET {
            let offset = if length < TARGET { 0 } else { TARGET };
            assert_ne!(
                writer.buffers.last().unwrap().as_ptr(),
                original.slice(offset..).as_ptr()
            );
        }
    }
}

#[tokio::test]
async fn aligned_full_views_keep_their_owners_and_empty_inputs_add_no_feeds() {
    let original = Bytes::from(vec![59; 4 * TARGET + 17]);
    let frames = (0..original.len())
        .step_by(TARGET)
        .map(|start| original.slice(start..(start + TARGET).min(original.len())))
        .collect();
    let (writer, _) = checked(frames, None).await;
    assert_eq!(
        writer.buffers.iter().map(Bytes::len).collect::<Vec<_>>(),
        [TARGET, TARGET, TARGET, TARGET, 17]
    );
    for (index, buffer) in writer.buffers[..4].iter().enumerate() {
        assert_eq!(buffer.as_ptr(), original.slice(index * TARGET..).as_ptr());
    }
    assert_ne!(writer.buffers[4].as_ptr(), original.slice(4 * TARGET..).as_ptr());
    for declared in [None, Some(0)] {
        let (writer, metrics) = checked(vec![Bytes::new(), Bytes::new()], declared).await;
        assert!(writer.buffers.is_empty());
        assert_eq!(metrics.digest_enqueues, 0);
    }
}

#[tokio::test]
async fn a_four_mib_body_keeps_sixteen_frame_groups_across_non_aligned_input_windows() {
    let original = Bytes::from(vec![73; (4 << 20) + 17]);
    for declared in [None, Some(original.len())] {
        let (writer, metrics) = checked(vec![original.clone()], declared).await;
        assert_eq!(
            writer.buffers.iter().map(Bytes::len).collect::<Vec<_>>(),
            [TARGET, TARGET, TARGET, TARGET, 2193]
        );
        assert_eq!(writer.buffers[0].as_ptr(), original.as_ptr());
        for (index, buffer) in writer.buffers.iter().enumerate().skip(1) {
            assert_ne!(buffer.as_ptr(), original.slice(index * TARGET..).as_ptr());
        }
        assert_eq!(metrics.body_frames, 5);
        assert_eq!(metrics.digest_enqueues, 5);
    }
}

#[tokio::test]
async fn fragmented_payload_is_copied_into_the_same_target_groups() {
    let original = Bytes::from(vec![83; 2 * TARGET + 37]);
    let frames = (0..original.len())
        .step_by(997)
        .map(|start| original.slice(start..(start + 997).min(original.len())))
        .collect();
    let (writer, _) = checked(frames, None).await;
    assert_eq!(
        writer.buffers.iter().map(Bytes::len).collect::<Vec<_>>(),
        [TARGET, TARGET, 37]
    );
    for (index, buffer) in writer.buffers.iter().enumerate() {
        assert_ne!(buffer.as_ptr(), original.slice(index * TARGET..).as_ptr());
    }
}

#[tokio::test]
async fn coalesced_fragments_do_not_prevent_a_later_aligned_owner_handoff() {
    let first = Bytes::from(vec![17; TARGET - 17]);
    let direct = Bytes::from(vec![59; TARGET]);
    let (writer, _) = checked(
        vec![
            first.clone(),
            Bytes::from(vec![29; 34]),
            Bytes::from(vec![37; TARGET - 17]),
            Bytes::new(),
            direct.clone(),
        ],
        None,
    )
    .await;
    assert_eq!(
        writer.buffers.iter().map(Bytes::len).collect::<Vec<_>>(),
        [TARGET, TARGET, TARGET]
    );
    assert_ne!(writer.buffers[0].as_ptr(), first.as_ptr());
    assert_eq!(writer.buffers[2].as_ptr(), direct.as_ptr());
}
