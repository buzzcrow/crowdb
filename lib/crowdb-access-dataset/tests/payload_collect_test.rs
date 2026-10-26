// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/payload_collect.rs"]
mod payload_collect;
use crowdb_access_dataset::{collect_payload_for_tests, UploadError, MAX_UPLOAD_PART_BYTES};
use payload_collect::TestPayloadCollector;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn complete_single_buffer_moves_the_same_allocation_and_keeps_its_owner() {
    for length in [1, 513, 4096, 65_513] {
        let mut stream = TestPayloadCollector::new(vec![vec![7; length]]);
        let bytes = collect_payload_for_tests(&mut stream, length).await.unwrap();
        assert_eq!(bytes.as_ptr() as usize, stream.first_pointer);
        assert_eq!(bytes.as_ref(), vec![7; length]);
        assert_eq!(stream.pulls, 2, "successful EOF is required");
        assert_eq!(stream.active.load(Ordering::Relaxed), 1);
        let active = stream.active.clone();
        drop(stream);
        assert_eq!(active.load(Ordering::Relaxed), 1);
        drop(bytes);
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn fragmented_payload_releases_each_credit_before_fetching_the_next_buffer() {
    for lengths in [
        vec![1, 4096, 513],
        vec![65_502; 16],
        vec![1, MAX_UPLOAD_PART_BYTES - 1],
    ] {
        let chunks: Vec<_> = lengths
            .iter()
            .enumerate()
            .map(|(i, len)| vec![u8::try_from(i).unwrap(); *len])
            .collect();
        let expected = chunks.concat();
        let mut stream = TestPayloadCollector::new(chunks);
        let output = collect_payload_for_tests(&mut stream, expected.len())
            .await
            .unwrap();
        assert_eq!(output.as_ref(), expected);
        assert_ne!(output.as_ptr() as usize, stream.first_pointer);
        assert_eq!(stream.active.load(Ordering::Relaxed), 0);
        assert_eq!(stream.pulls, lengths.len() + 1);
    }
}

#[tokio::test]
async fn invalid_boundaries_and_late_eof_errors_never_return_success() {
    for (chunks, length) in [
        (vec![], 1),
        (vec![vec![]], 1),
        (vec![vec![1; 4]], 3),
        (vec![vec![1; 2]], 3),
        (vec![vec![1; 2], vec![]], 3),
        (vec![vec![1; 2], vec![2; 2]], 3),
        (vec![vec![1; 3], vec![2]], 3),
    ] {
        let mut stream = TestPayloadCollector::new(chunks);
        // These streams deliberately violate the declared logical object bounds.
        stream.enforce_credit = false;
        assert!(matches!(
            collect_payload_for_tests(&mut stream, length).await,
            Err(UploadError::Integrity)
        ));
        assert_eq!(stream.active.load(Ordering::Relaxed), 0);
    }
    for length in [0, MAX_UPLOAD_PART_BYTES + 1] {
        let mut stream = TestPayloadCollector::new(vec![vec![1]]);
        assert!(matches!(
            collect_payload_for_tests(&mut stream, length).await,
            Err(UploadError::Integrity)
        ));
        assert_eq!(stream.pulls, 0);
    }
    for chunks in [vec![vec![1; 3]], vec![vec![1], vec![2; 2]]] {
        let mut stream = TestPayloadCollector::new(chunks);
        stream.fail_eof = true;
        assert!(matches!(
            collect_payload_for_tests(&mut stream, 3).await,
            Err(UploadError::Storage(_))
        ));
        assert_eq!(stream.active.load(Ordering::Relaxed), 0);
    }
}
