// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::client_data::fixture_client;
use crowdb_kv_client::ReadMode;

const STORE_ID: u64 = 1;
const GROUP_ID: u64 = 1;

use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap()
}

#[tokio::test]
#[ignore = "requires the owned release-container KV client profile"]
async fn scan_keys_only_and_scan_count() {
    let client = fixture_client().await;

    for i in 0..12u32 {
        client
            .put(STORE_ID, GROUP_ID, format!("k{i:02}").as_bytes(), b"v", None)
            .await
            .expect("put");
    }

    // keys_only: same keys as a full scan, all values empty.
    let keys_out = client
        .scan(
            STORE_ID,
            GROUP_ID,
            b"k",
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
            true,
            None,
        )
        .await
        .expect("keys_only scan");
    assert_eq!(keys_out.items.len(), 12, "all 12 keys returned");
    assert!(
        keys_out.items.iter().all(|(_, v)| v.is_empty()),
        "keys_only values empty"
    );
    for (i, (key, _)) in keys_out.items.iter().enumerate() {
        assert_eq!(key.as_ref(), format!("k{i:02}").as_bytes(), "key {i} in order");
    }

    // scan_count: returns the total count, no items shipped.
    let count = client
        .scan_count(
            STORE_ID,
            GROUP_ID,
            b"k",
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
            None,
        )
        .await
        .expect("scan_count");
    assert_eq!(count, 12, "scan_count returns the live matching key count");

    // scan_count with prefix that matches nothing.
    let count = client
        .scan_count(
            STORE_ID,
            GROUP_ID,
            b"nonexistent",
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
            None,
        )
        .await
        .expect("scan_count empty");
    assert_eq!(count, 0, "scan_count returns 0 for no matches");

    // scan_count with limit caps the count.
    let count = client
        .scan_count(
            STORE_ID,
            GROUP_ID,
            b"k",
            &[],
            &[],
            5,
            ReadMode::Linearizable,
            None,
            None,
        )
        .await
        .expect("scan_count limit");
    assert_eq!(count, 5, "scan_count respects limit");
}

#[tokio::test]
#[ignore = "requires the owned release-container KV client profile"]
async fn scan_deadline_returns_partial_result() {
    let client = fixture_client().await;

    // Populate enough keys that the engine merge loop's periodic deadline
    // check (every 1024 entries) fires mid-scan.
    let items: Vec<_> = (0..3000u64)
        .map(|i| crowdb_kv_client::BatchOp::Put {
            key: bytes::Bytes::from(format!("k{i:05}")),
            value: bytes::Bytes::from_static(b"v"),
        })
        .collect();
    client
        .batch_write(STORE_ID, GROUP_ID, &items)
        .await
        .expect("populate scan");

    // No deadline: full scan returns all 3000 entries.
    let full = client
        .scan(
            STORE_ID,
            GROUP_ID,
            b"",
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .expect("full scan");
    assert_eq!(full.items.len(), 3000);
    assert!(!full.truncated);
    assert!(!full.timed_out);
    for (i, (key, value)) in full.items.iter().enumerate() {
        assert_eq!(key.as_ref(), format!("k{i:05}").as_bytes());
        assert_eq!(value.as_ref(), b"v");
    }

    // Tight deadline (already expired): the scan returns a partial result
    // with truncated = true and timed_out = true.
    let deadline = now_ms();
    let partial = client
        .scan(
            STORE_ID,
            GROUP_ID,
            b"",
            &[],
            &[],
            0,
            ReadMode::Linearizable,
            None,
            false,
            Some(deadline),
        )
        .await
        .expect("deadline scan");
    assert!(partial.truncated);
    assert!(partial.timed_out);
    assert!(partial.items.len() < full.items.len());
    // The partial result is a correctly-ordered prefix of the full scan.
    for (i, (k, _)) in partial.items.iter().enumerate() {
        assert_eq!(k, &full.items[i].0, "partial result must be a prefix");
    }
}
