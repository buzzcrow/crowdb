// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::http::{kv_get, kv_scan};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

async fn assert_kv_state(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    expected: &BTreeMap<String, Option<String>>,
) {
    for (key, value) in expected {
        let body = kv_get(client, base, store_id, group_id, key).await;
        match value {
            Some(expected_value) => {
                assert_eq!(
                    body["found"], true,
                    "expected {store_id}/{group_id}/{key} to exist: {body}"
                );
                assert_eq!(body["value_utf8"], expected_value.as_str());
            }
            None => {
                assert_eq!(
                    body["found"], false,
                    "expected {store_id}/{group_id}/{key} to be deleted: {body}"
                );
            }
        }
    }
}

pub(super) async fn wait_for_kv_state(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    expected: &BTreeMap<String, Option<String>>,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let (status, body) = kv_scan(client, base, store_id, group_id).await;
        if status != 200 {
            assert!(
                Instant::now() < deadline,
                "kv scan {store_id}/{group_id} failed (status {status}) within {timeout:?}: {body}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        let items = body["items"].as_array().cloned().unwrap_or_default();
        let mut actual: BTreeMap<String, String> = BTreeMap::new();
        for item in &items {
            if let (Some(k), Some(v)) = (item["key_utf8"].as_str(), item["value_utf8"].as_str()) {
                actual.insert(k.to_string(), v.to_string());
            }
        }
        let mut all_match = true;
        for (key, value) in expected {
            let matches = match value {
                Some(v) => actual.get(key).is_some_and(|av| av == v),
                None => !actual.contains_key(key),
            };
            if !matches {
                all_match = false;
                break;
            }
        }
        if all_match {
            return;
        }
        if Instant::now() >= deadline {
            assert_kv_state(client, base, store_id, group_id, expected).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
