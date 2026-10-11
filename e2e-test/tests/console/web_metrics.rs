// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::web_legacy::{bootstrap, get};

#[tokio::test]
#[ignore = "requires owned release KV and Web containers"]
async fn structured_metrics_preserve_node_group_and_store_scopes() {
    let (http, base) = bootstrap().await;
    let node = get(&http, &base, "/api/nodes/1/metrics").await;
    assert!(node["window_secs"].as_f64().is_some());
    assert!(node["timestamp"].is_string());
    let metrics = node["metrics"].as_array().unwrap();
    assert!(!metrics.is_empty(), "{node}");
    for metric in metrics {
        assert!(metric["name"].is_string());
        assert!(metric["kind"].is_string());
        assert!(metric["fields"].as_array().is_some());
    }
    for (path, prefix) in [
        ("/api/stores/0/groups/0/metrics", "s.0.g.0."),
        ("/api/stores/0/metrics", "s.0."),
    ] {
        let response = get(&http, &base, path).await;
        let metrics = response["metrics"].as_array().unwrap();
        assert!(!metrics.is_empty(), "{path}: {response}");
        for metric in metrics {
            let name = metric["name"].as_str().unwrap();
            assert!(name.starts_with(prefix), "{path}: unexpected metric {name}");
        }
    }
}
