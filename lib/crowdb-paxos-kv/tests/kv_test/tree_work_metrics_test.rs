// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv::kv::{CrowdbTreeConfig, CrowdbTreeEngine};
use crowdb_tree_ffi::{TreeRuntime, TreeRuntimeOptions, TreeWorkStage};
use std::sync::Arc;

#[test]
fn logging_and_diagnostics_share_the_host_collector_window() {
    let engine = CrowdbTreeEngine::open(&CrowdbTreeConfig {
        runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
        ..Default::default()
    })
    .unwrap();
    let tree = engine.handle();
    tree.enable_work_histogram(TreeWorkStage::FlushBatch).unwrap();
    tree.apply_put(1, b"key", b"value").unwrap();
    tree.flush().unwrap();
    let window = engine.capture_work_metrics().unwrap().unwrap();
    let first = engine.format_work_metrics_window("s.7.g.9", "now");
    assert!(first.contains("s.7.g.9.tree.work.flush.batch"));
    assert!(first.contains("window=1"));
    let histogram = window.histograms.stage(TreeWorkStage::FlushBatch).unwrap();
    assert_eq!(histogram.count, 1);
    assert_eq!(window.histograms.window_sequence, window.window_sequence);
    assert!(first.contains("s.7.g.9.tree.histogram.flush.batch"));
    assert!(first.contains(&format!("p50_ns={}", histogram.p50_ns)));
    assert!(first.contains(&format!("p95_ns={}", histogram.p95_ns)));
    assert!(first.contains(&format!("p99_ns={}", histogram.p99_ns)));
    assert!(!first.contains("tree.histogram.page.load"));
    assert!(window.age_sampled_at_ns > 0);
    assert!(first.contains(&format!("age_sampled_at_ns={}", window.age_sampled_at_ns)));
    assert!(first.contains("age_tracked="));
    assert!(first.contains("age_untracked="));
    assert!(first.contains("observed_oldest_age_ns="));
    assert!(first.contains("age_complete="));
    let diagnostics = engine.work_metrics_window().unwrap();
    assert!(Arc::ptr_eq(&window, &diagnostics));
    let second = engine.format_work_metrics_window("s.7.g.9", "now");
    assert_eq!(first, second);
    let next = engine.capture_work_metrics().unwrap().unwrap();
    assert_eq!(next.window_sequence, 2);
    assert_eq!(next.histograms.stage(TreeWorkStage::FlushBatch).unwrap().count, 0);
    assert_eq!(
        window.histograms.stage(TreeWorkStage::FlushBatch).unwrap().count,
        1
    );
    assert!(Arc::ptr_eq(&window, &diagnostics));
}
