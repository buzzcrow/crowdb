// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::{Config, Crowdbtree, TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;

#[test]
fn dispatch_totals_follow_real_flushes_without_draining_collector_windows() {
    let tree = Crowdbtree::open(&Config {
        runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
        ..Default::default()
    })
    .unwrap();
    let collector = tree.work_metrics_collector().unwrap();
    assert_eq!(tree.flush_dispatch_stats().unwrap().prepared_entries, 0);
    tree.apply_put(1, b"key", b"first").unwrap();
    tree.flush().unwrap();
    let first = tree.flush_dispatch_stats().unwrap();
    assert_eq!(first.prepared_entries, 1);
    assert!(first.prepared_runs > 0);
    assert!(first.prepared_bytes >= 8);
    assert!(first.cow_bytes > 0);
    assert!(first.peak_steps > 0);
    assert!(first.active_steps <= first.peak_steps);
    let again = tree.flush_dispatch_stats().unwrap();
    assert_eq!(again.prepared_entries, first.prepared_entries);
    let window = collector.capture().unwrap();
    assert_eq!(window.window_sequence, 1);
    assert!(window.stages.iter().any(|stage| stage.window_count > 0));
    tree.apply_put(2, b"next", b"second").unwrap();
    tree.flush().unwrap();
    let second = tree.flush_dispatch_stats().unwrap();
    assert_eq!(second.prepared_entries, 2);
    assert!(second.cow_bytes > first.cow_bytes);
    assert!(second.sampled_at_ns >= first.sampled_at_ns);
    let standalone = Crowdbtree::open(&Config::default()).unwrap();
    standalone.apply_put(1, b"default", b"value").unwrap();
    standalone.flush().unwrap();
    assert_eq!(standalone.flush_dispatch_stats().unwrap().prepared_entries, 1);
}
