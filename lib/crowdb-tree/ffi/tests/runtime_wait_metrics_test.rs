// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{Config, Crowdbtree};
#[test]
fn wait_totals_are_non_draining_and_collector_outlives_tree() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    let collector = tree.work_metrics_collector().unwrap();
    let initial = tree.wait_metrics_totals().unwrap();
    assert_eq!(initial.classes.len(), 7);
    assert!(initial.classes.iter().all(|value| value.age.complete));
    let first = collector.capture().unwrap();
    let read = collector.wait_metrics_totals().unwrap();
    let second = collector.capture().unwrap();
    assert_eq!(second.window_sequence, first.window_sequence + 1);
    assert_eq!(read.classes, second.waits.classes);
    drop(tree);
    let after_close = collector.wait_metrics_totals().unwrap();
    assert!(after_close.classes.iter().all(|value| value.inflight == 0));
}
