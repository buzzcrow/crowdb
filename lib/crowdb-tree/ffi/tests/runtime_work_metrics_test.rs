// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::{
    Config, Crowdbtree, CtError, TreeRuntime, TreeRuntimeOptions, TREE_WORK_CAUSE_NAMES,
    TREE_WORK_COUNTER_NAMES,
};
use std::sync::Arc;

#[test]
fn one_collector_distributes_a_window_and_totals_never_drain_it() {
    let tree = Crowdbtree::open(&Config {
        runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
        ..Default::default()
    })
    .unwrap();
    let collector = tree.work_metrics_collector().unwrap();
    assert_eq!(tree.work_metrics_collector().unwrap_err(), CtError::Unavailable);
    tree.apply_put(1, b"key", b"value").unwrap();
    tree.flush().unwrap();
    let before = tree.work_metrics_totals().unwrap();
    let first = Arc::new(collector.capture().unwrap());
    let second_reader = Arc::clone(&first);
    let after = tree.work_metrics_totals().unwrap();
    assert_eq!(first.window_sequence, 1);
    let flush = TREE_WORK_CAUSE_NAMES
        .iter()
        .position(|name| *name == "flush")
        .unwrap();
    let values = TREE_WORK_COUNTER_NAMES
        .iter()
        .position(|name| *name == "flush.winning.value.bytes")
        .unwrap();
    assert_eq!(before.counters.values[flush][values], 5);
    assert_eq!(first.counters.values[flush][values], 5);
    assert_eq!(after.counters.values[flush][values], 5);
    assert!(first.counters.sampled_at_ns > 0);
    assert!(first.age_sampled_at_ns > 0);
    assert!(before.age_sampled_at_ns > 0);
    assert_eq!(first.stages[0].age.inflight, 0);
    assert_eq!(first.stages[0].age.tracked, 0);
    assert!(first.stages[0].age.complete);
    assert!(first.stages.iter().any(|stage| stage.window_count > 0));
    assert!(Arc::ptr_eq(&first, &second_reader));
    assert!(after
        .stages
        .iter()
        .zip(before.stages)
        .all(|(a, b)| a.total_count >= b.total_count));
    drop(tree);
    let final_window = collector.capture().unwrap();
    assert_eq!(final_window.window_sequence, 2);
    assert_eq!(final_window.counters.values[flush][values], 5);
    assert!(final_window.age_sampled_at_ns >= first.age_sampled_at_ns);
    assert_eq!(final_window.stages[0].age.untracked, 0);
    assert!(final_window.stages[0].age.complete);
    assert_eq!(first, second_reader);
}

#[test]
fn releasing_the_collector_allows_a_new_single_owner() {
    let tree = Crowdbtree::open(&Config {
        runtime: Some(Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())),
        ..Default::default()
    })
    .unwrap();
    drop(tree.work_metrics_collector().unwrap());
    assert!(tree.work_metrics_collector().is_ok());
    let standalone = Crowdbtree::open(&Config::default()).unwrap();
    let standalone_collector = standalone.work_metrics_collector().unwrap();
    assert_eq!(
        standalone.work_metrics_collector().unwrap_err(),
        CtError::Unavailable
    );
    drop(standalone_collector);
    assert!(standalone.work_metrics_collector().is_ok());
}
