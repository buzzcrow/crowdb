// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{Config, Crowdbtree, TreeRuntime, TreeRuntimeOptions, TreeWorkStage};
use std::sync::Arc;

#[test]
fn selected_tree_stage_has_one_paid_histogram_and_immutable_collector_fanout() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let tree = Crowdbtree::open(&Config {
        runtime: Some(Arc::clone(&runtime)),
        ..Default::default()
    })
    .unwrap();
    let other = Crowdbtree::open(&Config {
        runtime: Some(Arc::clone(&runtime)),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(tree.work_histogram_totals().unwrap().enabled_stages, 0);
    tree.enable_work_histogram(TreeWorkStage::FlushBatch).unwrap();
    tree.enable_work_histogram(TreeWorkStage::FlushBatch).unwrap();
    assert_eq!(other.work_histogram_totals().unwrap().enabled_stages, 0);
    let collector = tree.work_metrics_collector().unwrap();
    tree.apply_put(1, b"key", b"value").unwrap();
    tree.flush().unwrap();
    let totals = tree.work_histogram_totals().unwrap();
    let stage = totals.stage(TreeWorkStage::FlushBatch).unwrap();
    assert_eq!(stage.count, 1);
    assert!(stage.sum_ns > 0);
    assert!(stage.p50_ns <= stage.p95_ns && stage.p95_ns <= stage.p99_ns);
    assert!(stage.p99_ns <= stage.max_ns);
    assert!(totals.stage(TreeWorkStage::PageLoadPipeline).is_none());
    let first = Arc::new(collector.capture().unwrap());
    let fanout = Arc::clone(&first);
    assert_eq!(first.window_sequence, first.histograms.window_sequence);
    assert_eq!(
        first.histograms.stage(TreeWorkStage::FlushBatch).unwrap().count,
        1
    );
    let empty = collector.capture().unwrap();
    assert_eq!(
        empty.histograms.stage(TreeWorkStage::FlushBatch).unwrap().count,
        0
    );
    assert_eq!(
        empty
            .histograms
            .stage(TreeWorkStage::FlushBatch)
            .unwrap()
            .total_count,
        1
    );
    drop(other);
    drop(tree);
    let final_window = collector.capture().unwrap();
    assert_eq!(
        final_window
            .histograms
            .stage(TreeWorkStage::FlushBatch)
            .unwrap()
            .total_count,
        1
    );
    assert!(Arc::ptr_eq(&first, &fanout));
    assert_eq!(
        fanout.histograms.stage(TreeWorkStage::FlushBatch).unwrap().count,
        1
    );
}
