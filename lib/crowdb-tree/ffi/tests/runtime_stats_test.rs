// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{Config, Crowdbtree, TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;

#[test]
fn publication_stats_are_constant_cost_and_account_local() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let config = Config {
        runtime: Some(runtime),
        ..Default::default()
    };
    let first = Crowdbtree::open(&config).unwrap();
    let second = Crowdbtree::open(&config).unwrap();
    let before = first.state_stats().unwrap();
    assert_eq!(before.logical.leaf_pages, 1);
    assert!(before.resources.allocated_bytes > 0);
    assert!(before.resources.reserved_bytes > 0);
    let progress = first.progress_stats().unwrap();
    assert_eq!(progress.instance_generation, before.instance_generation);
    assert_eq!(progress.commit_seq, before.commit_seq);
    assert!(progress.sampled_at_ns >= before.sampled_at_ns);
    assert_eq!(progress.candidate_oldest_age_ns, 0);
    assert_eq!(progress.checkpoint_oldest_pending_age_ns, 0);
    let second_before = second.state_stats().unwrap();
    let value = vec![b'v'; 64 * 1024];
    first.apply_put(1, b"key", &value).unwrap();
    // Resource fields are independent relaxed samples. Finish the host CPU
    // tails before comparing account ownership; initialization can return
    // unused metadata from allocated bytes to reserved bytes asynchronously.
    config.runtime.as_ref().unwrap().drain_cpu();
    let first_after = first.state_stats().unwrap();
    let second_after = second.state_stats().unwrap();
    assert!(first_after.resources.allocated_bytes > before.resources.allocated_bytes);
    assert!(
        first_after.resources.allocated_bytes > second_after.resources.allocated_bytes + value.len() as u64
    );
    assert_eq!(second_before.logical, second_after.logical);
    assert_eq!(second_before.commit_seq, second_after.commit_seq);
}

#[test]
fn standalone_stats_sample_the_same_runtime_state() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    let state = tree.state_stats().unwrap();
    assert_eq!(state.logical.leaf_pages, 1);
    assert!(state.resources.allocated_bytes > 0);
    let progress = tree.progress_stats().unwrap();
    assert_eq!(progress.instance_generation, state.instance_generation);
    assert_eq!(progress.commit_seq, state.commit_seq);
    tree.backing_stats().unwrap();
}
