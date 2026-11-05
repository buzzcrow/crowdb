// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv::common::config::PaxosKvConfig;
use crowdb_tree_ffi::{Config, Crowdbtree, CtError};

const CANONICAL: &str = include_str!("../conf/crowdb_paxos_kv_server_config.toml");

fn load(text: &str) -> Result<PaxosKvConfig, String> {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-config");
    let path = root.path().join("server.toml");
    std::fs::write(&path, text).unwrap();
    PaxosKvConfig::load_from_file(&path)
}

#[test]
fn canonical_config_preserves_engine_and_runtime_defaults() {
    let config = load(CANONICAL).unwrap();
    let defaults = PaxosKvConfig::default();
    assert_eq!(
        serde_json::to_value(&config.tree).unwrap(),
        serde_json::to_value(&defaults.tree).unwrap()
    );
    let host = config.tree_runtime.runtime_options();
    let expected = defaults.tree_runtime.runtime_options();
    assert_eq!(host.workers, expected.workers);
    assert_eq!(host.backend_workers, expected.backend_workers);
    assert_eq!(host.backend_queue_limit, expected.backend_queue_limit);
    assert_eq!(host.limits.reclaim_bytes, expected.limits.reclaim_bytes);
    assert_eq!(
        config.tree_runtime.binding_options().backend_queue_limit,
        defaults.tree_runtime.binding_options().backend_queue_limit
    );
    assert!(config.wal_early_ack);
    assert!(config.async_engine_apply);
}

#[test]
fn toml_key_limit_is_enforced_by_native_admission() {
    let config = load(&CANONICAL.replace("max_key_size = 0", "max_key_size = 4")).unwrap();
    let mut options = Config::default();
    config.tree.apply_to(&mut options);
    let tree = Crowdbtree::open(&options).unwrap();
    assert_eq!(tree.put(b"abcde", b"value"), Err(CtError::InvalidArgument));
    tree.put(b"abcd", b"value").unwrap();
    assert_eq!(
        tree.get(b"abcd").unwrap().map(|(_, value)| value),
        Some(b"value".to_vec())
    );
}

#[test]
fn enlarged_source_capacity_is_included_in_scan_admission() {
    let config =
        load(&CANONICAL.replace("source_max_entries = 4_096", "source_max_entries = 8_192")).unwrap();
    let mut options = Config::default();
    config.tree.apply_to(&mut options);
    let tree = Crowdbtree::open(&options).unwrap();
    for index in 0..4200 {
        tree.put(format!("key{index:05}").as_bytes(), b"value").unwrap();
    }
    assert_eq!(tree.frozen_table_count(), 0);
    let (entries, truncated) = tree.scan(b"", b"", b"", 0, 1024 * 1024, true, 0, false).unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 4200);
    assert_eq!(entries.first().unwrap().key.as_ref(), b"key00000");
    assert_eq!(entries.last().unwrap().key.as_ref(), b"key04199");
}

#[test]
fn incompatible_or_unknown_work_windows_are_rejected_on_load() {
    for (input, field) in [
        (
            CANONICAL.replace("flush_parallelism = 0", "flush_parallelism = 5"),
            "flush_parallelism",
        ),
        (
            CANONICAL.replace("cache_max_pages = 256", "cache_max_pages = 0"),
            "cache_max_pages",
        ),
        (
            CANONICAL.replace("source_max_gap_entries = 64", "source_max_gap_entries = 16"),
            "coalesce_max_keys",
        ),
        (
            CANONICAL.replace("run_max_entries = 128", "run_entries_typo = 128"),
            "run_entries_typo",
        ),
    ] {
        assert!(load(&input).unwrap_err().contains(field), "{field}");
    }
}

#[test]
fn partial_tables_keep_omitted_defaults() {
    let config = load("[paxos]\ncoalesce_max_keys = 16\n[election]\nmaintenance_tick_ms = 3000\n[wal]\nwal_flush_batch_bytes = 131072\n").unwrap();
    assert_eq!(config.paxos.coalesce_max_keys, 16);
    assert_eq!(config.paxos.max_inflight_proposals, 32);
    assert_eq!(config.election.snapshot_time_threshold_ms, 600_000);
    assert_eq!(config.wal.wal_segment_size, 64 * 1024 * 1024);
    assert!(config.wal_early_ack);
    assert!(config.async_engine_apply);
}
