// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::sync::Arc;

use crowdb_chunk_kv_server::ChunkKvServerConfig;
use crowdb_tree_ffi::{Crowdbtree, CtError, TreeRuntime};

const CANONICAL: &str = include_str!("../conf/crowdb_chunk_kv_server_config.toml");
const PAXOS: &str = include_str!("../../crowdb-paxos-kv-server/conf/crowdb_paxos_kv_server_config.toml");

fn load(text: &str) -> Result<ChunkKvServerConfig, String> {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("chunk-tree-config");
    let path = root.path().join("server.toml");
    std::fs::write(&path, text).unwrap();
    ChunkKvServerConfig::load(&path).map_err(|error| error.to_string())
}

#[test]
fn canonical_config_loads_all_service_defaults() {
    let config = load(CANONICAL).unwrap();
    assert_eq!(
        config,
        ChunkKvServerConfig {
            instance_id: 1,
            tree_runtime: config.tree_runtime.clone(),
            ..Default::default()
        }
    );
    let expected = ChunkKvServerConfig::default().tree_runtime.runtime_options();
    let host = config.tree_runtime.runtime_options();
    assert_eq!(host.workers, expected.workers);
    assert_eq!(host.backend_workers, expected.backend_workers);
    assert_eq!(host.limits, expected.limits);
}

fn field_paths(value: &toml::Value, prefix: &str, paths: &mut BTreeSet<String>) {
    if let Some(table) = value.as_table() {
        for (key, child) in table {
            field_paths(child, &format!("{prefix}.{key}"), paths);
        }
    } else {
        paths.insert(prefix.to_owned());
    }
}

#[test]
fn independent_server_files_keep_the_same_tree_fields() {
    let chunk: toml::Value = toml::from_str(CANONICAL).unwrap();
    let paxos: toml::Value = toml::from_str(PAXOS).unwrap();
    for section in ["tree", "tree_runtime"] {
        let mut chunk_fields = BTreeSet::new();
        let mut paxos_fields = BTreeSet::new();
        field_paths(&chunk[section], section, &mut chunk_fields);
        field_paths(&paxos[section], section, &mut paxos_fields);
        assert_eq!(chunk_fields, paxos_fields, "{section}");
    }
}

#[test]
fn configured_native_engine_and_shared_runtime_are_used() {
    let mut config = load(&CANONICAL.replace("max_key_size = 0", "max_key_size = 4")).unwrap();
    config.tree_runtime.host.workers = Some(2);
    config.tree_runtime.host.backend_workers = Some(1);
    config.tree_runtime.tree.cpu_queue_limit = Some(32);
    config.tree.tuning.flush_parallelism = 2;
    config.validate().unwrap();
    let runtime = Arc::new(TreeRuntime::new(config.tree_runtime.runtime_options()).unwrap());
    let mut options = config.native_tree_options();
    assert_eq!(options.runtime_binding.cpu_queue_limit, 32);
    options.runtime = Some(runtime.clone());
    let first = Crowdbtree::open(&options).unwrap();
    let second = Crowdbtree::open(&options).unwrap();
    assert_eq!(runtime.snapshot().unwrap().bindings, 2);
    for tree in [&first, &second] {
        assert_eq!(tree.put(b"abcde", b"value"), Err(CtError::InvalidArgument));
        tree.put(b"abcd", b"value").unwrap();
        assert_eq!(tree.get(b"abcd").unwrap().unwrap().1, b"value");
    }
}

#[test]
fn configured_chunk_tree_survives_range_rebuild() {
    use crowdb_tree_ffi::chunk::test_util::TestChunkBackend;

    let config = load(&CANONICAL.replace("max_key_size = 0", "max_key_size = 4")).unwrap();
    let source_backend = TestChunkBackend::new(84, 1).unwrap();
    let child_backend = TestChunkBackend::new(85, 1).unwrap();
    let source_options = crowdb_tree_ffi::Config {
        runtime: Some(source_backend.runtime.clone()),
        page_store: Some(source_backend.page_store(0).unwrap()),
        ..config.native_tree_options()
    };
    let source = Crowdbtree::open(&source_options).unwrap();
    source.put(b"abcd", b"value").unwrap();
    let destination = crowdb_tree_ffi::Config {
        runtime: Some(child_backend.runtime.clone()),
        page_store: Some(child_backend.page_store(0).unwrap()),
        ..config.native_tree_options()
    };
    let (child, stats) = source.rebuild_range(&destination).unwrap();
    assert_eq!(stats.entries_emitted, 1);
    assert_eq!(child.get(b"abcd").unwrap().unwrap().1, b"value");
    assert_eq!(child.put(b"abcde", b"value"), Err(CtError::InvalidArgument));
}

#[test]
fn chunk_backend_alignment_is_validated_before_open() {
    let mut config = load(CANONICAL).unwrap();
    config.tree.frame_bytes = 16 * 1024;
    assert!(config
        .validate()
        .unwrap_err()
        .to_string()
        .contains("Chunk backend IU"));
    config.tree.iu_size = 4096;
    config.validate().unwrap();
    assert_eq!(config.native_tree_options().iu_size, 4096);
}

#[test]
fn invalid_runtime_engine_and_partition_bounds_fail_on_load() {
    for (from, to, field) in [
        (
            "flush_parallelism = 0",
            "flush_parallelism = 5",
            "flush_parallelism",
        ),
        ("cache_max_pages = 256", "cache_max_pages = 0", "cache_max_pages"),
        ("queue_requests = 1_024", "queue_requests = 0", "partition bounds"),
        ("batch_bytes = 4_194_304", "batch_bytes = 0", "partition bounds"),
        ("backend_workers = 2", "backend_workers = 0", "backend workers"),
        (
            "run_max_entries = 128",
            "run_entries_typo = 128",
            "run_entries_typo",
        ),
        ("queue_requests = 1_024", "queue_typo = 1_024", "queue_typo"),
    ] {
        assert!(
            load(&CANONICAL.replace(from, to)).unwrap_err().contains(field),
            "{field}"
        );
    }
}

#[test]
fn partial_partition_table_keeps_native_defaults() {
    let config = load("instance_id = 1\n[partition]\nbatch_requests = 16\n").unwrap();
    assert_eq!(config.partition.batch_requests, 16);
    assert_eq!(config.partition.queue_requests, 1024);
    assert_eq!(config.partition.max_value_bytes, 16 * 1024 * 1024);
}

#[test]
fn commented_optional_examples_are_loadable() {
    let start = CANONICAL.find("# [bootstrap_partition]").unwrap();
    let end = CANONICAL.find("# Keep Tree field additions").unwrap();
    let examples = CANONICAL[start..end]
        .lines()
        .filter_map(|line| line.strip_prefix("# "))
        .filter(|line| line.starts_with('[') || line.contains(" = "))
        .collect::<Vec<_>>()
        .join("\n");
    let config = load(&format!("{CANONICAL}\n{examples}")).unwrap();
    assert_eq!(config.bootstrap_partition.unwrap().tree_id, 1);
    let mut rdma = config.rpc_rdma.unwrap();
    assert_eq!(rdma.control_data_capacity(), Some(262_144));
    // Exercise explicit server configuration without requiring an RDMA device.
    rdma.enabled = false;
    crowdb_chunk_kv_server::ChunkKvRpcService::create_server(1, Some(&rdma)).unwrap();
}
