// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

use std::sync::Arc;

use crowdb_chunk_kv::{CrowdbPartitionTree, Partition, PartitionConfig, PartitionId, PartitionRange};

#[path = "common/generated_journal.rs"]
mod journal;

fn peak_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn native_replay_drains_overwritten_memtables_without_publishing_a_checkpoint() {
    let journal = Arc::new(journal::TestGeneratedJournal::new(8192, None, false));
    let config = crowdb_tree_ffi::Config {
        page_store: Some(Arc::new(crowdb_tree_ffi::PageStore::open_mem(65536).unwrap())),
        ..crowdb_tree_ffi::Config::default()
    };
    let tree = Arc::new(CrowdbPartitionTree::open(1, &config).unwrap());
    let before = peak_rss_kib();
    let partition = Partition::recover(
        PartitionId { high: 1, low: 1 },
        PartitionRange::default(),
        1,
        journal.checkpoint(),
        PartitionConfig::default(),
        tree,
        journal,
    )
    .await
    .unwrap();
    let peak_growth = peak_rss_kib().saturating_sub(before);
    println!("Native tree replay peak RSS growth: {peak_growth} KiB for 512 MiB of overwritten values");
    assert!(
        peak_growth < 96 * 1024,
        "unflushed replay buffers retained {peak_growth} KiB"
    );
    let value = partition.get(1, b"overwritten", None).await.unwrap().unwrap();
    assert_eq!(value.revision, 8192);
    assert_eq!(value.value, vec![(8192 % 251) as u8; 65536]);
    let observed = partition.observe_tree().unwrap();
    assert_eq!(observed.checkpoint_manifest, 0);
    assert_eq!(observed.checkpoint_applied_seq, 0);
}
