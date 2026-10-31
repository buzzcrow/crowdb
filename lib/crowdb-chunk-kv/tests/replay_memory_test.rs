// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, ChunkKvError, Partition, PartitionConfig, PartitionId, PartitionRange,
};

#[path = "common/generated_journal.rs"]
mod journal;

async fn recover(count: u64, duplicate: Option<bool>) -> crowdb_chunk_kv::Result<Partition> {
    recover_journal(journal::TestGeneratedJournal::new(count, duplicate, false)).await
}

async fn recover_journal(journal: journal::TestGeneratedJournal) -> crowdb_chunk_kv::Result<Partition> {
    let journal = Arc::new(journal);
    Partition::recover(
        PartitionId { high: 1, low: 1 },
        PartitionRange::default(),
        1,
        journal.checkpoint(),
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::default()),
        journal,
    )
    .await
}

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
#[tokio::test]
async fn replay_does_not_retain_half_a_gibibyte_of_overwritten_payloads() {
    let before = peak_rss_kib();
    let partition = recover(8192, None).await.unwrap();
    let peak_growth = peak_rss_kib().saturating_sub(before);
    println!("WAL replay peak RSS growth: {peak_growth} KiB for 512 MiB of payloads");
    assert!(
        peak_growth < 64 * 1024,
        "replay retained payload history: {peak_growth} KiB"
    );
    let value = partition.get(1, b"overwritten", None).await.unwrap().unwrap();
    assert_eq!(value.revision, 8192);
    assert_eq!(value.value, vec![(8192 % 251) as u8; 65536]);
}

#[tokio::test]
async fn a_late_exact_duplicate_remains_valid() {
    let partition = recover(32, Some(false)).await.unwrap();
    assert_eq!(partition.snapshot().applied_seq, 32);
}

#[tokio::test]
async fn a_late_conflicting_duplicate_is_still_corruption() {
    let result = recover(32, Some(true)).await;
    assert!(matches!(result, Err(ChunkKvError::JournalCorruption(message))
        if message == "conflicting duplicate mutation sequence"));
}

#[tokio::test]
async fn identical_operations_with_changed_request_identity_are_conflicting_duplicates() {
    let journal = journal::TestGeneratedJournal::new(32, Some(false), true);
    let result = recover_journal(journal).await;
    assert!(matches!(result, Err(ChunkKvError::JournalCorruption(message))
        if message == "conflicting duplicate mutation sequence"));
}
