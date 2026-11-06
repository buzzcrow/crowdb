// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv::wal::io_backend::IoBackend;
use crowdb_paxos_kv::wal::record::{RecordType, WALRecord};
use crowdb_paxos_kv::wal::segment::SegmentReader;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Summary of WAL contents for a single node/group.
#[derive(Debug)]
pub(super) struct WalSummary {
    accepted_count: usize,
    unique_slots: BTreeSet<u64>,
    pub(super) max_slot: u64,
}

impl WalSummary {
    fn contiguous_chosen(&self) -> u64 {
        let mut expected = 1u64;
        for slot in &self.unique_slots {
            if *slot == expected {
                expected += 1;
            } else {
                break;
            }
        }
        expected - 1
    }
}

/// Read all WAL records from a node's WAL directory for a given store/group.
pub(super) async fn read_wal_records(wal_dir: &Path, store_id: u64, group_id: u64) -> Vec<WALRecord> {
    let group_dir = wal_dir
        .join(format!("store{store_id}"))
        .join(format!("group{group_id}"));
    if !group_dir.exists() {
        return Vec::new();
    }

    let backend = Arc::new(IoBackend::File);
    let mut segments: Vec<(u64, PathBuf)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&group_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(seg_id) = name
                .strip_prefix("seg-")
                .and_then(|s| s.strip_suffix(".ck"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                segments.push((seg_id, entry.path()));
            }
        }
    }
    segments.sort_by_key(|(id, _)| *id);

    let mut records = Vec::new();
    for (_seg_id, path) in &segments {
        let mut seg = SegmentReader::open(&backend, path).await.unwrap();
        while let Ok(Some((record, _))) = seg.next_record().await {
            records.push(record);
        }
    }
    records
}

/// Summarize WAL records into a `WalSummary`.
pub(super) fn summarize_wal(records: &[WALRecord]) -> WalSummary {
    let mut unique_slots = BTreeSet::new();
    let mut max_slot = 0u64;
    let mut accepted_count = 0usize;

    for record in records {
        if record.record_type == RecordType::Accepted {
            accepted_count += 1;
            unique_slots.insert(record.slot);
            if record.slot > max_slot {
                max_slot = record.slot;
            }
        }
    }

    WalSummary {
        accepted_count,
        unique_slots,
        max_slot,
    }
}

/// Print a WAL summary for debugging.
pub(super) fn print_wal_summary(label: &str, summary: &WalSummary) {
    eprintln!(
        "[WAL] {label}: accepted={} unique_slots={} max_slot={} \
         contiguous_chosen={}",
        summary.accepted_count,
        summary.unique_slots.len(),
        summary.max_slot,
        summary.contiguous_chosen(),
    );
}
