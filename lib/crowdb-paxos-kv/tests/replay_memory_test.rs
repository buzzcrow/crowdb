// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use crowdb_paxos_kv::paxos::roles::{PxBallot, PxLogEntry};
use crowdb_paxos_kv::wal::record::WALRecord;
use crowdb_paxos_kv::wal::replay::replay_group;
use crowdb_paxos_kv::wal::IoBackend;

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
async fn binary_replay_keeps_small_record_history_within_one_gibibyte() {
    const COUNT: u64 = 800_000;
    const PAYLOAD: usize = 768;
    let temp = crowdb_test_harness::test_dirs::tempdir_in_test_data("wal-small-record-memory");
    let directory = temp.path().join("group1");
    std::fs::create_dir(&directory).unwrap();
    let mut file = std::io::BufWriter::new(std::fs::File::create(directory.join("seg-0000001.ck")).unwrap());
    file.write_all(&0x5345_474D_u32.to_le_bytes()).unwrap();
    file.write_all(&1_u16.to_le_bytes()).unwrap();
    file.write_all(&1_u64.to_le_bytes()).unwrap();
    file.write_all(&1_u64.to_le_bytes()).unwrap();
    for slot in 1..=COUNT {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(7, 1),
            term: 7,
            payload: Bytes::from(vec![u8::try_from(slot % 251).unwrap(); PAYLOAD]),
        };
        file.write_all(&WALRecord::from_accepted(1, &entry).encode())
            .unwrap();
    }
    file.flush().unwrap();
    drop(file);
    let before = peak_rss_kib();
    let started = std::time::Instant::now();
    let result = replay_group(
        &Arc::new(IoBackend::block_device()),
        &[temp.path().to_path_buf()],
        1,
    )
    .await
    .unwrap();
    let growth = peak_rss_kib().saturating_sub(before);
    println!(
        "Replay {} records / {} payload bytes: peak growth {} KiB; elapsed {:?}",
        COUNT,
        COUNT * PAYLOAD as u64,
        growth,
        started.elapsed()
    );
    assert_eq!(result.records.len(), usize::try_from(COUNT).unwrap());
    assert_eq!(result.index.slot_count(), usize::try_from(COUNT).unwrap());
    assert_eq!(result.current_term, 7);
    for (index, record) in result.records.iter().enumerate() {
        let slot = u64::try_from(index).unwrap() + 1;
        assert_eq!(record.slot, slot);
        assert_eq!(record.payload.len(), PAYLOAD);
        assert!(record
            .payload
            .iter()
            .all(|byte| *byte == u8::try_from(slot % 251).unwrap()));
    }
    assert!(
        growth < 1024 * 1024,
        "small-record WAL replay exceeded its 1 GiB memory budget: {growth} KiB"
    );
}
