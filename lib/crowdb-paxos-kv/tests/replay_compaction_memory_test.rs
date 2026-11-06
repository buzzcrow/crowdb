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
async fn replay_releases_superseded_accepts_while_scanning_half_a_gibibyte() {
    let temp = crowdb_test_harness::test_dirs::tempdir_in_test_data("wal-compaction-memory");
    let directory = temp.path().join("group1");
    std::fs::create_dir(&directory).unwrap();
    let mut file = std::io::BufWriter::new(std::fs::File::create(directory.join("seg-0000001.ck")).unwrap());
    for bytes in [
        &0x5345_474D_u32.to_le_bytes()[..],
        &1_u16.to_le_bytes(),
        &1_u64.to_le_bytes(),
        &1_u64.to_le_bytes(),
    ] {
        file.write_all(bytes).unwrap();
    }
    for sequence in 1_u64..=8192 {
        let round = (sequence - 1) / 32 + 1;
        let entry = PxLogEntry {
            slot: (sequence - 1) % 32 + 1,
            ballot: PxBallot::new(round, 7),
            term: round,
            payload: Bytes::from(vec![u8::try_from(sequence % 251).unwrap(); 65536]),
        };
        file.write_all(&WALRecord::from_accepted(1, &entry).encode())
            .unwrap();
    }
    file.flush().unwrap();
    drop(file);
    let before = peak_rss_kib();
    let result = replay_group(
        &Arc::new(IoBackend::block_device()),
        &[temp.path().to_path_buf()],
        1,
    )
    .await
    .unwrap();
    let growth = peak_rss_kib().saturating_sub(before);
    println!("Compacted replay peak growth: {growth} KiB for 512 MiB of accepted payloads");
    assert!(
        growth < 64 * 1024,
        "superseded accepts remained resident during replay: {growth} KiB"
    );
    assert_eq!(result.records.len(), 32);
    assert_eq!(result.current_term, 256);
    assert_eq!(result.index.slot_count(), 32);
    assert_eq!(result.index.segments().next().unwrap().record_count, 8192);
    for (index, record) in result.records.iter().enumerate() {
        let slot = u64::try_from(index).unwrap() + 1;
        assert_eq!(record.slot, slot);
        assert_eq!(record.ballot, PxBallot::new(256, 7));
        assert_eq!(
            record.payload,
            vec![u8::try_from((8160 + slot) % 251).unwrap(); 65536]
        );
    }
}
