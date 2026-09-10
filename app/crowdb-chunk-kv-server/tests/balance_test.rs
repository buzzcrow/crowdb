// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_server::{
    choose_split, choose_transfer, desired_partition_count, BalanceConfig, OwnerLoad, PartitionLoad,
};
use crowdb_protocol::chunk_kv::{Id128, KeyRange};

fn owner(id: u64, count: usize, bytes: u64) -> OwnerLoad {
    OwnerLoad {
        instance_id: id,
        healthy: true,
        partition_count: count,
        durable_bytes: bytes,
        request_rate: 10,
        headroom_bytes: 10_000,
        transfer_active: false,
    }
}

fn partition(id: u64, owner: u64, bytes: u64, last_moved_ms: u64) -> PartitionLoad {
    PartitionLoad {
        partition_id: Id128 { high: 1, low: id },
        range: KeyRange {
            start: b"a".to_vec(),
            end: Some(b"z".to_vec()),
        },
        owner_instance_id: owner,
        durable_bytes: bytes,
        request_rate: 5,
        last_moved_ms,
        transition_active: false,
        live_byte_samples: Vec::new(),
    }
}

#[test]
fn three_owners_target_twelve_partitions() {
    assert_eq!(desired_partition_count(3, 5, &BalanceConfig::default()), 12);
    assert_eq!(desired_partition_count(3, 14, &BalanceConfig::default()), 14);
}

#[test]
fn split_uses_largest_eligible_partition_live_byte_median() {
    let config = BalanceConfig {
        target_partition_bytes: 100,
        cooldown_ms: 0,
        ..BalanceConfig::default()
    };
    let mut smaller = partition(1, 1, 120, 0);
    smaller.live_byte_samples = vec![(b"g".to_vec(), 60), (b"m".to_vec(), 60)];
    let mut larger = partition(2, 1, 300, 0);
    larger.live_byte_samples = vec![(b"d".to_vec(), 20), (b"k".to_vec(), 180), (b"t".to_vec(), 100)];
    let proposal = choose_split(&[smaller, larger], 1, &config).unwrap();
    assert_eq!(proposal.partition_id.low, 2);
    assert_eq!(proposal.split_key, b"k");
}

#[test]
fn count_imbalance_precedes_bytes_and_respects_cooldown() {
    let config = BalanceConfig::default();
    let owners = vec![owner(1, 5, 500), owner(2, 3, 300), owner(3, 3, 300)];
    let recent = partition(1, 1, 100, 950_000);
    assert!(choose_transfer(&owners, &[recent], 1_000_000, &config).is_none());
    let eligible = partition(1, 1, 100, 0);
    let proposal = choose_transfer(&owners, &[eligible], 1_000_000, &config).unwrap();
    assert_eq!(proposal.source_instance_id, 1);
    assert!(matches!(proposal.target_instance_id, 2 | 3));
}

#[test]
fn balanced_counts_require_weighted_improvement_threshold() {
    let config = BalanceConfig {
        cooldown_ms: 0,
        ..BalanceConfig::default()
    };
    let owners = vec![owner(1, 2, 1_000), owner(2, 2, 100)];
    let useful = partition(1, 1, 300, 0);
    assert!(choose_transfer(&owners, &[useful], 1, &config).is_some());
    let harmful = partition(2, 1, 800, 0);
    assert!(choose_transfer(&owners, &[harmful], 1, &config).is_none());
}
