// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::mgmt::node::storage::{managed_file_zone_bytes, DEFAULT_FILE_CAPACITY_BYTES};

#[test]
fn default_file_preserves_its_full_capacity_zone() {
    let zone = managed_file_zone_bytes(DEFAULT_FILE_CAPACITY_BYTES).unwrap();
    assert_eq!(zone, DEFAULT_FILE_CAPACITY_BYTES);
    assert_eq!(DEFAULT_FILE_CAPACITY_BYTES / zone, 1);
}

#[test]
fn non_power_of_two_files_keep_whole_word_aligned_zones() {
    const MIB: u64 = 1024 * 1024;
    for capacity in [64, 128, 192, 320, 384, 768, 16 * 1024, 32 * 1024] {
        let zone = managed_file_zone_bytes(capacity * MIB).unwrap();
        assert_eq!(zone, capacity * MIB);
        assert_eq!(capacity * MIB % zone, 0);
        assert_eq!(zone / MIB % 64, 0);
    }
    assert_eq!(managed_file_zone_bytes(0), None);
    assert_eq!(managed_file_zone_bytes(65 * MIB), None);
    assert_eq!(managed_file_zone_bytes(33 * 1024 * MIB), None);
    assert_eq!(managed_file_zone_bytes(64 * 1024 * MIB), Some(32 * 1024 * MIB));
    assert_eq!(managed_file_zone_bytes(i64::MAX as u64 + 1), None);
    assert_eq!(managed_file_zone_bytes(u64::MAX), None);
}
