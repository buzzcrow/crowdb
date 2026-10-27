// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::mgmt::node::storage::{managed_file_zone_bytes, DEFAULT_FILE_CAPACITY_BYTES};

#[test]
fn default_file_has_spare_zones_for_background_reclamation() {
    let zone = managed_file_zone_bytes(DEFAULT_FILE_CAPACITY_BYTES).unwrap();
    assert_eq!(zone, 256 * 1024 * 1024);
    assert_eq!(DEFAULT_FILE_CAPACITY_BYTES / zone, 64);
}

#[test]
fn non_power_of_two_files_keep_whole_word_aligned_zones() {
    const MIB: u64 = 1024 * 1024;
    for (capacity, expected) in [(64, 64), (128, 128), (192, 64), (320, 64), (384, 128), (768, 256)] {
        let zone = managed_file_zone_bytes(capacity * MIB).unwrap();
        assert_eq!(zone, expected * MIB);
        assert_eq!(capacity * MIB % zone, 0);
        assert_eq!(zone / MIB % 64, 0);
    }
    assert_eq!(managed_file_zone_bytes(0), None);
    assert_eq!(managed_file_zone_bytes(65 * MIB), None);
    assert_eq!(
        managed_file_zone_bytes((u64::from(u32::MAX) + 1) * 256 * MIB),
        None
    );
}
