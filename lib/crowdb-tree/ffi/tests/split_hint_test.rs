// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::{Config, Crowdbtree, KeyRange};

#[test]
fn resident_index_hint_is_interior_without_reading_values() {
    let tree = Crowdbtree::open(&Config {
        frame_bytes: 4096,
        ..Config::default()
    })
    .unwrap();
    assert!(tree.approximate_split_key().unwrap().is_none());
    for sequence in 1..=5000 {
        tree.apply_put(sequence, format!("key-{sequence:05}").as_bytes(), &[9; 100])
            .unwrap();
    }
    tree.flush().unwrap();
    let before = tree.stats();
    let key = tree.approximate_split_key().unwrap().unwrap();
    let after = tree.stats();
    assert!(key.as_slice() > b"key-00001".as_slice() && key.as_slice() <= b"key-05000".as_slice());
    assert_eq!(after.buffer_pool_misses, before.buffer_pool_misses);
    assert_eq!(after.l1_get_total, before.l1_get_total);
    assert_eq!(after.mt_get_total, before.mt_get_total);
    assert!(tree.get(&key).unwrap().is_some());
}

#[test]
fn leaf_hint_respects_bounded_range_and_empty_keys() {
    let tree = Crowdbtree::open(&Config {
        key_range: KeyRange::Bounded {
            start: Some(Vec::new()),
            end: Some(b"z".to_vec()),
        },
        ..Config::default()
    })
    .unwrap();
    for (index, key) in [b"".as_slice(), b"a", b"b", b"c"].iter().enumerate() {
        tree.apply_put(index as u64 + 1, key, b"value").unwrap();
    }
    tree.flush().unwrap();
    // Small flushes can leave a delta over the initially empty leaf base.
    // A snapshot materializes the leaf; structural hints never force that work.
    tree.snapshot().unwrap();
    let key = tree.approximate_split_key().unwrap().unwrap();
    assert!(!key.is_empty());
    assert!(key.as_slice() < b"z".as_slice());
    assert!(tree.get(&key).unwrap().is_some());
}
