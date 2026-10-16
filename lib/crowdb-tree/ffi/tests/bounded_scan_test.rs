// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::{Config, Crowdbtree};

#[test]
fn strict_scan_rejects_oversized_first_value_and_preserves_legacy_progress() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    tree.apply_put(1, b"a", &vec![7; 128 * 1024]).unwrap();
    tree.apply_put(2, b"b", b"small").unwrap();
    for persisted in [false, true] {
        if persisted {
            tree.flush().unwrap();
        }
        let (entries, truncated) = tree.scan_from_bounded(b"a", true, b"", 10, 1024).unwrap();
        assert!(entries.is_empty());
        assert!(truncated);
        let (entries, truncated) = tree.scan_from_bounded(b"a", false, b"", 10, 6).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.as_ref(), b"b");
        assert!(!truncated);
        let (legacy, _) = tree.scan(b"", b"", b"", 1, 1024, false, 0, false).unwrap();
        assert_eq!(legacy[0].value.len(), 128 * 1024);
    }
    assert!(tree.scan_from_bounded(b"", true, b"", 1, 0).is_err());
}

#[test]
fn strict_scan_reports_partial_page_without_skipping_the_next_key() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    tree.apply_put(1, b"a", b"123").unwrap();
    tree.apply_put(2, b"b", b"456").unwrap();
    tree.apply_put(3, b"c", b"789").unwrap();
    tree.flush().unwrap();
    let (first, truncated) = tree.scan_from_bounded(b"a", true, b"c", 10, 4).unwrap();
    assert!(truncated);
    assert_eq!(first.len(), 1);
    let (next, truncated) = tree.scan_from_bounded(&first[0].key, false, b"c", 10, 4).unwrap();
    assert!(!truncated);
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].key.as_ref(), b"b");
}

#[test]
fn strict_scan_bounds_external_cells_and_ignores_values_outside_interval() {
    use bytes::Bytes;
    use crowdb_tree_ffi::ExtOp;
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    tree.apply_batch_external(
        1,
        vec![
            ExtOp::Put {
                key: Bytes::from_static(b"a"),
                value: Bytes::from(vec![1; 128 * 1024]),
            },
            ExtOp::Put {
                key: Bytes::from_static(b"b"),
                value: Bytes::from_static(b"ok"),
            },
            ExtOp::Put {
                key: Bytes::from_static(b"c"),
                value: Bytes::from(vec![2; 128 * 1024]),
            },
        ],
    )
    .unwrap();
    let (entries, truncated) = tree.scan_from_bounded(b"a", true, b"", 10, 8).unwrap();
    assert!(entries.is_empty());
    assert!(truncated);
    let (entries, truncated) = tree.scan_from_bounded(b"b", true, b"c", 10, 8).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value.as_ref(), b"ok");
    assert!(!truncated);
}
