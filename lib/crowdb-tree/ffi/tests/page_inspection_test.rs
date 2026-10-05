// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::{Config, Crowdbtree, CtError};

#[test]
fn follows_real_root_children_and_preserves_binary_keys() {
    let tree = Crowdbtree::open(&Config {
        frame_bytes: 4096,
        ..Config::default()
    })
    .unwrap();
    for slot in 1..=2000_u64 {
        tree.apply_put(slot, &slot.to_be_bytes(), &[255; 100]).unwrap();
    }
    tree.flush().unwrap();
    tree.snapshot().unwrap();
    let before = tree.stats().snapshot_pages_total;
    let root = tree.inspect_page(&[], None).unwrap();
    assert!(root.inner);
    assert_eq!(root.page, root.root);
    let mut path = vec![0];
    let mut child = root.entry(0).unwrap().child.unwrap();
    loop {
        let page = tree.inspect_page(&path, Some(root.version)).unwrap();
        assert_eq!(page.page, child);
        if !page.inner {
            assert_eq!(page.entry(0).unwrap().key.unwrap(), 1_u64.to_be_bytes());
            assert_eq!(&page.entry(0).unwrap().cell.unwrap()[9..], &[255; 100]);
            break;
        }
        child = page.entry(0).unwrap().child.unwrap();
        path.push(0);
    }
    assert_eq!(tree.stats().snapshot_pages_total, before);
    assert!(matches!(
        tree.inspect_page(&[0; 33], None),
        Err(CtError::InvalidArgument)
    ));
    tree.apply_put(2001, b"new", b"value").unwrap();
    tree.flush().unwrap();
    tree.snapshot().unwrap();
    assert!(matches!(
        tree.inspect_page(&[], Some(root.version)),
        Err(CtError::Unavailable)
    ));
}

#[test]
fn inspection_does_not_flush_pending_memory() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    let before = tree.inspect_page(&[], None).unwrap();
    tree.apply_put(1, b"key", b"pending").unwrap();
    let after = tree.inspect_page(&[], None).unwrap();
    assert_eq!(before.fingerprint(), after.fingerprint());
    assert!(after.is_empty());
    assert_eq!(tree.get(b"key").unwrap().unwrap().1, b"pending");
}

#[test]
fn reopened_pages_are_read_without_advancing_the_checkpoint() {
    let config = Config {
        page_store: Some(crowdb_tree_ffi::PageStore::open_mem(4096).unwrap().into()),
        ..Config::default()
    };
    let tree = Crowdbtree::open(&config).unwrap();
    tree.apply_put(1, &[0, 255], b"retained").unwrap();
    tree.flush().unwrap();
    tree.snapshot().unwrap();
    let root = tree.inspect_page(&[], None).unwrap();
    drop(tree);
    let reopened = Crowdbtree::open(&config).unwrap();
    let checkpoint = reopened.snapshot_state().unwrap();
    let observed = reopened.inspect_page(&[], None).unwrap();
    assert_eq!(observed.page, root.page);
    assert_eq!(observed.entry(0).unwrap().key.unwrap(), &[0, 255]);
    assert_eq!(&observed.entry(0).unwrap().cell.unwrap()[9..], b"retained");
    assert_eq!(checkpoint, reopened.snapshot_state().unwrap());
}
