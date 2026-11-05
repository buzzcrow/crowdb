// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crowdb_tree_ffi::{Config, Crowdbtree, CtError, ExtOp};

#[path = "common/external_owner.rs"]
mod external_owner;
use external_owner::payload;

#[test]
fn unknown_capacity_slice_releases_large_backing_during_apply() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let backing = payload(1024 * 1024, &drops);
    let value = backing.slice(500_000..500_064);
    drop(backing);
    tree.apply_batch_external(
        1,
        vec![ExtOp::Put {
            key: Bytes::from_static(b"slice"),
            value,
        }],
    )
    .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(tree.get(b"slice").unwrap(), Some((1, vec![0xA7; 64])));
    tree.flush().unwrap();
    assert_eq!(tree.get(b"slice").unwrap(), Some((1, vec![0xA7; 64])));
    drop(tree);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn cloned_slices_across_trees_release_last_rust_owner_once() {
    let first = Crowdbtree::open(&Config::default()).unwrap();
    let second = Crowdbtree::open(&Config::default()).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let backing = payload(65_536, &drops);
    let left = backing.slice(100..200);
    let right = backing.slice(200..400);
    drop(backing);
    first
        .apply_batch_external(
            1,
            vec![ExtOp::Put {
                key: Bytes::from_static(b"first"),
                value: left,
            }],
        )
        .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    second
        .apply_batch_external(
            1,
            vec![ExtOp::Put {
                key: Bytes::from_static(b"second"),
                value: right,
            }],
        )
        .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(first.get(b"first").unwrap(), Some((1, vec![0xA7; 100])));
    assert_eq!(second.get(b"second").unwrap(), Some((1, vec![0xA7; 200])));
}

#[test]
fn rejected_batch_releases_every_transferred_owner_without_partial_apply() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    let ops = vec![
        ExtOp::Put {
            key: Bytes::from_static(b"valid"),
            value: payload(1024, &first),
        },
        ExtOp::Put {
            key: Bytes::from(vec![b'k'; 65_536]),
            value: payload(2048, &second),
        },
    ];
    assert_eq!(tree.apply_batch_external(1, ops), Err(CtError::InvalidArgument));
    assert_eq!(first.load(Ordering::SeqCst), 1);
    assert_eq!(second.load(Ordering::SeqCst), 1);
    assert_eq!(tree.get(b"valid").unwrap(), None);
}

#[test]
fn cancelling_unsubmitted_clones_and_write_handle_preserves_tree() {
    let drops = Arc::new(AtomicUsize::new(0));
    let value = payload(1024, &drops);
    let ops = vec![ExtOp::Put {
        key: Bytes::from_static(b"cancel"),
        value: value.clone(),
    }];
    drop(value);
    drop(ops);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let tree = Crowdbtree::open(&Config::default()).unwrap();
    let mut handle = tree.alloc_put(6, 1024).unwrap();
    handle.key_mut().copy_from_slice(b"cancel");
    handle.value_mut().fill(0xA7);
    drop(handle);
    assert_eq!(tree.get(b"cancel").unwrap(), None);
    let mut handle = tree.alloc_put(2, 2).unwrap();
    handle.key_mut().copy_from_slice(b"ok");
    handle.value_mut().copy_from_slice(b"ok");
    handle.apply(1).unwrap();
    assert_eq!(tree.get(b"ok").unwrap(), Some((1, b"ok".to_vec())));
}

#[test]
fn empty_value_owner_is_released_before_returning() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    tree.apply_batch_external(
        1,
        vec![ExtOp::Put {
            key: Bytes::from_static(b"empty"),
            value: payload(0, &drops),
        }],
    )
    .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(tree.get(b"empty").unwrap(), Some((1, vec![])));
}
