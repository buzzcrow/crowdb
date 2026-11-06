// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{Config, Crowdbtree, CtError, OwnedOp, TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;

fn runtime() -> Arc<TreeRuntime> {
    Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap())
}
fn tree(runtime: &Arc<TreeRuntime>) -> Crowdbtree {
    Crowdbtree::open(&Config {
        runtime: Some(runtime.clone()),
        ..Default::default()
    })
    .unwrap()
}
fn payload(tree: &Crowdbtree, capacity: usize) -> crowdb_tree_ffi::OwnedAllocation {
    let mut data = Vec::with_capacity(capacity);
    data.extend_from_slice(&[0xA7; 256]);
    tree.own_vec(data).unwrap()
}

#[test]
fn spare_capacity_is_charged_once_across_clones_and_slices() {
    let host = runtime();
    let first = tree(&host);
    // Opening publishes a new root and retires its temporary bootstrap state.
    // Exclude that asynchronous release from the owned-allocation delta.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let before = loop {
        let resources = first.state_stats().unwrap().resources;
        if resources.retired_bytes == 0 {
            break resources;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "bootstrap retirement did not complete"
        );
        std::thread::yield_now();
    };
    let owner = payload(&first, 1 << 20);
    let left = owner.slice(32..96).unwrap();
    let right = owner.slice(96..160).unwrap();
    assert!(left.shares_allocation(&right));
    assert_eq!(left.capacity(), owner.capacity());
    let after = first.state_stats().unwrap().resources;
    assert_eq!(
        after.payload_bytes - before.payload_bytes,
        owner.capacity() as u64
    );
    assert!(
        after.metadata_bytes >= before.metadata_bytes + std::mem::size_of::<Vec<u8>>() as u64,
        "owned allocation must charge its foreign control metadata"
    );
    assert_eq!(left.as_ref(), &[0xA7; 64]);
    assert_eq!(owner.slice(0..257).unwrap_err(), CtError::InvalidArgument);
    drop(owner);
    drop(left);
    assert_eq!(
        first.state_stats().unwrap().resources.payload_bytes,
        after.payload_bytes
    );
    drop(right);
    assert_eq!(
        first.state_stats().unwrap().resources.payload_bytes,
        before.payload_bytes
    );
}

#[test]
fn same_host_trees_share_charge_and_different_host_copies() {
    let host = runtime();
    let first = tree(&host);
    let second = tree(&host);
    let other = tree(&runtime());
    let owner = payload(&first, 1 << 20);
    let slice = owner.slice(0..128).unwrap();
    let second_before = second.state_stats().unwrap().resources.payload_bytes;
    second
        .apply_batch_owned(
            1,
            &[OwnedOp::Put {
                key: b"shared",
                value: &slice,
            }],
        )
        .unwrap();
    assert_eq!(
        second.state_stats().unwrap().resources.payload_bytes,
        second_before
    );
    let other_before = other.state_stats().unwrap().resources.payload_bytes;
    other
        .apply_batch_owned(
            1,
            &[OwnedOp::Put {
                key: b"copied",
                value: &slice,
            }],
        )
        .unwrap();
    assert_eq!(
        other.state_stats().unwrap().resources.payload_bytes - other_before,
        128 + 9
    );
    drop(first);
    drop(owner);
    drop(slice);
    assert_eq!(second.get(b"shared").unwrap(), Some((1, vec![0xA7; 128])));
    assert_eq!(other.get(b"copied").unwrap(), Some((1, vec![0xA7; 128])));
}

#[test]
fn rejected_batch_has_no_partial_write_and_caller_retains_owner() {
    let tree = tree(&runtime());
    let owner = payload(&tree, 4096);
    let invalid_key = vec![0; 65536];
    let operations = [
        OwnedOp::Put {
            key: b"valid",
            value: &owner,
        },
        OwnedOp::Put {
            key: &invalid_key,
            value: &owner,
        },
    ];
    assert_eq!(
        tree.apply_batch_owned(1, &operations),
        Err(CtError::InvalidArgument)
    );
    assert_eq!(tree.get(b"valid").unwrap(), None);
    assert_eq!(owner.as_ref(), &[0xA7; 256]);
    tree.apply_batch_owned(
        1,
        &[OwnedOp::Put {
            key: b"valid",
            value: &owner,
        }],
    )
    .unwrap();
    assert_eq!(tree.get(b"valid").unwrap(), Some((1, vec![0xA7; 256])));
}

#[test]
fn unsubmitted_owner_outlives_tree_and_host_handles() {
    let host = runtime();
    let tree = tree(&host);
    let owner = payload(&tree, 4096);
    drop(tree);
    drop(host);
    let slice = owner.slice(128..256).unwrap();
    drop(owner);
    assert_eq!(slice.as_ref(), &[0xA7; 128]);
}

#[test]
fn ownership_admission_supports_standalone_and_rejects_stopped_host() {
    let standalone = Crowdbtree::open(&Config::default()).unwrap();
    let owned = standalone.own_vec(vec![1]).unwrap();
    drop(standalone);
    assert_eq!(owned.as_ref(), &[1]);
    let host = runtime();
    let tree = tree(&host);
    host.stop_admission();
    assert!(tree.own_vec(vec![1]).is_err());
}
