// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::sync::Arc;

use crowdb_tree_ffi::{BatchOp, Config, Crowdbtree, CtError, TreeRuntime, TreeRuntimeOptions};

fn tree() -> Arc<Crowdbtree> {
    Arc::new(
        Crowdbtree::open(&Config {
            runtime: Some(Arc::new(
                TreeRuntime::new(TreeRuntimeOptions {
                    workers: 1,
                    ..Default::default()
                })
                .unwrap(),
            )),
            ..Default::default()
        })
        .unwrap(),
    )
}

#[test]
fn unarmed_admission_does_not_keep_a_tree_alive() {
    let tree = tree();
    let weak = Arc::downgrade(&tree);
    let prepared = tree
        .prepare_proposal(&[BatchOp::Put {
            key: b"key",
            value: b"value",
        }])
        .unwrap();
    drop(tree);
    assert!(weak.upgrade().is_none());
    assert!(matches!(prepared.arm(), Err(CtError::Unavailable)));
}

#[test]
fn generation_replacement_invalidates_unarmed_admission() {
    let tree = tree();
    let prepared = tree
        .prepare_proposal(&[BatchOp::Put {
            key: b"old",
            value: b"generation",
        }])
        .unwrap();
    tree.clear().unwrap();
    assert!(matches!(prepared.arm(), Err(CtError::Unavailable)));
    assert!(tree.get(b"old").unwrap().is_none());
}

#[test]
fn cancelled_armed_proposal_releases_close_tickets_and_future_is_send() {
    fn require_send<T: Send>(_: T) {}
    let tree = tree();
    let armed = tree.prepare_proposal(&[]).unwrap().arm().unwrap();
    require_send(armed.apply(1));
    // Dropping an unpolled Rust future cancels the still unsubmitted permit.
    tree.clear().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_apply_owns_bytes_and_preserves_last_operation() {
    let tree = tree();
    let mut key = b"key".to_vec();
    let mut value = b"owned".to_vec();
    let prepared = tree
        .prepare_proposal(&[
            BatchOp::Put {
                key: &key,
                value: b"first",
            },
            BatchOp::Delete { key: &key },
            BatchOp::Put {
                key: &key,
                value: &value,
            },
        ])
        .unwrap();
    key.fill(b'x');
    value.fill(b'y');
    prepared.arm().unwrap().apply(1).await.unwrap();
    assert_eq!(tree.get(b"key").unwrap(), Some((1, b"owned".to_vec())));
    tree.prepare_replay(&[BatchOp::Delete { key: b"key" }])
        .unwrap()
        .arm()
        .unwrap()
        .apply(2)
        .await
        .unwrap();
    assert!(tree.get(b"key").unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn replay_noop_advances_the_same_frontier() {
    let tree = tree();
    tree.prepare_replay(&[])
        .unwrap()
        .arm()
        .unwrap()
        .apply(1)
        .await
        .unwrap();
    tree.prepare_proposal(&[BatchOp::Put {
        key: b"after",
        value: b"noop",
    }])
    .unwrap()
    .arm()
    .unwrap()
    .apply(2)
    .await
    .unwrap();
    assert_eq!(tree.get(b"after").unwrap(), Some((2, b"noop".to_vec())));
    tree.flush().unwrap();
}

#[tokio::test]
async fn standalone_tree_uses_the_same_prepared_admission() {
    let tree = Arc::new(Crowdbtree::open(&Config::default()).unwrap());
    tree.prepare_proposal(&[BatchOp::Put {
        key: b"key",
        value: b"value",
    }])
    .unwrap()
    .arm()
    .unwrap()
    .apply(1)
    .await
    .unwrap();
    assert_eq!(tree.get(b"key").unwrap(), Some((1, b"value".to_vec())));
    tree.flush().unwrap();
}
