// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;

use bytes::Bytes;
use crowdb_tree_ffi::{Config, Crowdbtree, CtError, ExtOp, TreeRuntime, TreeRuntimeOptions};

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

struct InputOwner {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for InputOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for InputOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_waiter_retains_chosen_bytes_until_gap_completion() {
    let tree = tree();
    let drops = Arc::new(AtomicUsize::new(0));
    let input = Bytes::from_owner(InputOwner {
        bytes: b"retained".to_vec(),
        drops: Arc::clone(&drops),
    });
    let mut high = Box::pin(tree.apply_replay(
        2,
        vec![ExtOp::Put {
            key: Bytes::from_static(b"high"),
            value: input,
        }],
    ));
    poll_fn(|context| {
        assert!(high.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(high);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    tree.apply_replay(1, Vec::new()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while tree.stats().contiguous_slot != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(tree.get(b"high").unwrap(), Some((2, b"retained".to_vec())));
    assert_eq!(drops.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn replay_empty_and_duplicate_slots_keep_the_same_contiguous_frontier() {
    let tree = tree();
    tree.apply_replay(1, Vec::new()).await.unwrap();
    tree.apply_replay(
        1,
        vec![ExtOp::Put {
            key: Bytes::from_static(b"ignored"),
            value: Bytes::from_static(b"old"),
        }],
    )
    .await
    .unwrap();
    tree.apply_replay(
        2,
        vec![ExtOp::Put {
            key: Bytes::from_static(b"new"),
            value: Bytes::from_static(b"value"),
        }],
    )
    .await
    .unwrap();
    assert_eq!(tree.stats().contiguous_slot, 2);
    assert!(tree.get(b"ignored").unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_replay_releases_foreign_input_once() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let tree = Arc::new(
        Crowdbtree::open(&Config {
            runtime: Some(Arc::clone(&runtime)),
            key_range: crowdb_tree_ffi::KeyRange::Bounded {
                start: Some(b"m".to_vec()),
                end: None,
            },
            ..Default::default()
        })
        .unwrap(),
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let input = Bytes::from_owner(InputOwner {
        bytes: b"owned".to_vec(),
        drops: Arc::clone(&drops),
    });
    let result = tree
        .apply_replay(
            1,
            vec![ExtOp::Put {
                key: Bytes::from_static(b"key"),
                value: input,
            }],
        )
        .await;
    assert_eq!(result, Err(CtError::InvalidArgument));
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_eq!(tree.stats().contiguous_slot, 0);
}

#[test]
fn replay_future_is_send_before_and_after_admission() {
    fn require_send<T: Send>(_: T) {}
    let tree = tree();
    require_send(tree.apply_replay(1, Vec::new()));
}
