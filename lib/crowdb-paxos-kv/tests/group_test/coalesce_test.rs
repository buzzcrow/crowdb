// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R45 event-driven proposal coalescing integration tests.

use std::sync::Arc;

use crowdb_paxos_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_paxos_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::common::config::PaxosKvConfig;

#[allow(clippy::cast_possible_truncation)]
fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    encode_puts(&[(key, value)])
}

#[allow(clippy::cast_possible_truncation)]
fn encode_puts(items: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(items.len() as u16).to_le_bytes());
    for (key, value) in items {
        buf.push(0u8);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(value);
    }
    buf
}

fn coalesce_group(max_keys: usize) -> Arc<PxGroup> {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    let config = PaxosKvConfig {
        wal_early_ack: false,
        async_engine_apply: false,
        paxos: crowdb_paxos_kv::common::config::PaxosConfig {
            coalesce_max_keys: max_keys,
            ..crowdb_paxos_kv::common::config::PaxosConfig::DEFAULT
        },
        ..PaxosKvConfig::default()
    };
    group.set_from_config(&config);
    let arc = Arc::new(group);
    arc.set_self_weak();
    arc
}

#[tokio::test]
async fn individually_valid_batches_do_not_exceed_native_limit_when_coalesced() {
    let group = coalesce_group(32);
    let (release, gate) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate);
    let first_group = Arc::clone(&group);
    let first =
        tokio::spawn(async move { first_group.propose(encode_put(b"first", b"v"), None, None).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !group.has_coalesce_pending_for_tests() {
            tokio::task::yield_now().await;
        }
        let small_group = Arc::clone(&group);
        let small =
            tokio::spawn(async move { small_group.propose(encode_put(b"small", b"v"), None, None).await });
        while group.coalesce_pending_count_for_tests() != 1 {
            tokio::task::yield_now().await;
        }
        let keys: Vec<_> = (0..64).map(|i| format!("large-{i}")).collect();
        let items: Vec<_> = keys.iter().map(|key| (key.as_bytes(), b"v".as_slice())).collect();
        let result = group.propose(encode_puts(&items), None, None).await;
        let _ = release.send(());
        assert!(
            matches!(result, ProposeResult::Chosen { .. }),
            "large request: {result:?}"
        );
        let result = small.await.unwrap();
        assert!(
            matches!(result, ProposeResult::Chosen { .. }),
            "pending request: {result:?}"
        );
        assert!(matches!(first.await.unwrap(), ProposeResult::Chosen { .. }));
    })
    .await
    .expect("bounded coalescer requests did not complete");
}

#[tokio::test]
async fn coalesce_disabled_when_max_keys_zero() {
    let group = coalesce_group(0);
    let r1 = group.propose(encode_put(b"k1", b"v1"), Some(1), Some(1)).await;
    let r2 = group.propose(encode_put(b"k2", b"v2"), Some(1), Some(2)).await;
    let slot1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let slot2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    assert_ne!(slot1, slot2);
}

#[tokio::test]
async fn coalescer_watchdog_is_joined_and_does_not_retain_group() {
    let group = coalesce_group(32);
    let weak = Arc::downgrade(&group);
    let result = group
        .propose(encode_put(b"watchdog", b"v1"), Some(1), Some(1))
        .await;
    assert!(matches!(result, ProposeResult::Chosen { .. }), "got {result:?}");

    let report = group.shutdown(std::time::Duration::from_millis(200)).await;
    assert!(report.errors.is_empty(), "shutdown errors: {:?}", report.errors);
    drop(group);
    assert_eq!(weak.strong_count(), 0, "watchdog retained PxGroup after shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_resolves_pending_coalescer_waiter() {
    let group = coalesce_group(32);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);

    let first_group = Arc::clone(&group);
    let first = tokio::spawn(async move {
        first_group
            .propose(encode_put(b"shutdown-first", b"v1"), Some(10), Some(1))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while !group.has_coalesce_pending_for_tests() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first coalescer round did not start");

    let second_group = Arc::clone(&group);
    let second = tokio::spawn(async move {
        second_group
            .propose(encode_put(b"shutdown-second", b"v2"), Some(11), Some(1))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while group.coalesce_pending_count_for_tests() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second coalescer request did not become pending");

    let report = group.shutdown(std::time::Duration::from_millis(200)).await;
    assert!(report.errors.is_empty(), "shutdown errors: {:?}", report.errors);
    let _ = gate_tx.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), first)
        .await
        .expect("first coalescer round did not finish")
        .expect("first coalescer task panicked");
    let second_result = tokio::time::timeout(std::time::Duration::from_secs(1), second)
        .await
        .expect("pending coalescer waiter did not finish")
        .expect("pending coalescer task panicked");
    assert!(
        matches!(second_result, ProposeResult::Err(ref message) if message.contains("shutting down")),
        "got {second_result:?}"
    );
}

#[tokio::test]
async fn coalesce_first_op_starts_immediately() {
    let group = coalesce_group(32);
    let r = group.propose(encode_put(b"solo", b"v1"), Some(1), Some(1)).await;
    assert!(matches!(r, ProposeResult::Chosen { .. }), "got {r:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enqueue_during_empty_drain_is_never_discarded() {
    struct TestReleaseDrain(Arc<PxGroup>);
    impl Drop for TestReleaseDrain {
        fn drop(&mut self) {
            self.0.release_coalesce_empty_drain_for_tests();
        }
    }
    let group = coalesce_group(32);
    group.arm_coalesce_empty_drain_for_tests();
    let release = TestReleaseDrain(Arc::clone(&group));
    let first = group
        .propose(encode_put(b"drain-first", b"v1"), Some(1), Some(1))
        .await;
    assert!(matches!(first, ProposeResult::Chosen { .. }));
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while !group.coalesce_empty_drain_paused_for_tests() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("empty drain did not pause");
    let other = Arc::clone(&group);
    let pending = tokio::spawn(async move {
        other
            .propose(encode_put(b"drain-second", b"v2"), Some(2), Some(1))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while !pending.is_finished() && group.coalesce_pending_count_for_tests() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second enqueue did not reach the queue");
    drop(release);
    let result = tokio::time::timeout(std::time::Duration::from_millis(500), pending)
        .await
        .expect("enqueue waited for the watchdog")
        .unwrap();
    assert!(matches!(result, ProposeResult::Chosen { .. }), "got {result:?}");
    assert!(group.local_replica().learner.dedup_lookup(2, 1).is_some());
}

#[tokio::test]
async fn coalesce_ops_during_round_share_next_slot() {
    let group = coalesce_group(32);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"k1", b"v1"), Some(10), Some(1)).await });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"k2", b"v2"), Some(20), Some(1)).await });
    let g = Arc::clone(&group);
    let h3 = tokio::spawn(async move { g.propose(encode_put(b"k3", b"v3"), Some(30), Some(1)).await });
    while group.coalesce_pending_count_for_tests() < 2 {
        tokio::task::yield_now().await;
    }
    let _ = gate_tx.send(());
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();
    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s3 = match r3 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    assert_ne!(s1, s2, "first op must get its own slot");
    assert_eq!(s2, s3, "ops arriving during round must share next slot");
}

#[tokio::test]
async fn unrelated_inflight_proposal_does_not_strand_pending_batch() {
    let group = coalesce_group(32);
    let unrelated = group.hold_inflight_permit_for_tests();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);

    let g = Arc::clone(&group);
    let first = tokio::spawn(async move { g.propose(encode_put(b"mixed-a", b"v1"), Some(1), Some(1)).await });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }

    let g = Arc::clone(&group);
    let pending =
        tokio::spawn(async move { g.propose(encode_put(b"mixed-b", b"v2"), Some(2), Some(1)).await });
    while group.coalesce_pending_count_for_tests() < 1 {
        tokio::task::yield_now().await;
    }
    let _ = gate_tx.send(());

    assert!(matches!(first.await.unwrap(), ProposeResult::Chosen { .. }));
    let result = tokio::time::timeout(tokio::time::Duration::from_millis(500), pending)
        .await
        .expect("pending batch waited for watchdog")
        .unwrap();
    assert!(matches!(result, ProposeResult::Chosen { .. }), "got {result:?}");
    drop(unrelated);
}

#[tokio::test]
async fn coalesce_dedup_tags_recorded() {
    let group = coalesce_group(32);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"dk1", b"v1"), Some(100), Some(1)).await });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"dk2", b"v2"), Some(200), Some(1)).await });
    while group.coalesce_pending_count_for_tests() < 1 {
        tokio::task::yield_now().await;
    }
    let _ = gate_tx.send(());
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let learner = &group.local_replica().learner;
    assert_eq!(learner.dedup_lookup(100, 1), Some(s1));
    assert_eq!(learner.dedup_lookup(200, 1), Some(s2));
}

#[tokio::test]
async fn coalesce_max_keys_overflow_starts_concurrent_round() {
    let group = coalesce_group(32);
    group.set_coalesce_max_keys_for_tests(2);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"mk1", b"v1"), Some(1), Some(1)).await });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"mk2", b"v2"), Some(2), Some(1)).await });
    let g = Arc::clone(&group);
    let h3 = tokio::spawn(async move { g.propose(encode_put(b"mk3", b"v3"), Some(3), Some(1)).await });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let _ = gate_tx.send(());
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();
    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s3 = match r3 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    assert_ne!(s1, s2, "first op must get its own slot");
    assert_eq!(s2, s3, "overflow batch ops must share slot");
}

#[tokio::test]
async fn coalesce_applies_all_keys_to_engine() {
    let group = coalesce_group(32);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"ak1", b"v1"), Some(1), Some(1)).await });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"ak2", b"v2"), Some(2), Some(1)).await });
    while group.coalesce_pending_count_for_tests() < 1 {
        tokio::task::yield_now().await;
    }
    let _ = gate_tx.send(());
    let _ = h1.await.unwrap();
    let _ = h2.await.unwrap();
    let learner = &group.local_replica().learner;
    let v1 = learner.engine_get(b"ak1").await.expect("ak1 missing");
    let v2 = learner.engine_get(b"ak2").await.expect("ak2 missing");
    assert_eq!(v1.1, b"v1");
    assert_eq!(v2.1, b"v2");
}

#[tokio::test]
async fn coalesce_preserves_operation_counts_from_batch_requests() {
    let group = coalesce_group(32);
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate_rx);
    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move {
        g.propose(
            encode_puts(&[(b"batch-a", b"value-a"), (b"batch-b", b"value-b")]),
            Some(1),
            Some(1),
        )
        .await
    });
    while !group.has_coalesce_pending_for_tests() {
        tokio::task::yield_now().await;
    }
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move {
        g.propose(
            encode_puts(&[(b"batch-c", b"value-c"), (b"batch-d", b"value-d")]),
            Some(2),
            Some(1),
        )
        .await
    });
    while group.coalesce_pending_count_for_tests() < 2 {
        tokio::task::yield_now().await;
    }
    let _ = gate_tx.send(());
    assert!(matches!(h1.await.unwrap(), ProposeResult::Chosen { .. }));
    assert!(matches!(h2.await.unwrap(), ProposeResult::Chosen { .. }));

    for (key, value) in [
        (&b"batch-a"[..], &b"value-a"[..]),
        (&b"batch-b"[..], &b"value-b"[..]),
        (&b"batch-c"[..], &b"value-c"[..]),
        (&b"batch-d"[..], &b"value-d"[..]),
    ] {
        let stored = group
            .local_replica()
            .learner
            .engine_get(key)
            .await
            .expect("key missing");
        assert_eq!(stored.1, value);
    }
}

#[tokio::test]
async fn coalesce_sequential_ops_get_increasing_slots() {
    let group = coalesce_group(32);
    let r1 = group.propose(encode_put(b"sb1", b"v1"), Some(1), Some(1)).await;
    let r2 = group.propose(encode_put(b"sb2", b"v2"), Some(2), Some(1)).await;
    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("got {other:?}"),
    };
    assert!(s2 > s1, "second op must get a higher slot");
}
