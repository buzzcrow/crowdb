// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/conditional_servers.rs"]
mod conditional_servers;

use conditional_servers::TestServers;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv_client::{BatchOp, Error, KvRpcTransport};

#[tokio::test]
async fn conditional_writes_discover_leader_after_explicit_rejection_without_a_hint() {
    let servers = TestServers::start(true).await;
    let raw = KvRpcTransport::new()
        .send_put_cas(
            &servers.follower.listen_addr().unwrap().to_string(),
            b"sanity",
            b"value",
            0,
            99,
            1,
            1,
            0,
            1,
        )
        .await
        .unwrap();
    assert!(!raw.ok);
    assert_eq!(raw.error, "not leader");
    assert!(raw.not_leader_hint.is_empty());
    servers
        .client()
        .put_cas(1, 1, b"single", b"value", 0)
        .await
        .unwrap();
    let batch = [BatchOp::Put {
        key: b"batch".as_slice().into(),
        value: b"value".as_slice().into(),
    }];
    servers
        .client()
        .batch_write_cas(1, 1, &batch, b"batch", 0)
        .await
        .unwrap();
}

#[tokio::test]
async fn conditional_unknown_leader_retries_are_bounded() {
    let servers = TestServers::start(false).await;
    assert!(matches!(
        servers.client().put_cas(1, 1, b"single", b"value", 0).await,
        Err(Error::RetriesExhausted { attempts: 3, .. })
    ));
    let batch = [BatchOp::Put {
        key: b"batch".as_slice().into(),
        value: b"value".as_slice().into(),
    }];
    assert!(matches!(
        servers.client().batch_write_cas(1, 1, &batch, b"batch", 0).await,
        Err(Error::RetriesExhausted { attempts: 3, .. })
    ));
}

#[tokio::test]
async fn cyclic_leader_hints_exhaust_a_bounded_budget_for_all_write_forms() {
    let servers = TestServers::start(false).await;
    servers.install_hint_cycle();
    let client = servers.client();
    let batch = [BatchOp::Put {
        key: b"cycle".as_slice().into(),
        value: b"value".as_slice().into(),
    }];
    let results = [
        client.put(1, 1, b"cycle", b"value", None).await,
        client.delete(1, 1, b"cycle", None).await,
        client.batch_write(1, 1, &batch).await,
        client.put_cas(1, 1, b"cycle", b"value", 0).await,
        client.batch_write_cas(1, 1, &batch, b"cycle", 0).await,
    ];
    for result in results {
        assert!(matches!(result, Err(Error::RetriesExhausted { attempts: 3, .. })));
    }
    let metrics = client.metrics();
    assert_eq!(metrics.unknown_leader_wait, 10);
    assert_eq!(metrics.retries_exhausted, 5);
    assert!(metrics.not_leader_hint_followed <= 20);
}
