// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_client::{BatchOp, ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};

const STORE_ID: u64 = 1;
const GROUP_ID: u64 = 1;

pub(super) async fn fixture_client() -> CrowdbKvClient {
    let ips = crate::common::management::addresses(1);
    let http = crate::common::management::client();
    let seed = crate::common::management::origin(&ips[0]);
    crate::common::management::wait_for_leader(&http, &seed).await;
    CrowdbKvClient::new(ClientConfig::new(vec![seed]))
}

#[tokio::test]
#[ignore = "requires the owned release-container KV client profile"]
async fn put_get_delete_round_trip_via_topology_discovery() {
    let client = fixture_client().await;
    let ips = crate::common::management::addresses(1);
    let topology = super::cluster::topology(&crate::common::management::client(), &ips[0]).await;
    assert_eq!(topology["stores"][0]["listen_addr"], "0.0.0.0:7001");

    // No leader cached yet -- `put` must discover it via `/topology`.
    let outcome = client
        .put(STORE_ID, GROUP_ID, b"k1", b"v1", None)
        .await
        .expect("put");
    assert!(outcome.revision > 0);

    match client
        .get(STORE_ID, GROUP_ID, b"k1", ReadMode::Linearizable, None)
        .await
        .unwrap()
    {
        GetOutcome::Found { value, .. } => assert_eq!(value.as_ref(), b"v1"),
        GetOutcome::NotFound => panic!("expected k1 to be found"),
    }

    let del = client
        .delete(STORE_ID, GROUP_ID, b"k1", None)
        .await
        .expect("delete");
    assert!(
        del.revision > outcome.revision,
        "delete of an existing key gets a fresh, higher revision"
    );

    match client
        .get(STORE_ID, GROUP_ID, b"k1", ReadMode::MinSlot, None)
        .await
        .unwrap()
    {
        GetOutcome::NotFound => {}
        GetOutcome::Found { .. } => panic!("k1 should have been deleted"),
    }
}

#[tokio::test]
#[ignore = "requires the owned release-container KV client profile"]
async fn revision_cas_and_conditional_batch_round_trip() {
    let client = fixture_client().await;

    let created = client
        .put_cas(STORE_ID, GROUP_ID, b"guard", b"one", 0)
        .await
        .expect("create-if-absent CAS");
    assert!(matches!(
        client.put_cas(STORE_ID, GROUP_ID, b"guard", b"stale", 0).await,
        Err(crowdb_kv_client::Error::CasFailed {
            current_revision, ..
        }) if current_revision == created.revision
    ));

    let batch = [
        BatchOp::Delete {
            key: Bytes::from_static(b"guard"),
        },
        BatchOp::Put {
            key: Bytes::from_static(b"related"),
            value: Bytes::from_static(b"value"),
        },
    ];
    client
        .batch_write_cas(STORE_ID, GROUP_ID, &batch, b"guard", created.revision)
        .await
        .expect("matching conditional batch");
    assert!(matches!(
        client
            .get(STORE_ID, GROUP_ID, b"guard", ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
    assert!(matches!(
        client
            .get(STORE_ID, GROUP_ID, b"related", ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::Found { value, .. } if value.as_ref() == b"value"
    ));
}

#[tokio::test]
#[ignore = "requires the owned release-container KV client profile"]
async fn batch_write_and_scan() {
    let client = fixture_client().await;

    client
        .batch_write(
            STORE_ID,
            GROUP_ID,
            &[
                BatchOp::Put {
                    key: Bytes::from_static(b"a1"),
                    value: Bytes::from_static(b"1"),
                },
                BatchOp::Put {
                    key: Bytes::from_static(b"a2"),
                    value: Bytes::from_static(b"2"),
                },
            ],
        )
        .await
        .expect("batch_write");

    let scanned = client
        .scan(
            STORE_ID,
            GROUP_ID,
            b"a",
            &[],
            &[],
            0,
            ReadMode::MinSlot,
            None,
            false,
            None,
        )
        .await
        .expect("scan");
    assert_eq!(scanned.items.len(), 2);
    assert!(!scanned.truncated);
    assert_eq!(
        scanned.items,
        vec![
            (Bytes::from_static(b"a1"), Bytes::from_static(b"1")),
            (Bytes::from_static(b"a2"), Bytes::from_static(b"2"))
        ]
    );
}
