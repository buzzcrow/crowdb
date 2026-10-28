// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, origin};
use crowdb_paxos_kv_client::{ClientConfig, GetOutcome, PaxosKvClient, ReadMode};

#[tokio::test]
#[ignore = "requires three release-container nodes, each hosting stores one and two"]
async fn multi_store_isolation_and_recovery() {
    let seeds = addresses(3).iter().map(|ip| origin(ip)).collect();
    let client = PaxosKvClient::new(ClientConfig::new(seeds));
    if std::env::var("CROWDB_E2E_PHASE").unwrap() == "write" {
        for (store, key, value) in [(1, b"ms-a", b"val-a"), (2, b"ms-b", b"val-b")] {
            client.put(store, 10, key, value, None).await.unwrap();
            client.put(store, 10, b"shared-key", value, None).await.unwrap();
        }
    }
    for (store, key, other, value) in [(1, b"ms-a", b"ms-b", b"val-a"), (2, b"ms-b", b"ms-a", b"val-b")] {
        for own_key in [key.as_slice(), b"shared-key"] {
            assert!(
                matches!(client.get(store, 10, own_key, ReadMode::Linearizable, None).await.unwrap(),
                GetOutcome::Found { value: found, .. } if found.as_ref() == value)
            );
        }
        assert!(matches!(
            client
                .get(store, 10, other, ReadMode::Linearizable, None)
                .await
                .unwrap(),
            GetOutcome::NotFound
        ));
    }
}
