// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv_client::PaxosKvClusterMetaClient;
use crowdb_protocol::common::ReplicaValue;

pub(crate) async fn register_actual_voters() {
    let addresses: Vec<String> = serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").unwrap()).unwrap();
    assert_eq!(
        addresses.len(),
        3,
        "production metadata needs three owned KV nodes"
    );
    let metadata = PaxosKvClusterMetaClient::new(crate::common::hardware::kv_client());
    metadata.add_store(0, &[1, 2, 3]).await.unwrap();
    for (index, address) in addresses.iter().enumerate() {
        let topology: serde_json::Value = reqwest::Client::new()
            .get(format!("http://{address}:7000/topology"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let store = &topology["stores"][0];
        assert_eq!(store["store_id"], 0);
        for group in store["groups"].as_array().unwrap() {
            let group_id = group["group_id"].as_u64().unwrap();
            if index == 0 {
                metadata.add_group(0, group_id).await.unwrap();
            }
            assert_eq!(group["remotes"].as_array().unwrap().len(), 2);
            assert_eq!(group["local_replica"]["voting"], true);
            metadata
                .add_replica(&ReplicaValue {
                    store_id: 0,
                    group_id,
                    replica_id: group["local_replica_id"].as_u64().unwrap(),
                    node_id: index as u64 + 1,
                    role: group["local_replica"]["role"].as_str().unwrap().to_owned(),
                    voting: true,
                    endpoint: format!(
                        "{address}:{}",
                        store["listen_addr"].as_str().unwrap().rsplit(':').next().unwrap()
                    ),
                })
                .await
                .unwrap();
        }
    }
}
