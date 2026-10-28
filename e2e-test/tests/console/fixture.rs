// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient, ServiceRegistryClient};

pub(super) struct TestKvCluster {
    pub mgmt_endpoints: Vec<String>,
    pub group0_leader_endpoint: String,
}

impl TestKvCluster {
    pub async fn start() -> Self {
        Self::from_addresses(crate::common::management::addresses(3)).await
    }

    pub(super) async fn from_addresses(addresses: Vec<String>) -> Self {
        assert_eq!(addresses.len(), 3);
        let mgmt_endpoints = addresses
            .iter()
            .map(|address| crate::common::management::origin(address))
            .collect::<Vec<_>>();
        let topology = crate::common::management::wait_for_leader(
            &crate::common::management::client(),
            &mgmt_endpoints[0],
        )
        .await;
        let group = topology["stores"][0]["groups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|group| group["group_id"] == 0)
            .unwrap();
        let leader = usize::try_from(group["leader_id"].as_u64().unwrap()).unwrap();
        assert!((1..=3).contains(&leader));
        Self {
            group0_leader_endpoint: format!("{}:7001", addresses[leader - 1]),
            mgmt_endpoints,
        }
    }

    pub fn make_service_registry_client(&self) -> ServiceRegistryClient {
        let client = PaxosKvClient::new(ClientConfig::new(self.mgmt_endpoints.clone()));
        client.seed_leader(0, 0, self.group0_leader_endpoint.clone());
        ServiceRegistryClient::new(client)
    }
}

pub(super) fn client_data_dir(name: &str) -> std::path::PathBuf {
    let path =
        std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").expect("owned fixture artifacts"))
            .join("client-data")
            .join(name);
    std::fs::create_dir_all(&path).unwrap();
    path
}

pub(super) async fn web_kv() -> TestKvCluster {
    let addresses = crate::common::management::addresses(5);
    TestKvCluster::from_addresses(addresses[..3].to_vec()).await
}
