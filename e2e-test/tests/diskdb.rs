// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod allocation;
mod concurrency;
mod hardware;
mod ownership;
mod space_reuse;

use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport, RetryConfig};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use std::sync::Arc;

fn kv_client() -> CrowdbKvClient {
    let addresses: Vec<String> = serde_json::from_str(&std::env::var("CROWDB_E2E_RPC").unwrap()).unwrap();
    CrowdbKvClient::new(ClientConfig::new(vec![format!("http://{}:7000", addresses[0])]))
}

fn registry() -> ServiceRegistryClient {
    ServiceRegistryClient::new(kv_client())
}

fn make_client(svc: ServiceRegistryClient) -> Arc<DiskdbClient> {
    Arc::new(
        DiskdbClient::new(svc, Arc::new(DiskdbRpcTransport::new())).with_retry_config(RetryConfig {
            max_retries: 5,
            initial_backoff: std::time::Duration::from_millis(100),
        }),
    )
}

fn make_chunk_id(high: u64, low: u64) -> crowdb_protocol::common::ChunkId {
    crowdb_protocol::common::ChunkId { high, low }
}
