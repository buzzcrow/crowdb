// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_kv_client::{ChunkKvRpcTransport, ChunkKvRpcTransportConfig, ChunkKvTransport};
use crowdb_chunk_kv_server::ChunkKvRpcService;
use crowdb_rpc_ffi::{RdmaConfig, RpcServer};
use serde_json::{json, Value};

#[path = "rdma_chunk_kv/corpus.rs"]
mod corpus;
#[path = "rdma_chunk_kv/service.rs"]
mod service;

pub(super) struct TestChunkKvProfile {
    endpoint: String,
    output: PathBuf,
    profile: String,
}

impl TestChunkKvProfile {
    pub(super) fn load() -> Self {
        Self {
            endpoint: std::env::var("CROWDB_RDMA_CHUNK_KV_ENDPOINT").unwrap(),
            output: PathBuf::from(std::env::var("CROWDB_RDMA_CHUNK_KV_OUTPUT").unwrap()),
            profile: std::env::var("CROWDB_RDMA_CHUNK_KV_PROFILE").unwrap(),
        }
    }

    fn config(&self, capacity: u32, server: bool) -> RdmaConfig {
        RdmaConfig {
            enabled: self.profile != "tcp" && !(server && self.profile == "fallback"),
            allow_tcp_fallback: self.profile == "fallback",
            listen_enabled: server,
            cq_count: 1,
            cq_depth: 16,
            sq_depth: 2,
            send_batch_size: std::env::var("CROWDB_RDMA_SEND_BATCH_SIZE")
                .map_or(1, |value| value.parse().unwrap()),
            receive_batch_size: std::env::var("CROWDB_RDMA_RECEIVE_BATCH_SIZE")
                .map_or(1, |value| value.parse().unwrap()),
            aggregation_enabled: std::env::var("CROWDB_RDMA_AGGREGATION_ENABLED").as_deref() == Ok("1"),
            rq_depth: 2,
            posted_receive_target: 2,
            device: std::env::var("CROWDB_RDMA_DEVICE").unwrap_or_default(),
            source_address: self.endpoint.rsplit_once(':').unwrap().0.into(),
            ..RdmaConfig::default()
        }
        .with_control_data_capacity(capacity)
        .unwrap()
    }

    fn write(&self, name: &str, value: &Value) {
        let path = self.output.join(name);
        let temporary = path.with_extension("tmp");
        std::fs::write(&temporary, serde_json::to_vec_pretty(value).unwrap()).unwrap();
        std::fs::rename(temporary, path).unwrap();
    }

    fn status(server: &RpcServer) -> Value {
        serde_json::from_slice(&server.transport_status_json().unwrap()).unwrap()
    }

    pub(super) async fn server(&self) {
        let service = service::create(&self.endpoint).await;
        let handlers = Arc::new(ChunkKvRpcService::new(
            service.clone(),
            tokio::runtime::Handle::current(),
        ));
        let rpc = ChunkKvRpcService::create_server(1, Some(&self.config(262_144, true))).unwrap();
        let (host, port) = self.endpoint.rsplit_once(':').unwrap();
        rpc.listen(host, port.parse().unwrap()).unwrap();
        handlers.register_handlers(&rpc);
        rpc.start();
        self.write("ready.json", &Self::status(&rpc));
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        let mut lease_sequence = 1;
        let mut next_renewal = tokio::time::Instant::now() + Duration::from_secs(2);
        let mut fragment_limit = None;
        while !self.output.join("stop").exists() {
            tick.tick().await;
            if tokio::time::Instant::now() >= next_renewal {
                lease_sequence += 1;
                service::renew(&service, lease_sequence);
                next_renewal = tokio::time::Instant::now() + Duration::from_secs(2);
                self.write("server-progress.json", &Self::status(&rpc));
            }
            if fragment_limit.is_none() && self.output.join("arm-fault").exists() {
                let before = Self::status(&rpc)["rdma"]["control_fragments_sent"]
                    .as_u64()
                    .unwrap();
                assert!(rpc.limit_control_fragments_for_tests(before + 1));
                fragment_limit = Some(before + 1);
                self.write("fault-armed.json", &json!({"fragment_limit": before + 1}));
            }
            if let Some(limit) = fragment_limit {
                let status = Self::status(&rpc);
                if status["rdma"]["control_fragments_sent"].as_u64().unwrap() == limit {
                    self.write("fault-ready.json", &status);
                }
            }
        }
        self.write("server-final.json", &Self::status(&rpc));
        rpc.stop();
    }

    pub(super) async fn client(&self) {
        let config = ChunkKvRpcTransportConfig {
            metadata_rdma: Some(self.config(65_536, false)),
            aggregate_rdma: Some(self.config(262_144, false)),
        };
        let transport = ChunkKvRpcTransport::with_config(2, 1, 1, &config).unwrap();
        let mut result = match tokio::time::timeout(
            Duration::from_secs(10),
            corpus::run(&transport, &self.endpoint),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                self.write("client-timeout.json", &json!({
                    "transport": serde_json::from_slice::<Value>(&transport.transport_status_json().unwrap()).unwrap(),
                    "connections": serde_json::from_slice::<Value>(&transport.connection_status_json(&self.endpoint).unwrap()).unwrap(),
                }));
                panic!("real Chunk-KV corpus deadline: {error}");
            }
        };
        let connections: Value =
            serde_json::from_slice(&transport.connection_status_json(&self.endpoint).unwrap()).unwrap();
        let expected_path = if self.profile == "tcp" || self.profile == "fallback" {
            "tcp"
        } else {
            "rdma"
        };
        assert_eq!(connections["metadata"]["path"], expected_path);
        assert_eq!(connections["aggregate"]["path"], expected_path);
        result["connections"] = connections;
        result["settings"] = serde_json::to_value(config).unwrap();
        result["expected_transport_errors"] = json!(0);
        if self.profile == "disconnect" {
            self.partial_response(&transport, &mut result).await;
        }
        result["transport"] = serde_json::from_slice(&transport.transport_status_json().unwrap()).unwrap();
        result["effective_uid"] = json!(std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find(|line| line.starts_with("Uid:"))
            .unwrap()
            .split_whitespace()
            .nth(2)
            .unwrap());
        result["profile"] = json!(self.profile);
        self.write("client.json", &result);
    }

    async fn partial_response(&self, transport: &ChunkKvRpcTransport, result: &mut Value) {
        std::fs::write(self.output.join("arm-fault"), "arm\n").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.output.join("fault-armed.json").exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("server fault gate");
        let before: Value = serde_json::from_slice(&transport.transport_status_json().unwrap()).unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            self.wait_partial_close(transport, &before),
        )
        .await
        .expect("partial RDMA close did not complete")
        .expect_err("incomplete logical response was published");
        let after: Value = serde_json::from_slice(&transport.transport_status_json().unwrap()).unwrap();
        let field = "control_fragments_received";
        let received = after["aggregate"]["transport"]["rdma"][field].as_u64().unwrap()
            - before["aggregate"]["transport"]["rdma"][field].as_u64().unwrap();
        assert_eq!(received, 1, "fault must follow one actual response fragment");
        assert_eq!(after["aggregate"]["transport"]["rdma"]["fallback_count"], 0);
        result["expected_transport_errors"] = json!(1);
        result["partial_response"] =
            json!({"received_fragments": received, "error": error.to_string(), "published_results": 0});
    }

    async fn wait_partial_close(
        &self,
        transport: &ChunkKvRpcTransport,
        before: &Value,
    ) -> crowdb_chunk_kv_client::Result<crowdb_protocol::chunk_kv::ChunkKvResponse> {
        let scan = corpus::scan();
        let response = transport.scan(&self.endpoint, &scan);
        tokio::pin!(response);
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        loop {
            tokio::select! {
                result = &mut response => return result,
                _ = tick.tick() => {
                    let current: Value = serde_json::from_slice(&transport.transport_status_json().unwrap()).unwrap();
                    let received = current["aggregate"]["transport"]["rdma"]["control_fragments_received"].as_u64().unwrap()
                        - before["aggregate"]["transport"]["rdma"]["control_fragments_received"].as_u64().unwrap();
                    if received == 1 {
                        self.write("client-partial-ready.json", &current);
                    }
                }
            }
        }
    }
}
