// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunk_task::{
    ChunkTaskState, ChunkTaskValue, CHUNK_TASK_SCHEMA_VERSION, TASK_KIND_FINALIZE_CHUNK,
};
use crowdb_protocol::common::ChunkId;
use std::sync::Arc;

pub struct TestGroups {
    server: Arc<PxKvStore>,
    pub kv: Arc<CrowdbKvClient>,
}

impl TestGroups {
    pub async fn start() -> Self {
        let server = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        for group in 0..=3 {
            server.add_group(PxGroup::new(
                group,
                PxLocalReplica::new(group + 1, PxLocalReplicaRole::Leader),
            ));
        }
        server.start().await.unwrap();
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new())));
        for group in 0..=3 {
            kv.seed_leader(0, group, server.listen_addr().unwrap().to_string());
        }
        Self { server, kv }
    }
}

impl Drop for TestGroups {
    fn drop(&mut self) {
        self.server.stop();
    }
}

pub fn finalize(id: ChunkId) -> ChunkTaskValue {
    ChunkTaskValue {
        schema_version: CHUNK_TASK_SCHEMA_VERSION,
        task_id: id,
        partition_id: id,
        kind: TASK_KIND_FINALIZE_CHUNK,
        kind_version: 1,
        state: ChunkTaskState::Pending,
        priority: u8::MAX,
        revision: 1,
        operation_id: id,
        source_revision: 1,
        created_at_ms: 1,
        updated_at_ms: 1,
        eligible_at_ms: 100,
        attempt: 0,
        max_attempts: u32::MAX,
        estimated_queue_bytes: 0,
        claim_owner: 0,
        claim_generation: 0,
        claim_deadline_ms: 0,
        last_error_code: 0,
        last_error: String::new(),
        payload: Vec::new(),
    }
}
