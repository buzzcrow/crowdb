// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use axum::{routing::get, Json, Router};
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::px_kv_store::PxKvStore;
use crowdb_kv::cluster::remote_replica::PxRemoteReplica;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::mgmt::{GroupStatus, ReplicaStatus, StoreStatus, TopologyResponse};

pub struct TestServers {
    leader: Arc<PxKvStore>,
    pub follower: Arc<PxKvStore>,
    management: tokio::task::JoinHandle<()>,
    seed: String,
}

impl TestServers {
    pub async fn start(advertise_leader: bool) -> Self {
        let leader = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
        leader.add_group(PxGroup::new(
            1,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        leader.start().await.unwrap();
        let follower = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
        follower.add_group(PxGroup::new(
            1,
            PxLocalReplica::new(2, PxLocalReplicaRole::Follower),
        ));
        follower.start().await.unwrap();
        let endpoint = if advertise_leader {
            leader.listen_addr()
        } else {
            follower.listen_addr()
        }
        .unwrap()
        .to_string();
        let app = Router::new().route(
            "/topology",
            get(move || {
                let endpoint = endpoint.clone();
                async move {
                    Json(TopologyResponse {
                        stores: vec![StoreStatus {
                            store_id: 1,
                            listen_addr: Some(endpoint),
                            groups: vec![GroupStatus {
                                group_id: 1,
                                local_replica_id: 1,
                                leader_id: 1,
                                local_replica: ReplicaStatus {
                                    id: 1,
                                    ..Default::default()
                                },
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed = format!("http://{}", listener.local_addr().unwrap());
        let management = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            leader,
            follower,
            management,
            seed,
        }
    }

    pub fn client(&self) -> CrowdbKvClient {
        let mut config = ClientConfig::new(vec![self.seed.clone()]);
        config.retry.max_retries = 2;
        config.retry.unknown_leader_wait = Duration::from_millis(1);
        let client = CrowdbKvClient::new(config);
        client.seed_leader(1, 1, self.follower.listen_addr().unwrap().to_string());
        client
    }

    pub fn install_hint_cycle(&self) {
        for (node, peer, id, peer_id) in [
            (&self.leader, &self.follower, 1, 2),
            (&self.follower, &self.leader, 2, 1),
        ] {
            let replica = PxLocalReplica::new(id, PxLocalReplicaRole::Follower);
            replica.set_believed_leader(peer_id);
            let mut group = PxGroup::new(1, replica);
            group.set_remote_replicas(vec![PxRemoteReplica::new(
                peer_id,
                peer.listen_addr().unwrap().to_string(),
            )]);
            node.add_group(group);
        }
    }
}

impl Drop for TestServers {
    fn drop(&mut self) {
        self.leader.stop();
        self.follower.stop();
        self.management.abort();
    }
}
