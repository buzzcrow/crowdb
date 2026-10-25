// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use crowdb_console_shared::ops::kv_logical::create_local_group_for_tests;
use crowdb_protocol::mgmt::{AddGroupRequest, GroupCreationStatus, GroupDeploymentIdentity};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct TestReplayState {
    owner: Option<GroupCreationStatus>,
    lost_reply: bool,
    lists: AtomicUsize,
    posts: AtomicUsize,
}

pub struct TestDeploymentReplay {
    endpoint: String,
    state: Arc<TestReplayState>,
    server: tokio::task::JoinHandle<()>,
}

impl TestDeploymentReplay {
    pub async fn start(owner: Option<GroupCreationStatus>, lost_reply: bool) -> Self {
        let state = Arc::new(TestReplayState {
            owner,
            lost_reply,
            lists: AtomicUsize::new(0),
            posts: AtomicUsize::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/stores/0/groups", get(list).post(create))
            .route("/stores/0/groups/1/creation", get(ownership))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            endpoint,
            state,
            server,
        }
    }

    pub async fn replay(
        &self,
        deployment: GroupDeploymentIdentity,
    ) -> crowdb_console_shared::error::Result<bool> {
        create_local_group_for_tests(
            &self.endpoint,
            0,
            &AddGroupRequest {
                group_id: 1,
                replica_id: 7,
                initial_role: None,
                start_election: Some(false),
                deployment: Some(deployment),
            },
        )
        .await
    }

    pub fn posts(&self) -> usize {
        self.state.posts.load(Ordering::Relaxed)
    }
}

impl Drop for TestDeploymentReplay {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn list(State(state): State<Arc<TestReplayState>>) -> Json<Value> {
    let first = state.lists.fetch_add(1, Ordering::Relaxed) == 0;
    Json(if first && state.lost_reply {
        json!([])
    } else {
        json!([{"group_id":1,"local_replica_id":7,"leader_id":0,"remote_count":0}])
    })
}

async fn create(State(state): State<Arc<TestReplayState>>) -> (StatusCode, Json<Value>) {
    state.posts.fetch_add(1, Ordering::Relaxed);
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({"error":"upstream response lost after creation"})),
    )
}

async fn ownership(State(state): State<Arc<TestReplayState>>) -> (StatusCode, Json<Value>) {
    state.owner.as_ref().map_or_else(
        || (StatusCode::NOT_FOUND, Json(json!({}))),
        |owner| (StatusCode::OK, Json(serde_json::to_value(owner).unwrap())),
    )
}

pub fn owner() -> GroupCreationStatus {
    GroupCreationStatus {
        version: 1,
        store_id: 0,
        group_id: 1,
        replica_id: 7,
        deployment: Some(GroupDeploymentIdentity {
            cluster_id: uuid::Uuid::new_v4().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
        }),
        retired: false,
    }
}
