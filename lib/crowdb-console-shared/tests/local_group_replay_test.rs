// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use crowdb_console_shared::ops::kv_logical::create_local_group_for_tests;
use crowdb_protocol::mgmt::AddGroupRequest;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct TestGroupState {
    replica: u64,
    race: bool,
    lists: AtomicUsize,
    posts: AtomicUsize,
}

async fn list(State(state): State<Arc<TestGroupState>>) -> Json<Value> {
    let index = state.lists.fetch_add(1, Ordering::Relaxed);
    Json(if state.race && index == 0 {
        json!([])
    } else {
        json!([{ "group_id": 1, "local_replica_id": state.replica, "leader_id": 0, "remote_count": 0 }])
    })
}

async fn create(State(state): State<Arc<TestGroupState>>) -> (StatusCode, Json<Value>) {
    state.posts.fetch_add(1, Ordering::Relaxed);
    (
        StatusCode::CONFLICT,
        Json(json!({"error":"group 1 already exists in store 0"})),
    )
}

async fn replay(replica: u64, race: bool) -> (crowdb_console_shared::error::Result<bool>, usize) {
    let state = Arc::new(TestGroupState {
        replica,
        race,
        lists: AtomicUsize::new(0),
        posts: AtomicUsize::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/stores/0/groups", get(list).post(create))
        .with_state(Arc::clone(&state));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let request = AddGroupRequest {
        group_id: 1,
        replica_id: 10,
        initial_role: None,
        start_election: Some(false),
        deployment: None,
    };
    let result = create_local_group_for_tests(&endpoint, 0, &request).await;
    let posts = state.posts.load(Ordering::Relaxed);
    server.abort();
    (result, posts)
}

#[tokio::test]
async fn replay_adopts_the_exact_local_replica_without_recreating_it() {
    let (result, posts) = replay(10, false).await;
    assert!(
        !result.unwrap(),
        "adopted replicas must not enter rollback ownership"
    );
    assert_eq!(posts, 0);
}

#[tokio::test]
async fn replay_confirms_a_conflicting_create_without_retrying_the_mutation() {
    let (result, posts) = replay(10, true).await;
    assert!(!result.unwrap());
    assert_eq!(posts, 1);
}

#[tokio::test]
async fn replay_rejects_a_foreign_replica_before_and_after_a_create_race() {
    for race in [false, true] {
        let (result, posts) = replay(99, race).await;
        assert!(result.is_err());
        assert_eq!(posts, usize::from(race));
    }
}
