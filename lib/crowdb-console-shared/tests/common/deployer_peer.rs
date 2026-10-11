// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Pinned Console protocol peer and fake children for local client orchestration.

use axum::{
    body::Bytes,
    extract::State,
    http::{Method, StatusCode, Uri},
    routing::any,
    Json, Router,
};
use serde_json::{json, Value};
use std::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

struct TestRequest {
    method: Method,
    path: String,
    body: Value,
    reply: oneshot::Sender<Value>,
}

struct TestState {
    children: Vec<(u64, Child)>,
    stores: Vec<u64>,
    journal: Vec<String>,
}

impl Drop for TestState {
    fn drop(&mut self) {
        for (_, child) in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl TestState {
    fn apply(&mut self, request: &TestRequest) -> Value {
        let path = request.path.as_str();
        if path == "/snapshot" {
            return json!({"journal":self.journal, "stores":self.stores,
                "alive":self.children.iter_mut().map(|(_,child)| child.try_wait().unwrap().is_none()).filter(|alive| *alive).count()});
        }
        self.journal.push(format!("{} {path}", request.method));
        if path == "/internal/reset" {
            for (_, child) in &mut self.children {
                let _ = child.kill();
                child.wait().unwrap();
            }
            self.children.clear();
            self.stores.clear();
            return json!({"stopped":[]});
        }
        if path.ends_with("/server/deploy") {
            let node: u64 = path.split('/').nth(3).unwrap().parse().unwrap();
            assert!(!self.children.iter().any(|(id, _)| *id == node));
            let child = Command::new("sleep").arg("60").spawn().unwrap();
            let pid = child.id();
            self.children.push((node, child));
            return json!({"node_id":node,"pid":pid,"mgmt_url":format!("http://127.0.0.1:{}",request.body["rest_port"]),
                "rpc_url":format!("127.0.0.1:{}",request.body["rpc_port"])});
        }
        if path.ends_with("/server/stop") {
            // The HTTP acknowledgement precedes process exit. The deployer's
            // local PID reaper must stop these fake children itself.
            return json!({"sent":true});
        }
        if path == "/api/stores" && request.method == Method::POST {
            let id = request.body["store_id"].as_u64().unwrap();
            assert!(
                !self.stores.contains(&id),
                "reset must clear the previous cycle's store"
            );
            self.stores.push(id);
        }
        if request.method == Method::GET && path.ends_with("/endpoint") {
            return json!({"rpc_url":"127.0.0.1:7001"});
        }
        if request.method == Method::GET && path.contains("/groups/") {
            return json!({"store_id":1,"group_id":1,"replicas":[
                {"replica_id":1,"node_id":1,"role":"leader","state":"running"},
                {"replica_id":2,"node_id":2,"role":"follower","state":"running"},
                {"replica_id":3,"node_id":3,"role":"follower","state":"running"}]});
        }
        request.body.clone()
    }
}

async fn handle(
    State(sender): State<mpsc::Sender<TestRequest>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let (reply, result) = oneshot::channel();
    sender
        .send(TestRequest {
            method,
            path: uri.path().into(),
            body: if body.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&body).unwrap()
            },
            reply,
        })
        .await
        .unwrap();
    (StatusCode::OK, Json(result.await.unwrap()))
}

pub struct TestPeer {
    pub origin: String,
    server: tokio::task::JoinHandle<()>,
    actor: tokio::task::JoinHandle<()>,
}

impl Drop for TestPeer {
    fn drop(&mut self) {
        self.server.abort();
        self.actor.abort();
    }
}

impl TestPeer {
    pub async fn start() -> Self {
        let (sender, mut receiver) = mpsc::channel::<TestRequest>(32);
        let actor = tokio::spawn(async move {
            let mut state = TestState {
                children: Vec::new(),
                stores: Vec::new(),
                journal: Vec::new(),
            };
            while let Some(request) = receiver.recv().await {
                let response = state.apply(&request);
                let _ = request.reply.send(response);
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new().fallback(any(handle)).with_state(sender);
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            origin,
            server,
            actor,
        }
    }

    pub async fn snapshot(&self) -> Value {
        reqwest::get(format!("{}/snapshot", self.origin))
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
}
