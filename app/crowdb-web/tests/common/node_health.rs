// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use crowdb_protocol::mgmt::node::{
    deployment::{DeploymentMode, NodeDeploymentPolicy, PublicAccessUris},
    observation::NodeRuntimeObservation,
    setup::NodeSetupChoices,
};
use crowdb_web::{AppState, NodeObserver};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

pub struct TestNodeHealth {
    pub state: AppState,
    pub calls: Arc<AtomicUsize>,
    pub fail: Arc<AtomicBool>,
    pub observer: Option<NodeObserver>,
    app: axum::Router,
}
impl TestNodeHealth {
    pub fn new() -> Self {
        let state = AppState::default()
            .with_node_monitor("http://127.0.0.1:1")
            .unwrap();
        let app = crowdb_web::router(state.clone());
        Self {
            state,
            calls: Arc::new(AtomicUsize::new(0)),
            fail: Arc::new(AtomicBool::new(false)),
            observer: None,
            app,
        }
    }
    pub fn start(&mut self) {
        let calls = self.calls.clone();
        let fail = self.fail.clone();
        self.observer = Some(
            crowdb_web::start_node_observer_for_tests(&self.state, move || {
                let calls = calls.clone();
                let fail = fail.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    if fail.load(Ordering::Relaxed) {
                        return Err((StatusCode::BAD_GATEWAY, "backend dependency unavailable".into()));
                    }
                    Ok(NodeRuntimeObservation {
                        version: 1,
                        discovery_id: "00000000-0000-4000-8000-000000000001".into(),
                        policy: NodeDeploymentPolicy {
                            version: 1,
                            mode: DeploymentMode::Local,
                            cluster: "cached-health".into(),
                            initial_voters: 1,
                            setup: NodeSetupChoices::default(),
                            public_access: PublicAccessUris::default(),
                        },
                        binding: None,
                        setup: None,
                        observed_at_unix_ms: u64::try_from(
                            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis(),
                        )
                        .unwrap(),
                        services: Vec::new(),
                        protocols: Vec::new(),
                    })
                }
            })
            .unwrap(),
        );
    }
    pub async fn after(&self, calls: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.calls.load(Ordering::Relaxed) <= calls {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("node sampler did not advance without reader traffic");
    }
    pub async fn read(&self, status: u16) -> serde_json::Value {
        let response = self
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cluster/node-observation")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        serde_json::from_slice(&to_bytes(response.into_body(), 256 * 1024).await.unwrap()).unwrap()
    }
}
