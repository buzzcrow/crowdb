// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::cluster_target::TestClusterTarget;
use axum::{routing::get, Json, Router};
use crowdb_console_shared::deployment::registry::{NodeRegistry, NODES_KEY};
use crowdb_protocol::mgmt::{
    cluster::DependencyState,
    node::{
        deployment::{DeploymentMode, NodeDeploymentPolicy, PublicAccessUris},
        observation::{NodeRuntimeObservation, NodeServiceObservation},
        setup::NodeSetupChoices,
        NodeServiceIntent,
    },
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub fn observation(
    fixture: &TestClusterTarget,
    discovery: &str,
    intent: &NodeServiceIntent,
) -> NodeRuntimeObservation {
    NodeRuntimeObservation {
        version: 1,
        discovery_id: discovery.into(),
        binding: Some(fixture.binding.clone()),
        setup: None,
        policy: NodeDeploymentPolicy {
            version: 1,
            mode: DeploymentMode::Local,
            cluster: "listener-recovery".into(),
            initial_voters: 1,
            setup: NodeSetupChoices::default(),
            public_access: PublicAccessUris::default(),
        },
        observed_at_unix_ms: 1,
        services: vec![NodeServiceObservation {
            service_id: intent.service_id.clone(),
            kind: "access".into(),
            operation_id: Some(intent.operation_id.clone()),
            state: DependencyState::Ready,
            detail: String::new(),
            observed_at_unix_ms: 1,
            max_age_ms: 3000,
            reason_code: None,
        }],
        protocols: Vec::new(),
    }
}

pub async fn publish(fixture: &TestClusterTarget, registry: &NodeRegistry) {
    fixture
        .kv
        .put(0, 0, NODES_KEY, &serde_json::to_vec(registry).unwrap(), None)
        .await
        .unwrap();
}

pub async fn listener() -> (
    std::net::SocketAddr,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.2:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let counted = calls.clone();
    let app = Router::new()
        .route(
            "/",
            get(|| async { "<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>" }),
        )
        .route(
            "/v1/config",
            get(|| async { Json(serde_json::json!({"defaults":{},"overrides":{}})) }),
        )
        .route(
            "/api/dataset/v1/namespaces/dataset-ns/datasets/crowdb_readiness_probe",
            get(|| async {
                (
                    axum::http::StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"code":"not_found"})),
                )
            }),
        )
        .route(
            "/_crowdb/health/ready",
            get(|| async {
                Json(serde_json::json!({"ready":true,
            "listener":"ready","metadata":"ready","chunks":"ready","native_pool":"ready",
            "cleanup":"ready","authentication":"ready"}))
            }),
        )
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let calls = counted.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    next.run(request).await
                }
            },
        ));
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (address, calls, task)
}

pub fn credentials(root: &std::path::Path) {
    let credentials = crowdb_monitor::ServerCredentials::load_or_create(root).unwrap();
    credentials
        .persist_client(&crowdb_monitor::ClientCredentials {
            s3_endpoint: "http://public.example.test:9091".into(),
            iceberg_endpoint: "http://public.example.test:9092".into(),
            dataset_endpoint: None,
            region: "us-east-1".into(),
            access_key_id: "test-access".into(),
            secret_access_key: "test-secret".into(),
        })
        .unwrap();
}
