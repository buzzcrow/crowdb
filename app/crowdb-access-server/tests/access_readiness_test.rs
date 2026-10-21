// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#![cfg(feature = "s3")]

use std::{
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crowdb_access_s3::{
    auth::{CredentialCache, RequestAuthenticator, SigV4Verifier, TrustedNetworkAuthenticator},
    metrics::{DependencyHealth, S3Health, S3Metrics},
    route::S3Route,
};
use crowdb_access_server::s3::{
    serve, AccessHealthHandler, DependencyProbeFuture, S3Dispatcher, S3Operations, S3OperationsFuture,
    StorageReadiness,
};
use hyper::{body::Incoming, Request};
use serde_json::Value;

struct TestDependencies(AtomicU8);

impl S3Operations for TestDependencies {
    fn observe_dependencies(self: Arc<Self>) -> DependencyProbeFuture {
        Box::pin(async move {
            let phase = self.0.load(Ordering::Relaxed);
            if phase == 3 {
                std::future::pending::<()>().await;
            }
            Some(StorageReadiness {
                metadata: if phase == 1 {
                    Err("metadata owner unavailable".into())
                } else {
                    Ok("current routed read".into())
                },
                chunks: if phase == 2 {
                    Err("physical disk read failed".into())
                } else {
                    Ok("current disk reads".into())
                },
            })
        })
    }

    fn execute(
        self: Arc<Self>,
        _: S3Route,
        _: Request<Incoming>,
        _: String,
        _: String,
    ) -> S3OperationsFuture {
        panic!("readiness must not dispatch application operations");
    }
}

struct TestHealth {
    url: String,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
    operations: Arc<TestDependencies>,
    health: Arc<S3Health>,
    metrics: Arc<S3Metrics>,
}

impl TestHealth {
    async fn new(authenticator: Arc<dyn RequestAuthenticator>) -> Self {
        let operations = Arc::new(TestDependencies(AtomicU8::new(1)));
        let health = Arc::new(S3Health::starting(1));
        health.set_listener(DependencyHealth::Ready);
        let metrics = Arc::new(S3Metrics::default());
        let dispatcher = Arc::new(
            S3Dispatcher::new(
                authenticator,
                operations.clone(),
                metrics.clone(),
                "test".into(),
                false,
            )
            .with_health(health.clone()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/_crowdb/health/ready", listener.local_addr().unwrap());
        let task = tokio::spawn(serve(
            listener,
            Arc::new(AccessHealthHandler(dispatcher)),
            std::future::pending(),
        ));
        Self {
            url,
            task,
            operations,
            health,
            metrics,
        }
    }

    async fn read(&self, status: u16) -> Value {
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(&self.url)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
    }
}

impl Drop for TestHealth {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn current_readiness_recovers_without_application_traffic_and_preserves_stop() {
    let fixture = TestHealth::new(Arc::new(TrustedNetworkAuthenticator::new())).await;
    let failed = fixture.read(503).await;
    assert_eq!(failed["metadata"], "unavailable");
    assert_eq!(failed["chunks"], "ready");
    assert_eq!(failed["details"]["metadata"], "metadata owner unavailable");
    fixture.operations.0.store(2, Ordering::Relaxed);
    let failed = fixture.read(503).await;
    assert_eq!(failed["metadata"], "ready");
    assert_eq!(failed["chunks"], "unavailable");
    fixture.operations.0.store(0, Ordering::Relaxed);
    assert_eq!(fixture.read(200).await["ready"], true);
    // Fresh evidence is response-local; request outcomes cannot overwrite it.
    assert_eq!(
        fixture.health.snapshot(&fixture.metrics, None).readiness.chunks,
        DependencyHealth::Unavailable
    );
    fixture.health.stop();
    let stopped = fixture.read(503).await;
    assert_eq!(stopped["ready"], false);
    assert!(stopped["reasons"]
        .as_array()
        .unwrap()
        .contains(&Value::from("live")));
}

#[tokio::test]
async fn stalled_observations_fail_closed_within_the_health_budget() {
    let fixture = TestHealth::new(Arc::new(TrustedNetworkAuthenticator::new())).await;
    fixture.operations.0.store(3, Ordering::Relaxed);
    let started = Instant::now();
    let failed = fixture.read(503).await;
    assert!(started.elapsed() < Duration::from_millis(1500));
    assert_eq!(failed["metadata"], "unavailable");
    assert_eq!(failed["chunks"], "unavailable");
    assert!(failed["details"]["metadata"]
        .as_str()
        .unwrap()
        .contains("deadline"));
}

#[tokio::test]
async fn credential_refresh_and_cleanup_are_observed_without_authenticating_a_request() {
    let cache = Arc::new(CredentialCache::new(90));
    let fixture = TestHealth::new(Arc::new(SigV4Verifier::new(
        cache.clone(),
        "us-east-1".into(),
        900,
    )))
    .await;
    fixture.operations.0.store(0, Ordering::Relaxed);
    assert_eq!(fixture.read(503).await["authentication"], "unavailable");
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    cache.install([], now - 91).unwrap();
    assert_eq!(fixture.read(503).await["authentication"], "unavailable");
    cache.install([], now).unwrap();
    assert_eq!(fixture.read(200).await["authentication"], "ready");
    fixture.metrics.enqueue_cleanup(2);
    let backlog = fixture.read(503).await;
    assert_eq!(backlog["cleanup"], "unavailable");
    assert_eq!(backlog["cleanup_backlog"], 2);
}
