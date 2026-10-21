// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::{
    auth::RequestAuthenticator,
    metrics::{DependencyHealth, S3Health, S3Metrics},
    route::S3Route,
};
use crowdb_access_server::s3::{
    serve, AccessHealthHandler, DependencyProbeFuture, HealthObserver, S3Dispatcher, S3Operations,
    S3OperationsFuture, StorageReadiness,
};
use hyper::{body::Incoming, Request};
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

pub struct TestDependencies {
    pub phase: AtomicU8,
    pub calls: AtomicUsize,
    pub in_flight: AtomicUsize,
    pub peak: AtomicUsize,
}

struct TestProbe<'a>(&'a AtomicUsize);
impl Drop for TestProbe<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl S3Operations for TestDependencies {
    fn observe_dependencies(self: Arc<Self>) -> DependencyProbeFuture {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let count = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            self.peak.fetch_max(count, Ordering::Relaxed);
            let _probe = TestProbe(&self.in_flight);
            let phase = self.phase.load(Ordering::Relaxed);
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

pub struct TestHealth {
    pub url: String,
    pub data_url: String,
    tasks: Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
    pub dispatcher: Arc<S3Dispatcher>,
    pub operations: Arc<TestDependencies>,
    pub health: Arc<S3Health>,
    pub metrics: Arc<S3Metrics>,
    pub observer: Option<HealthObserver>,
    client: reqwest::Client,
}

impl TestHealth {
    pub async fn new(authenticator: Arc<dyn RequestAuthenticator>, phase: u8, start: bool) -> Self {
        let operations = Arc::new(TestDependencies {
            phase: AtomicU8::new(phase),
            calls: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
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
            Arc::new(AccessHealthHandler(dispatcher.clone())),
            std::future::pending(),
        ));
        let data = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let data_url = format!("http://{}/", data.local_addr().unwrap());
        let data_task = tokio::spawn(serve(data, dispatcher.clone(), std::future::pending()));
        let observer = start.then(|| dispatcher.start_health_observer().unwrap());
        let fixture = Self {
            url,
            data_url,
            tasks: vec![task, data_task],
            dispatcher,
            operations,
            health,
            metrics,
            observer,
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
        };
        if start {
            fixture.after(0).await;
        }
        fixture
    }
    pub fn generation(&self) -> u64 {
        self.dispatcher.health_generation_for_tests()
    }
    pub async fn after(&self, generation: u64) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.generation() <= generation {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("backend sampler did not publish the next generation");
    }
    pub async fn read(&self, status: u16) -> Value {
        let response = self.client.get(&self.url).send().await.unwrap();
        assert_eq!(response.status().as_u16(), status);
        serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
    }
    pub async fn business_failure(&self) {
        assert_eq!(
            self.client
                .get(&self.data_url)
                .send()
                .await
                .unwrap()
                .status()
                .as_u16(),
            503
        );
    }
}
impl Drop for TestHealth {
    fn drop(&mut self) {
        self.observer.take();
        for task in &self.tasks {
            task.abort();
        }
    }
}
