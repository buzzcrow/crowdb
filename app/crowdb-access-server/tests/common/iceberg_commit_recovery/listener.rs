// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    common::TestCommitFixture,
    fault::{TestBoundary, TestCommitBlocks, TestCommitStore},
};
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds},
    wire::BearerAuthenticator,
};
use crowdb_access_server::iceberg::{serve, IcebergHttpService};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

pub struct TestCommitListener {
    server: tokio::task::JoinHandle<()>,
    pub address: SocketAddr,
    pub marker: PathBuf,
}

impl TestCommitListener {
    pub async fn normal(stack: &TestCommitFixture) -> Self {
        Self::start(
            stack,
            stack.directory.path().join(format!(
                "normal-{}.json",
                crowdb_access_iceberg::key::OperationId::random()
            )),
            usize::MAX,
            false,
        )
        .await
    }

    pub async fn start(stack: &TestCommitFixture, marker: PathBuf, target: usize, after: bool) -> Self {
        let boundary = Arc::new(TestBoundary::new(target, after, marker.clone()));
        let store = Arc::new(TestCommitStore {
            inner: stack.store(),
            boundary: boundary.clone(),
        });
        let blocks = Arc::new(TestCommitBlocks {
            inner: Arc::new(stack.blocks.with_store(store.clone())),
            boundary,
        });
        let repository = Arc::new(
            CatalogRepository::new(
                store.clone(),
                ClearBounds {
                    request_ms: 300_000,
                    delegated_access_ms: 900_000,
                    ..ClearBounds::default()
                },
            )
            .unwrap(),
        );
        let authentication =
            BearerAuthenticator::new(&"r".repeat(32), &"w".repeat(32), &"m".repeat(32), &"c".repeat(32))
                .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = IcebergHttpService::new(repository, authentication, Duration::from_secs(300))
            .with_namespaces(store.clone())
            .unwrap()
            .with_fileio(store.clone(), blocks.clone(), "us-east-1".into())
            .unwrap()
            .with_tables(store.clone(), blocks)
            .unwrap()
            .with_table_credentials(store, format!("http://{address}"))
            .unwrap();
        let server = tokio::spawn(async move {
            serve(listener, Arc::new(service), std::future::pending())
                .await
                .unwrap();
        });
        Self {
            server,
            address,
            marker,
        }
    }

    pub async fn paused(&mut self) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                assert!(
                    !self.server.is_finished(),
                    "fault listener exited before boundary"
                );
                if let Ok(bytes) = std::fs::read(&self.marker) {
                    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                        if value["paused"] == true {
                            return value;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
}

impl Drop for TestCommitListener {
    fn drop(&mut self) {
        // Dropping serve's JoinSet cancels its pending connection handlers.
        self.server.abort();
    }
}
