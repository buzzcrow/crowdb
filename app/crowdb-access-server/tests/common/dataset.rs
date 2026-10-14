// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_dataset::{
    CasOutcome, DatasetAuthority, DatasetIdentity, DatasetReadService, DatasetStore, NamespacePath,
    StoreError, StoredValue,
};
use crowdb_access_server::dataset::{serve, DatasetHttpService};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

#[derive(Default)]
pub struct TestStore {
    values: ArcSwap<BTreeMap<Vec<u8>, StoredValue>>,
}
#[async_trait]
impl DatasetStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        Ok(self.values.load().get(key).cloned())
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        loop {
            let old = self.values.load_full();
            let current = old.get(key).cloned();
            if current.as_ref().map(|v| v.bytes.as_slice()) != expected {
                return Ok(CasOutcome::Conflict(current));
            }
            let revision = current.as_ref().map_or(1, |v| v.revision + 1);
            let mut next = (*old).clone();
            next.insert(
                key.to_vec(),
                StoredValue {
                    bytes: value.to_vec(),
                    revision,
                },
            );
            let observed = self.values.compare_and_swap(&old, Arc::new(next));
            if Arc::ptr_eq(&observed, &old) {
                return Ok(CasOutcome::Applied(revision));
            }
        }
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        loop {
            let old = self.values.load_full();
            let mut next = (*old).clone();
            next.remove(key);
            let observed = self.values.compare_and_swap(&old, Arc::new(next));
            if Arc::ptr_eq(&observed, &old) {
                return Ok(());
            }
        }
    }
}

pub struct TestDatasetHttp {
    pub origin: String,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<std::io::Result<()>>,
}
impl TestDatasetHttp {
    pub async fn start(store: Arc<TestStore>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let reads = DatasetReadService::new(
            Arc::new(DatasetAuthority::new(store)),
            DatasetIdentity::new(NamespacePath::root(), "legacy").unwrap(),
            3,
            u64::MAX - 1000,
        )
        .unwrap();
        let service =
            Arc::new(DatasetHttpService::new(Arc::new(reads), Duration::from_secs(5)).with_bearer("secret"));
        let (stop, rx) = oneshot::channel();
        let task = tokio::spawn(serve(listener, service, async {
            let _ = rx.await;
        }));
        Self {
            origin,
            stop: Some(stop),
            task,
        }
    }
    pub async fn stop(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        self.task.await.unwrap().unwrap();
    }
}
