// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crowdb_access_dataset::{
    DatasetAuthority, DatasetIdentity, DatasetReadService, DatasetUploads, NamespacePath, UploadedChunkReader,
};
use crowdb_access_server::dataset::{serve, DatasetHttpService};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

#[path = "../../../../lib/crowdb-access-dataset/tests/common/upload.rs"]
mod upload;
pub use upload::TestStore;

pub struct TestDatasetHttp {
    pub origin: String,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<std::io::Result<()>>,
}
impl TestDatasetHttp {
    #[allow(dead_code)]
    pub async fn start(store: Arc<TestStore>) -> Self {
        Self::start_inner(store, false, 8).await
    }
    #[allow(dead_code)]
    pub async fn start_payload(store: Arc<TestStore>) -> Self {
        Self::start_inner(store, true, 8).await
    }
    #[allow(dead_code)]
    pub async fn start_with_capacity(store: Arc<TestStore>, capacity: usize) -> Self {
        Self::start_inner(store, false, capacity).await
    }
    async fn start_inner(store: Arc<TestStore>, payload: bool, capacity: usize) -> Self {
        let uploads = Arc::new(DatasetUploads::new(
            store.clone(),
            Arc::new(upload::TestPayload::default()),
        ));
        let authority = if payload {
            DatasetAuthority::with_chunk_reader(store, Arc::new(UploadedChunkReader::new((*uploads).clone())))
        } else {
            DatasetAuthority::new(store)
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let reads = DatasetReadService::new(
            Arc::new(authority),
            DatasetIdentity::new(NamespacePath::root(), "legacy").unwrap(),
            3,
            u64::MAX - 1000,
        )
        .unwrap();
        let service = DatasetHttpService::new(Arc::new(reads), Duration::from_secs(30))
            .with_bearer("secret")
            .with_api_capacity(capacity);
        let service = Arc::new(if payload {
            service.with_uploads(uploads)
        } else {
            service
        });
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
