// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "publication_payload/storage.rs"]
mod storage;
#[path = "../../../../lib/crowdb-access-dataset/tests/common/upload.rs"]
#[allow(dead_code)]
mod upload;
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use crowdb_access_dataset::*;
use crowdb_access_server::dataset::{serve, DatasetHttpService};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
pub use storage::TestPublicationStore;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

pub struct TestPublicationHttp {
    pub root: String,
    pub store: Arc<TestPublicationStore>,
    pub authority: Arc<DatasetAuthority>,
    pub uploads: Arc<DatasetUploads>,
    pub identity: DatasetIdentity,
    stop: oneshot::Sender<()>,
    task: JoinHandle<std::io::Result<()>>,
}
impl TestPublicationHttp {
    pub async fn start() -> Self {
        let store = Arc::new(TestPublicationStore::default());
        let uploads = Arc::new(DatasetUploads::new(
            store.clone(),
            Arc::new(upload::TestPayload::default()),
        ));
        let authority = Arc::new(DatasetAuthority::new(store.clone()));
        let identity = DatasetIdentity::new(NamespacePath::root(), "publication-payload").unwrap();
        authority.create_dataset(identity.clone()).await.unwrap();
        let reads = Arc::new(DatasetReadService::new(authority.clone(), identity.clone(), 3).unwrap());
        let service = DatasetHttpService::new(reads, Duration::from_secs(30))
            .with_bearer("secret")
            .with_uploads(uploads.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!(
            "http://{}/api/dataset/v1/namespaces/dataset-ns/datasets/publication-payload",
            listener.local_addr().unwrap()
        );
        let (stop, rx) = oneshot::channel();
        let task = tokio::spawn(serve(listener, Arc::new(service), async {
            let _ = rx.await;
        }));
        Self {
            root,
            store,
            authority,
            uploads,
            identity,
            stop,
            task,
        }
    }

    pub async fn upload(&self, marker: u8, complete: bool) -> String {
        let id = OperationId::from_bytes(&[marker; 16]).unwrap();
        let location = DatasetUploads::location(&self.identity, id).unwrap();
        let data = vec![marker; usize::from(marker) + 4];
        self.uploads
            .create(
                &location,
                data.len() as u64,
                4,
                Md5::digest(&data).into(),
                u64::MAX,
            )
            .await
            .unwrap();
        if complete {
            for (index, bytes) in data.chunks(4).enumerate() {
                self.uploads
                    .put_part(
                        &location,
                        u32::try_from(index).unwrap(),
                        Bytes::copy_from_slice(bytes),
                        1000,
                    )
                    .await
                    .unwrap();
            }
            self.uploads.complete(&location, 1000).await.unwrap();
        }
        id.to_string()
    }

    pub fn location(&self, id: &str) -> Vec<u8> {
        DatasetUploads::location(
            &self.identity,
            OperationId::from_bytes(&hex::decode(id).unwrap()).unwrap(),
        )
        .unwrap()
    }

    pub async fn stop(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap().unwrap();
    }
}

pub fn input(uploads: &[String], fields: u8, operation: u8) -> Value {
    let samples: Vec<_> = (0..fields)
        .map(|id| {
            json!({
                "id": STANDARD.encode([id]),
                "fields":{"value":{"kind":"payload","upload":uploads[usize::from(id) % uploads.len()]}}
            })
        })
        .collect();
    json!({
        "operation":hex::encode([operation;16]),"parent":null,
        "manifest":{"schema":[{"name":"value","required":true}],"samples":samples}
    })
}

pub async fn response(request: reqwest::RequestBuilder, expected: u16) -> Value {
    let response = request.bearer_auth("secret").send().await.unwrap();
    let status = response.status().as_u16();
    let bytes = response.bytes().await.unwrap();
    let body: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("{status}: {error}; {}", String::from_utf8_lossy(&bytes)));
    assert_eq!(status, expected, "{body}");
    body
}
