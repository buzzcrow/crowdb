// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{legacy, resource, split_store};
use crowdb_access_dataset::{composition::*, *};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub struct TestComposition {
    pub store: Arc<split_store::TestDraftStore>,
    pub authority: DatasetAuthority,
    pub identity: DatasetIdentity,
    pub uploads: DatasetUploads,
    pub operation: OperationId,
    pub location: Vec<u8>,
    request: CompositionRequest,
}
impl TestComposition {
    pub async fn new() -> Self {
        let (store, authority, identity, parent) = split_store::fixture().await;
        let operation = OperationId::random();
        legacy::editing_composition(store.as_ref(), &authority, &identity, operation, parent).await;
        let resource::TestUploadedResource {
            uploads,
            location,
            mut request,
        } = resource::TestUploadedResource::new(store.clone(), &identity).await;
        request.parent = Some(parent);
        let mut hash = Sha256::new();
        hash.update(b"crowdb-composition-staging-v1\0");
        hash.update(operation.as_bytes());
        let staging = OperationId::from_bytes(&hash.finalize()[..16]).unwrap();
        uploads.claim(&location, staging, 3).await.unwrap();
        let key = legacy::key(b"DS\0CD\0", &identity, operation);
        let old = store.get(&key).await.unwrap().unwrap();
        let mut state: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
        state["uploads"] = json!({
            "staged": [DatasetUploads::upload_id(&identity, &location).unwrap()],
            "staged_retired": 0, "candidates": [],
        });
        legacy::replace(store.as_ref(), &key, &old.bytes, &state).await;
        Self {
            store,
            authority,
            identity,
            uploads,
            operation,
            location,
            request,
        }
    }

    pub async fn prepare(&self) -> SnapshotId {
        let snapshot = SnapshotId::random();
        self.authority
            .prepare_composition_resource(
                &self.identity,
                snapshot,
                &self.request,
                Some(&self.uploads),
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
        self.attach(
            self.operation,
            snapshot,
            vec![DatasetUploads::upload_id(&self.identity, &self.location).unwrap()],
        )
        .await;
        snapshot
    }

    pub async fn attach(&self, operation: OperationId, snapshot: SnapshotId, staged: Vec<OperationId>) {
        legacy::prepared_composition(self.store.as_ref(), &self.identity, operation, snapshot).await;
        let key = legacy::key(b"DS\0CD\0", &self.identity, operation);
        let old = self.store.get(&key).await.unwrap().unwrap();
        let mut state: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
        state["uploads"] = json!({
            "staged": staged, "staged_retired": 0,
            "candidates": [{
                "input_revision": 1, "snapshot": snapshot,
                "owner": OperationId::from_bytes(snapshot.as_bytes()).unwrap(),
                "uploads": [DatasetUploads::upload_id(&self.identity, &self.location).unwrap()],
                "after": null, "complete": true, "retired": 0, "keep_committed": false, "fenced": false,
            }],
        });
        legacy::replace(self.store.as_ref(), &key, &old.bytes, &state).await;
    }

    pub async fn publish(&self, snapshot: SnapshotId) {
        self.authority
            .update_snapshot_head(
                &self.identity,
                &SnapshotHeadUpdate {
                    snapshot,
                    expected_head: self
                        .authority
                        .inspect_head(&self.identity)
                        .await
                        .unwrap()
                        .generation,
                },
            )
            .await
            .unwrap();
    }

    pub async fn cleanup(&self) -> CompositionUploadCleanup {
        self.authority
            .cleanup_composition_uploads_step(&self.identity, self.operation, &self.uploads)
            .await
            .unwrap()
    }
}
