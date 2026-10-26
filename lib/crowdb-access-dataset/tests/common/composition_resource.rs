// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_access_dataset::{composition::*, split::SplitPolicy, *};
use md5::{Digest, Md5};
use std::sync::Arc;

pub(super) struct TestUploadedResource {
    pub uploads: DatasetUploads,
    pub location: Vec<u8>,
    pub request: CompositionRequest,
}
impl TestUploadedResource {
    pub async fn new(store: Arc<crate::split_store::TestDraftStore>, identity: &DatasetIdentity) -> Self {
        let uploads = DatasetUploads::new(store, Arc::new(crate::upload::TestPayload::default()));
        let location = DatasetUploads::location(identity, OperationId::random()).unwrap();
        let md5 = Md5::digest(b"data").into();
        uploads.create(&location, 4, 4, md5, u64::MAX).await.unwrap();
        uploads
            .put_part(&location, 0, Bytes::from_static(b"data"), 1)
            .await
            .unwrap();
        uploads.complete(&location, 2).await.unwrap();
        let request = CompositionRequest {
            parent: None,
            source: CompositionSource::All,
            schema: Some(SchemaRecord {
                version: 1,
                fields: vec![FieldDefinition {
                    name: "marker".into(),
                    required: true,
                }],
            }),
            samples: vec![SampleChange::Upsert(SampleRecord {
                sample_id: b"uploaded".to_vec(),
                fields: vec![FieldRecord {
                    name: "marker".into(),
                    value: FieldLocator::Chunk {
                        location: location.clone(),
                        length: 4,
                        md5,
                    },
                }],
            })],
            splits: Vec::new(),
            policy: SplitPolicy::default(),
        };

        Self {
            uploads,
            location,
            request,
        }
    }
}
