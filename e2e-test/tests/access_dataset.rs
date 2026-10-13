// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    DatasetManifestResponse, DatasetPublishRequest, DatasetPublishResponse, DatasetScanRequest,
    DatasetScanResponse, DatasetSnapshotRequest, FieldDefinition, FieldLocator, FieldRecord, ManifestRecord,
    Ordering, ReadPlan, SampleRecord, SchemaRecord, Selection, SnapshotId,
};
use md5::{Digest, Md5};

fn endpoint() -> String {
    format!("http://{}:7503", crate::common::management::addresses(13)[12])
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap()
}

async fn post(client: &reqwest::Client, path: &str, body: Vec<u8>) -> Vec<u8> {
    let response = client
        .post(format!("{}{path}", endpoint()))
        .bearer_auth("token")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    response.bytes().await.unwrap().to_vec()
}

#[tokio::test]
#[ignore = "requires the owned release Dataset fixture"]
async fn listener_authenticates_and_serves_create_open() {
    let client = client();
    let denied = client
        .get(format!("{}/v1/dataset/open", endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::UNAUTHORIZED);
    post(&client, "/v1/dataset/create", Vec::new()).await;
    let opened = client
        .get(format!("{}/v1/dataset/open", endpoint()))
        .bearer_auth("token")
        .send()
        .await
        .unwrap();
    assert_eq!(opened.status(), reqwest::StatusCode::OK);
}

fn manifest() -> ManifestRecord {
    let value = b"payload".to_vec();
    ManifestRecord {
        version: 1,
        snapshot: SnapshotId::random(),
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "value".into(),
                required: true,
            }],
        },
        samples: vec![SampleRecord {
            sample_id: b"sample-1".to_vec(),
            fields: vec![FieldRecord {
                name: "value".into(),
                value: FieldLocator::Inline {
                    md5: Md5::digest(&value).into(),
                    value,
                },
            }],
        }],
    }
}

#[tokio::test]
#[ignore = "requires the owned release Dataset fixture"]
async fn publish_persist_restart_and_scan_round_trip() {
    let client = client();
    post(&client, "/v1/dataset/create", Vec::new()).await;
    let mut manifest = manifest();
    let publication = DatasetPublishRequest {
        parent: None,
        manifest: bincode::serialize(&manifest).unwrap(),
        operation: None,
    };
    let published: DatasetPublishResponse = bincode::deserialize(
        &post(
            &client,
            "/v1/dataset/publish",
            bincode::serialize(&publication).unwrap(),
        )
        .await,
    )
    .unwrap();
    manifest.snapshot = published.snapshot;
    crate::common::control::node("kill", 12).await;
    crate::common::control::node("kill", 11).await;
    crate::common::control::node("start", 11).await;
    crate::common::control::node("start", 12).await;
    let persisted: DatasetManifestResponse = bincode::deserialize(
        &post(
            &client,
            "/v1/dataset/manifest",
            bincode::serialize(&DatasetSnapshotRequest {
                snapshot: published.snapshot,
            })
            .unwrap(),
        )
        .await,
    )
    .unwrap();
    assert_eq!(persisted.manifest, manifest);
    let request = DatasetScanRequest {
        plan: ReadPlan {
            snapshot: published.snapshot,
            selection: Selection::Prefix(b"sample".to_vec()),
            ordering: Ordering::SampleId,
            projection: vec!["value".into()],
            batch_size: 1,
        },
        shuffle: None,
        cursor: None,
    };
    let scanned: DatasetScanResponse =
        bincode::deserialize(&post(&client, "/v1/dataset/scan", bincode::serialize(&request).unwrap()).await)
            .unwrap();
    assert!(scanned.end);
    assert_eq!(scanned.samples.len(), 1);
    assert_eq!(scanned.snapshot, published.snapshot);
    assert_eq!(scanned.samples[0].sample_id, b"sample-1");
    assert_eq!(scanned.samples[0].values["value"], b"payload");
}
