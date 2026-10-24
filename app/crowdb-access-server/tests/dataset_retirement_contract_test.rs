// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_v2.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/dataset_composition.rs"]
mod composition;
#[path = "common/dataset.rs"]
mod dataset;
use common::response;
use crowdb_access_dataset::{composition::*, split::SplitPolicy, *};
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::test]
async fn retirement_respects_retention_and_finishes_split_and_composition_cleanup() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start_payload(store.clone()).await;
    let client = Client::new();
    let (root, old) = common::fixture(&client, &test.origin).await;
    let revision = common::prepare(&client, &root, &old, "16161616161616161616161616161616", false).await;
    let head = response(client.get(format!("{root}/head")), 200).await;
    let mut input = composition::input(&Value::Null);
    input["samples"] = json!([{"kind":"upsert","id":"YQ==","fields":[{"name":"value","value":{"kind":"inline","data":"Yg=="}}]}]);
    let prepared = composition::prepare(&client, &root, "17171717171717171717171717171717", &input).await;
    composition::publish(&client, &root, &prepared, &head["generation"]).await;
    let snapshot = format!("{root}/snapshots/{old}");
    response(client.post(format!("{snapshot}/retain")), 200).await;
    response(client.post(format!("{snapshot}/reclaim")), 409).await;
    response(client.post(format!("{snapshot}/release")), 200).await;
    let marked = response(client.post(format!("{snapshot}/reclaim")), 200).await;
    assert_eq!(marked["phase"], "grace_period");
    assert_eq!(marked["complete"], false);
    assert_eq!(marked["reclaim_after"], 1300);
    response(client.get(format!("{snapshot}/summary")), 404).await;
    test.clock.store(1300, std::sync::atomic::Ordering::Release);
    response(
        client
            .post(format!("{snapshot}/reclaim"))
            .json(&json!({"complete":true,"cursor":{"sample":1000}})),
        400,
    )
    .await;
    let writes = store.mutations.load(std::sync::atomic::Ordering::Relaxed);
    for _ in 0..3 {
        assert_eq!(
            response(client.post(format!("{snapshot}/reclaim")), 200).await["complete"],
            false
        );
    }
    assert_eq!(
        store.mutations.load(std::sync::atomic::Ordering::Relaxed),
        writes,
        "repeating the deletion mark must not advance background cleanup"
    );
    let mut cursor = SnapshotReclaimSweep::default();
    for step in 0..100 {
        test.authority
            .reclaim_due_page(&test.uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        let progress = response(client.post(format!("{snapshot}/reclaim")), 200).await;
        assert_eq!(progress["physical_cleanup_deferred"], true);
        if progress["complete"] == true {
            assert_eq!(progress["phase"], "complete");
            break;
        }
        assert!(step < 99, "Snapshot retirement did not terminate");
    }
    response(
        client.get(format!(
            "{snapshot}/split-revisions/{}",
            revision["revision"].as_str().unwrap()
        )),
        404,
    )
    .await;
    response(client.get(format!("{snapshot}/summary")), 404).await;
    assert_eq!(
        response(client.get(format!("{root}/head")), 200).await["latest"],
        prepared["snapshot"]
    );
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn legacy_reclaim_marks_deletion_without_bypassing_payload_owner_cleanup() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start_payload(store.clone()).await;
    let client = DatasetHttpClient::new(&test.origin, Some("secret".into()));
    client.create().await.unwrap();
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy").unwrap();
    let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
    let digest = [
        0x9d, 0xd4, 0xe4, 0x61, 0x26, 0x8c, 0x80, 0x34, 0xf5, 0xc8, 0x56, 0x4e, 0x15, 0x5c, 0x67, 0xa6,
    ];
    test.uploads
        .create(&location, 1, 1, digest, u64::MAX)
        .await
        .unwrap();
    test.uploads
        .put_part(&location, 0, bytes::Bytes::from_static(b"x"), 1)
        .await
        .unwrap();
    test.uploads.complete(&location, 2).await.unwrap();
    let snapshot = SnapshotId::random();
    let input = CompositionRequest {
        parent: None,
        source: CompositionSource::All,
        schema: Some(SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "data".into(),
                required: true,
            }],
        }),
        samples: vec![SampleChange::Upsert(SampleRecord {
            sample_id: vec![1],
            fields: vec![FieldRecord {
                name: "data".into(),
                value: FieldLocator::Chunk {
                    location: location.clone(),
                    length: 1,
                    md5: digest,
                },
            }],
        })],
        splits: Vec::new(),
        policy: SplitPolicy::default(),
    };
    test.authority
        .prepare_composition_resource(
            &identity,
            snapshot,
            &input,
            Some(&test.uploads),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert!(!client.reclaim(snapshot).await.unwrap().reclaimed);
    test.clock.store(1300, std::sync::atomic::Ordering::Release);
    let writes = store.mutations.load(std::sync::atomic::Ordering::Relaxed);
    for _ in 0..3 {
        assert!(!client.reclaim(snapshot).await.unwrap().reclaimed);
    }
    assert_eq!(store.mutations.load(std::sync::atomic::Ordering::Relaxed), writes);
    assert_eq!(
        test.uploads.metadata(&location).await.unwrap().state,
        UploadState::Claimed
    );
    let mut cursor = SnapshotReclaimSweep::default();
    for _ in 0..100 {
        test.authority
            .reclaim_due_page(&test.uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if client.reclaim(snapshot).await.unwrap().reclaimed {
            assert_eq!(
                test.uploads.metadata(&location).await.unwrap().state,
                UploadState::Complete
            );
            test.stop().await;
            return;
        }
    }
    panic!("legacy deletion did not observe background completion");
}
