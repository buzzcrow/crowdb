// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_v2.rs"]
#[allow(dead_code)] // Shares the Split preparation and publication HTTP fixtures.
mod common;
#[path = "common/dataset.rs"]
mod dataset;

use common::response;
use crowdb_access_dataset::{composition::*, split::*, *};
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};

fn counts(store: &TestStore) -> (u64, u64) {
    (
        store.reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    )
}

fn splits() -> Vec<DraftSplit> {
    vec![DraftSplit {
        name: SplitName::new("train").unwrap(),
        definition: SplitDefinition::Selection(Selection::Explicit(vec![vec![1]])),
        origin: MembershipOrigin::Evaluated,
    }]
}

async fn resources(
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
) -> (SnapshotId, SplitRevisionId, SplitRevisionId) {
    let snapshot = SnapshotId::random();
    authority.create_dataset(identity.clone()).await.unwrap();
    let prepared = authority
        .prepare_composition_resource(
            identity,
            snapshot,
            &CompositionRequest {
                parent: None,
                source: CompositionSource::All,
                schema: Some(SchemaRecord {
                    version: 1,
                    fields: vec![],
                }),
                samples: vec![SampleChange::Upsert(SampleRecord {
                    sample_id: vec![1],
                    fields: vec![],
                })],
                splits: splits(),
                policy: SplitPolicy::default(),
            },
            None,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let revision = SplitRevisionId::random();
    authority
        .prepare_split_revision(
            identity,
            snapshot,
            &SplitPreparation {
                revision,
                policy: SplitPolicy::default(),
                splits: splits(),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    (snapshot, revision, prepared.initial_split.unwrap())
}

#[tokio::test(start_paused = true)]
async fn background_cleanup_without_uploads_discovers_complete_revision_marks() {
    let store = Arc::new(TestStore::default());
    let clock = Arc::new(std::sync::atomic::AtomicU64::new(1000));
    let authority = Arc::new(DatasetAuthority::new(store).with_reclaim_clock_for_tests(clock.clone()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "worker").unwrap();
    let (snapshot, revision, _) = resources(&authority, &identity).await;
    let mark = authority
        .delete_split_revision(&identity, snapshot, revision, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(mark.reclaim_after, Some(2200));
    let worker = crowdb_access_server::dataset::start_dataset_cleanup_for_tests(authority.clone(), None);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        authority
            .delete_split_revision(&identity, snapshot, revision, &ReadCancellation::new())
            .await
            .unwrap(),
        mark
    );
    clock.store(2200, Ordering::Release);
    for _ in 0..80 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let status = authority
            .delete_split_revision(&identity, snapshot, revision, &ReadCancellation::new())
            .await
            .unwrap();
        if status.complete {
            worker.abort();
            let _ = worker.await;
            assert_eq!(status.reclaim_after, None);
            authority.get_snapshot(&identity, snapshot).await.unwrap();
            return;
        }
    }
    worker.abort();
    let _ = worker.await;
    panic!("background worker did not reclaim the marked revision without upload services");
}

#[tokio::test]
async fn deletion_is_mark_only_and_retries_keep_the_original_deadline() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let revision = "ab".repeat(16);
    common::prepare(&client, &root, &snapshot, &revision, false).await;
    let url = format!("{root}/snapshots/{snapshot}/split-revisions/{revision}");
    let capabilities = response(
        client.get(format!("{}/api/dataset/v2/capabilities", test.origin)),
        200,
    )
    .await;
    assert!(capabilities["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!("split-revision-delete")));
    let before = counts(&store);
    let mark = response(client.delete(&url), 200).await;
    let after = counts(&store);
    assert_eq!((after.0 - before.0, after.1 - before.1), (4, 3));
    assert_eq!(
        mark,
        json!({"snapshot":snapshot,"revision":revision,"reclaim_after":"2200","complete":false})
    );
    test.clock.store(1100, Ordering::Release);
    let before = counts(&store);
    assert_eq!(response(client.delete(&url), 200).await, mark);
    let after = counts(&store);
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    response(client.get(&url), 404).await;
    response(client.get(format!("{url}/splits/train%2Fa%20b/members")), 404).await;
    response(client.get(format!("{root}/snapshots/{snapshot}/summary")), 200).await;
    let other = format!("{root}/snapshots/{}/split-revisions/{revision}", "cd".repeat(16));
    response(client.delete(&other), 404).await;
    test.clock.store(2200, Ordering::Release);
    let identity = DatasetIdentity::new(NamespacePath::root(), "splits").unwrap();
    let mut cursor = SplitRevisionReclaimCursor::new(
        identity,
        SnapshotId::from_bytes(&hex::decode(&snapshot).unwrap()).unwrap(),
        SplitRevisionId::from_bytes(&hex::decode(&revision).unwrap()).unwrap(),
    );
    let mut complete = false;
    for _ in 0..32 {
        if test
            .authority
            .advance_split_revision_reclaim(&mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "complete Split revision did not finish bounded reclamation"
    );
    assert_eq!(
        response(client.delete(&url), 200).await,
        json!({"snapshot":snapshot,"revision":revision,"reclaim_after":null,"complete":true})
    );
    test.stop().await;
}

#[tokio::test]
async fn initial_revision_remains_protected_after_the_current_head_changes() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let identity = DatasetIdentity::new(NamespacePath::root(), "initial").unwrap();
    let (snapshot, revision, initial) = resources(&test.authority, &identity).await;
    let root = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/initial/snapshots/{snapshot}",
        test.origin
    );
    let client = Client::new();
    response(client.put(format!("{root}/split-head"))
        .json(&json!({"revision":revision.to_string(),"expected_revision":initial.to_string(),"expected_head":null})), 200).await;
    response(client.delete(format!("{root}/split-revisions/{initial}")), 409).await;
    response(client.get(format!("{root}/split-revisions/{initial}")), 200).await;
    assert_eq!(
        response(client.get(format!("{root}/split-head")), 200).await["revision"],
        revision.to_string()
    );
    test.stop().await;
}

#[tokio::test]
async fn current_revision_is_protected_and_invalid_requests_do_not_touch_storage() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let revision = common::prepare(&client, &root, &snapshot, &"bc".repeat(16), false).await;
    let head = response(client.get(format!("{root}/snapshots/{snapshot}/split-head")), 200).await;
    common::publish(&client, &root, &snapshot, &revision, &head).await;
    let url = format!(
        "{root}/snapshots/{snapshot}/split-revisions/{}",
        revision["revision"].as_str().unwrap()
    );
    response(client.delete(&url), 409).await;
    assert_eq!(response(client.get(&url), 200).await, revision);
    let before = counts(&store);
    assert_eq!(client.delete(&url).send().await.unwrap().status(), 401);
    response(client.delete(format!("{url}?expected_revision=1")), 400).await;
    response(client.delete(&url).body("{}"), 400).await;
    response(
        client.delete(format!(
            "{root}/snapshots/bad/split-revisions/{}",
            "ab".repeat(16)
        )),
        400,
    )
    .await;
    response(
        client.delete(format!("{root}/snapshots/{snapshot}/split-revisions/bad")),
        400,
    )
    .await;
    assert_eq!(counts(&store), before);
    test.stop().await;
}
