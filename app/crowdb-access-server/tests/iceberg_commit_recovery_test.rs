// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Table publication boundaries over retained deterministic catalog and block stores.
//! A listener interruption cancels its connection handlers before recovery opens a fresh service.

#[path = "common/iceberg_commit_recovery/blocks.rs"]
mod blocks;
#[path = "common/iceberg_commit_case.rs"]
mod case;
#[path = "common/iceberg_commit_recovery/listener.rs"]
mod child;
#[path = "common/iceberg_commit_recovery/fixture.rs"]
mod common;
#[path = "common/iceberg_commit_fault.rs"]
mod fault;
#[path = "common/iceberg_commit_recovery/loser.rs"]
mod loser;
#[path = "common/iceberg_store.rs"]
mod memory;
#[path = "common/iceberg_commit_recovery/store.rs"]
mod store;

use std::collections::BTreeSet;
use std::path::Path;

use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogRepository, ClearBounds, ManagementPrivilege},
    key::OperationId,
    namespace::{NamespaceIdentifier, NamespaceRepository},
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
    table::TableRepository,
};
use serde_json::{json, Value};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_publication_recovers_before_and_after_every_durable_write() {
    let stack = common::TestCommitFixture::new();
    let context = initialize(&stack).await;
    let directory = stack.directory.path().to_path_buf();
    let setup = child::TestCommitListener::normal(&stack).await;
    case::success(
        &format!("http://{}", setup.address),
        "/v1/namespaces",
        &json!({"namespace":["analytics"]}),
    )
    .await;
    drop(setup);
    for kind in ["create", "stage", "publish-stage", "update"] {
        let count = baseline(&stack, &directory, kind).await;
        let mut labels = BTreeSet::new();
        let mut head_offset = None;
        for offset in 1..=count {
            for after in [false, true] {
                let name = format!("{kind}-{offset}-{after}");
                let setup = child::TestCommitListener::normal(&stack).await;
                let case =
                    case::TestCommitCase::prepare(&format!("http://{}", setup.address), kind, name).await;
                drop(setup);
                let marker = directory.join(format!("{kind}-{offset}-{after}.json"));
                let mut child = child::TestCommitListener::start(&stack, marker, offset, after).await;
                let endpoint = format!("http://{}", child.address);
                let path = case.path.clone();
                let identity = case.identity.clone();
                let body = case.body.clone();
                let mut request =
                    tokio::spawn(async move { case::post(&endpoint, &path, &identity, &body).await });
                let boundary = tokio::select! {
                    boundary = child.paused() => boundary,
                    result = &mut request => {
                        let response = result.unwrap().unwrap();
                        panic!("{kind} boundary {offset}/{count} returned early: {} {}", response.status(), response.text().await.unwrap());
                    }
                };
                println!(
                    "interrupt {kind} boundary {offset}/{count} after={after}: {}",
                    boundary["label"]
                );
                labels.insert(boundary["label"].as_str().unwrap().to_owned());
                if kind == "update" && boundary["label"] == "head-2" {
                    head_offset.get_or_insert(offset);
                }
                drop(child);
                assert!(
                    request.await.unwrap().is_err(),
                    "killed request must lose its response"
                );
                let recovery = child::TestCommitListener::normal(&stack).await;
                case.replay(&format!("http://{}", recovery.address)).await;
                verify_generation(&stack, context, &case).await;
                drop(recovery);
            }
        }
        if kind == "update" {
            assert_eq!(
                labels,
                BTreeSet::from([
                    "file-block".to_owned(),
                    "journal-or-fence".to_owned(),
                    "file-location-authority".to_owned(),
                    "head-2".to_owned(),
                ]),
                "direct updates must publish immutable metadata through one head CAS"
            );
            // The native tree persists data and its owner-bound directory before publication.
            assert_eq!(count, 6);
        } else if kind == "create" {
            assert_eq!(
                labels,
                BTreeSet::from([
                    "file-block".to_owned(),
                    "journal-or-fence".to_owned(),
                    "file-location-authority".to_owned(),
                    "head-1".to_owned(),
                    "name-Reserved".to_owned(),
                    "name-Published".to_owned(),
                ])
            );
            assert_eq!(count, 8);
        } else {
            assert!(labels.iter().any(|label| label.starts_with("create-")));
        }
        if kind != "stage" {
            assert!(
                labels.contains("file-block"),
                "candidate bytes must pass through file publication: {labels:?}"
            );
            assert!(labels.iter().any(|label| label.starts_with("head-")));
        }
        if let Some(offset) = head_offset {
            loser::verify(&stack, context, &directory, offset).await;
        }
    }
}

async fn baseline(stack: &common::TestCommitFixture, directory: &Path, kind: &str) -> usize {
    let setup = child::TestCommitListener::normal(stack).await;
    let case = case::TestCommitCase::prepare(
        &format!("http://{}", setup.address),
        kind,
        format!("baseline-{kind}"),
    )
    .await;
    drop(setup);
    let child = child::TestCommitListener::start(
        stack,
        directory.join(format!("baseline-{kind}.json")),
        usize::MAX,
        false,
    )
    .await;
    let response = case::post(
        &format!("http://{}", child.address),
        &case.path,
        &case.identity,
        &case.body,
    )
    .await
    .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    let marker: Value = serde_json::from_slice(&std::fs::read(&child.marker).unwrap()).unwrap();
    usize::try_from(marker["index"].as_u64().unwrap()).unwrap()
}

async fn verify_generation(
    stack: &common::TestCommitFixture,
    context: CatalogContext,
    case: &case::TestCommitCase,
) {
    let store = stack.store();
    let parent = NamespaceRepository::new(store.clone())
        .load(
            context,
            &NamespaceIdentifier::new(vec!["analytics".into()]).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let table = TableRepository::new(store)
        .select(context, parent.namespace, &case.name)
        .await
        .unwrap();
    if case.staged {
        assert!(table.is_none());
    } else {
        assert_eq!(table.unwrap().head.generation, case.generation);
    }
}

async fn initialize(stack: &common::TestCommitFixture) -> CatalogContext {
    let repository = CatalogRepository::new(
        stack.store(),
        ClearBounds {
            request_ms: 300_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )
    .unwrap();
    let now = common::now_ms();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now,
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "commit-crashes".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            now,
        )
        .await
        .unwrap();
    common::activate(&repository).await;
    repository.status().await.unwrap().0.context
}
