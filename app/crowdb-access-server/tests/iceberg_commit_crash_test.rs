#![cfg(feature = "iceberg-e2e")]

#[path = "common/iceberg_commit_case.rs"]
mod case;
#[path = "common/iceberg_commit_child.rs"]
mod child;
#[path = "common/iceberg_stack.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/iceberg_commit_fault.rs"]
mod fault;
#[path = "common/iceberg_commit_loser.rs"]
mod loser;
#[path = "common/iceberg_process.rs"]
#[allow(dead_code)]
mod process;

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
#[ignore = "test-only child listener invoked by native crash matrix"]
async fn native_fault_listener_child() {
    child::run().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native storage processes and kills listener subprocesses at every durable boundary"]
async fn native_table_publication_recovers_before_and_after_every_durable_write() {
    let mut stack = common::TestIcebergStack::start().await;
    let context = initialize(&stack).await;
    let directory = stack
        .cluster
        .runtime_mut()
        .service_dir("iceberg", "commit-faults")
        .unwrap();
    let setup = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
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
                let setup = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
                let case =
                    case::TestCommitCase::prepare(&format!("http://{}", setup.address), kind, name).await;
                drop(setup);
                let marker = directory.join(format!("{kind}-{offset}-{after}.json"));
                let mut child =
                    child::TestCommitChild::start(&stack.cluster.mgmt_endpoints, marker, offset, after).await;
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
                    "kill {kind} boundary {offset}/{count} after={after}: {}",
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
                let recovery = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
                case.replay(&format!("http://{}", recovery.address)).await;
                verify_generation(&stack, context, &case).await;
                drop(recovery);
            }
        }
        assert!(labels
            .iter()
            .any(|label| label.starts_with("create-") || label.starts_with("commit-")));
        if kind != "stage" {
            assert!(
                labels.contains("file-block"),
                "candidate bytes must use native chunks: {labels:?}"
            );
            assert!(labels.iter().any(|label| label.starts_with("head-")));
        }
        if let Some(offset) = head_offset {
            loser::verify(&stack, context, &directory, offset).await;
        }
    }
}

async fn baseline(stack: &common::TestIcebergStack, directory: &Path, kind: &str) -> usize {
    let setup = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let case = case::TestCommitCase::prepare(
        &format!("http://{}", setup.address),
        kind,
        format!("baseline-{kind}"),
    )
    .await;
    drop(setup);
    let child = child::TestCommitChild::start(
        &stack.cluster.mgmt_endpoints,
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
    stack: &common::TestIcebergStack,
    context: CatalogContext,
    case: &case::TestCommitCase,
) {
    let store = stack.store().await;
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

async fn initialize(stack: &common::TestIcebergStack) -> CatalogContext {
    let repository = CatalogRepository::new(
        stack.store().await,
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
            },
            ManagementPrivilege::Manage,
            now,
        )
        .await
        .unwrap();
    repository.status().await.unwrap().0.context
}
