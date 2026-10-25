// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod convergence;
mod foreground;
mod seed;
use super::{frontend, native::TestNativeStorage, now_ms};
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    gc::{GcLimits, GcRepository, GcStalledReason},
    key::{OperationId, TableId},
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
};
use seed::{seed_files, seed_head, seed_purge_marker, seed_table, tombstone_head};

fn command(stack: &TestNativeStorage, token: char, arguments: &[&str]) -> std::process::Output {
    frontend::command(&stack.seeds)
        .env("CROWDB_ICEBERG_TOKEN", token.to_string().repeat(32))
        .arg("gc")
        .args(arguments)
        .output()
        .unwrap()
}

fn response(output: std::process::Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .expect("GC command JSON is missing");
    serde_json::from_str(json).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release GC worker"]
async fn authenticated_gc_controls_survive_separate_processes() {
    let stack = TestNativeStorage::new();
    let (store, context, table) = seed_table(&stack).await;
    let identity = OperationId::random().to_string();
    let table_id = crowdb_access_iceberg::key::StorageTablePath {
        catalog: context.catalog,
        storage_namespace: crowdb_access_iceberg::key::NamespaceId::from_bytes(table.as_bytes()).unwrap(),
        table,
    }
    .to_string();
    let catalog_id = context.catalog.to_string();
    let denied = command(&stack, 'w', &["start-table", &identity, &table_id]);
    assert!(!denied.status.success());
    let live = command(&stack, 'm', &["start-table", &identity, &table_id]);
    assert!(!live.status.success());
    assert!(String::from_utf8_lossy(&live.stderr).contains("live-table GC is disabled"));

    tombstone_head(store.as_ref(), context, table).await;
    let created = response(command(&stack, 'm', &["start-table", &identity, &table_id]));
    assert_eq!(created["phase"], "Discover");
    assert_eq!(created["task_id"], identity);
    let repeated = response(command(&stack, 'm', &["start-table", &identity, &table_id]));
    assert_eq!(repeated["revision"], created["revision"]);
    let paused = response(command(&stack, 'm', &["pause", &catalog_id, &identity]));
    assert_eq!(paused["paused"], true);
    let resumed = response(command(&stack, 'm', &["resume", &catalog_id, &identity]));
    assert_eq!(resumed["paused"], false);

    let gc = GcRepository::new(store.clone());
    let current = gc
        .task(context.catalog, identity.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    let stalled = gc
        .defer(&current, GcStalledReason::Storage, now_ms(), GcLimits::default())
        .await
        .unwrap();
    let inspected = response(command(&stack, 'm', &["inspect", &catalog_id, &identity]));
    assert_eq!(inspected["revision"], stalled.revision);
    assert_eq!(inspected["stalled"], "Storage");
    let retried = response(command(&stack, 'm', &["retry", &catalog_id, &identity]));
    assert_eq!(retried["stalled"], "None");
    assert_eq!(retried["attempts"], 0);

    let server = frontend::TestFrontend::start(12).await;
    check_foreground_namespace(&server);
    let client = reqwest::Client::new();
    for _ in 0..3 {
        let response = client
            .get(format!("{}/v1/config", server.origin))
            .bearer_auth("r".repeat(32))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }
    let progress = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let progress = gc
                .task(context.catalog, identity.parse().unwrap())
                .await
                .unwrap()
                .unwrap();
            if progress.revision > retried["revision"].as_u64().unwrap() {
                break progress;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert!(progress.revision > stalled.revision);
    let available = command(&stack, 'm', &["inspect", &catalog_id, &identity]);
    assert!(available.status.success());
    server.stop().await;
    let restarted = frontend::TestFrontend::start(12).await;
    let persisted = gc
        .task(context.catalog, identity.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(persisted.revision >= progress.revision);
    restarted.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release GC worker"]
async fn enabled_scheduler_admits_durable_purge_markers_once() {
    let stack = TestNativeStorage::new();
    let (store, context, table) = seed_table(&stack).await;
    seed_purge_marker(store.as_ref(), context, table).await;
    let server = frontend::TestFrontend::start(12).await;
    let repository = GcRepository::new(store);
    let identity = OperationId::from_bytes(table.as_bytes()).unwrap();
    let task = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(task) = repository.task(context.catalog, identity).await.unwrap() {
                break task;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(task.kind, crowdb_access_iceberg::gc::GcTaskKind::PurgeTable);
    assert_eq!(task.head.as_ref().unwrap().table, table);
    server.stop().await;
    let restarted = frontend::TestFrontend::start(12).await;
    let resumed = repository.task(context.catalog, identity).await.unwrap().unwrap();
    assert_eq!(resumed.created_ms, task.created_ms);
    assert_eq!(resumed.head, task.head);
    restarted.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release GC worker"]
async fn enabled_scheduler_admits_and_advances_completed_clear() {
    let stack = TestNativeStorage::new();
    let (store, old, _) = seed_table(&stack).await;
    seed_files(store.as_ref(), old, TableId::random(), 48).await;
    let catalog = CatalogRepository::new(
        store.clone(),
        ClearBounds {
            request_ms: 300_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )
    .unwrap();
    let now_ms = now_ms();
    let identity = OperationId::random();
    let clear = ManagementRequest {
        identity: RequestIdentity {
            operation: identity,
            issued_ms: now_ms,
        },
        principal: "manager".into(),
        action: ManagementAction::Clear,
        expected_epoch: old.activation_epoch,
        display_name: "gc-replacement".into(),
        confirmation: Some(old.catalog),
        capabilities: None,
    };
    assert!(catalog
        .execute(clear.clone(), ManagementPrivilege::Clear, now_ms)
        .await
        .is_err());
    let crowdb_access_iceberg::catalog::RootState::Published(transition) =
        catalog.status().await.unwrap().0.state
    else {
        panic!("expected published maintenance");
    };
    catalog
        .execute(clear, ManagementPrivilege::Clear, transition.complete_after_ms)
        .await
        .unwrap();
    let active = catalog.status().await.unwrap().0.context;
    let table = seed_head(store.as_ref(), active).await;
    seed_purge_marker(store.as_ref(), active, table).await;
    let server = frontend::TestFrontend::start(12).await;
    let repository = GcRepository::new(store);
    let task = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Some(task) = repository.task(old.catalog, identity).await.unwrap() {
                if task.revision > 1 {
                    break task;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(task.kind, crowdb_access_iceberg::gc::GcTaskKind::RetiredCatalog);
    let active_task = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(task) = repository
                .task(active.catalog, OperationId::from_bytes(table.as_bytes()).unwrap())
                .await
                .unwrap()
            {
                if task.revision > 1 {
                    break task;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        active_task.kind,
        crowdb_access_iceberg::gc::GcTaskKind::PurgeTable
    );
    let retired = repository.task(old.catalog, identity).await.unwrap().unwrap();
    assert_eq!(retired.phase, crowdb_access_iceberg::gc::GcPhase::Discover);
    server.stop().await;
}

fn check_foreground_namespace(server: &frontend::TestFrontend) {
    let script = r#"import sys
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField
catalog = load_catalog("crowdb", type="rest", uri=sys.argv[1], token="w" * 32)
namespace = ("gc_foreground",)
catalog.create_namespace(namespace)
assert catalog.namespace_exists(namespace)
identifier = namespace + ("events",)
table = catalog.create_table(identifier, Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=True)))
table.transaction().set_properties({"gc-probe": "committed"}).commit_transaction()
assert catalog.load_table(identifier).properties["gc-probe"] == "committed"
catalog.drop_table(identifier)
catalog.drop_namespace(namespace)
assert not catalog.namespace_exists(namespace)
"#;
    let output = std::process::Command::new("pixi")
        .args(["run", "-e", "iceberg-e2e", "python", "-c", script, &server.origin])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "foreground SDK failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
