#[path = "common/iceberg_stack.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/iceberg_process.rs"]
#[allow(dead_code)]
mod process;

use crowdb_access_iceberg::{
    catalog::{
        CatalogContext, CatalogRepository, CatalogStore, ClearBounds, ManagementPrivilege, RoutedCatalogStore,
    },
    file::TableLocation,
    gc::{GcLimits, GcRepository, GcStalledReason, ReaderPins},
    key::{FileId, NamespaceId, OperationId, TableId},
    operation::{mutation_identity, ManagementAction, ManagementRequest, RequestIdentity},
    record::StorageRecord,
    table::{head_key, TableHead, TableLifecycle},
};
use std::sync::Arc;

fn command(stack: &common::TestIcebergStack, token: char, arguments: &[&str]) -> std::process::Output {
    process::command(&stack.cluster.mgmt_endpoints)
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

async fn seed_table(stack: &common::TestIcebergStack) -> (Arc<RoutedCatalogStore>, CatalogContext, TableId) {
    let store = stack.store().await;
    let catalog = CatalogRepository::new(
        store.clone(),
        ClearBounds {
            request_ms: 300_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )
    .unwrap();
    let now = common::now_ms();
    catalog
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now,
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "gc-control".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            now,
        )
        .await
        .unwrap();
    common::activate(&catalog).await;
    let context = catalog.status().await.unwrap().0.context;
    let table = TableId::random();
    let location = TableLocation {
        catalog: context.catalog,
        table,
    };
    let head = TableHead {
        catalog: context.catalog,
        table,
        namespace: NamespaceId::random(),
        name: "items".into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Ready,
        generation: 1,
        metadata_file: FileId::random(),
        metadata_location: location.file("metadata/first.json").unwrap(),
        metadata_digest: [7; 32],
        format_version: 1,
        table_uuid: None,
        operation_fence: 1,
        pending_operation: None,
    };
    let key = head_key(context.catalog, table).encode().unwrap();
    let bytes = StorageRecord::TableHead(Box::new(head)).encode().unwrap();
    store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    (store, context, table)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_gc_controls_survive_separate_processes() {
    let stack = common::TestIcebergStack::start().await;
    let (store, context, table) = seed_table(&stack).await;
    let identity = OperationId::random().to_string();
    let table_id = table.to_string();
    let catalog_id = context.catalog.to_string();
    let denied = command(&stack, 'w', &["start-table", &identity, &table_id]);
    assert!(!denied.status.success());
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
        .defer(
            &current,
            GcStalledReason::Storage,
            common::now_ms(),
            GcLimits::default(),
        )
        .await
        .unwrap();
    let inspected = response(command(&stack, 'm', &["inspect", &catalog_id, &identity]));
    assert_eq!(inspected["revision"], stalled.revision);
    assert_eq!(inspected["stalled"], "Storage");
    let retried = response(command(&stack, 'm', &["retry", &catalog_id, &identity]));
    assert_eq!(retried["stalled"], "None");
    assert_eq!(retried["attempts"], 0);

    let pin_id = OperationId::random().to_string();
    response(command(&stack, 'm', &["pin", &pin_id, &table_id]));
    let pin_identity = pin_id.parse().unwrap();
    let pins = ReaderPins::new(store);
    assert!(pins
        .get(context.catalog, table, pin_identity)
        .await
        .unwrap()
        .unwrap()
        .protects(common::now_ms()));
    response(command(&stack, 'm', &["unpin", &catalog_id, &table_id, &pin_id]));
    assert!(!pins
        .get(context.catalog, table, pin_identity)
        .await
        .unwrap()
        .unwrap()
        .protects(common::now_ms()));

    let server = process::TestIcebergProcess::start_with_gc(&stack.cluster.mgmt_endpoints, true).await;
    check_foreground_namespace(&server);
    let client = reqwest::Client::new();
    for _ in 0..3 {
        let response = client
            .get(format!("http://{}/v1/config", server.address))
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
    drop(server);
    let restarted = process::TestIcebergProcess::start_with_gc(&stack.cluster.mgmt_endpoints, true).await;
    let persisted = gc
        .task(context.catalog, identity.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(persisted.revision >= progress.revision);
    drop(restarted);
}

fn check_foreground_namespace(server: &process::TestIcebergProcess) {
    if let Ok(python) = std::env::var("CROWDB_ICEBERG_E2E_PYTHON") {
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
        let status = std::process::Command::new(python)
            .arg("-c")
            .arg(script)
            .arg(format!("http://{}", server.address))
            .status()
            .unwrap();
        assert!(status.success());
    }
}
