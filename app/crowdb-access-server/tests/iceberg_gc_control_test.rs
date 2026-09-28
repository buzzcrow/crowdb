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
    file::{file_key, ContentFormat, FileContent, FileKind, FileRecord, TableLocation},
    gc::{GcLimits, GcRepository, GcStalledReason, ReaderPins},
    key::{FileId, NamespaceId, OperationId, TableId},
    operation::{mutation_identity, ManagementAction, ManagementRequest, RequestIdentity},
    record::StorageRecord,
    table::{head_key, TableHead, TableLifecycle, TablePurgeTask},
};
use sha2::{Digest, Sha256};
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
    let table = seed_head(store.as_ref(), context).await;
    (store, context, table)
}

async fn seed_head(store: &RoutedCatalogStore, context: CatalogContext) -> TableId {
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
    table
}

async fn check_operator_pin(
    stack: &common::TestIcebergStack,
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    table: TableId,
) {
    let pin_id = OperationId::random().to_string();
    let table_id = table.to_string();
    let catalog_id = context.catalog.to_string();
    response(command(stack, 'm', &["pin", &pin_id, &table_id]));
    let pin_identity = pin_id.parse().unwrap();
    let pins = ReaderPins::new(store);
    assert!(pins
        .get(context.catalog, table, pin_identity)
        .await
        .unwrap()
        .unwrap()
        .protects(common::now_ms()));
    response(command(stack, 'm', &["unpin", &catalog_id, &table_id, &pin_id]));
    assert!(!pins
        .get(context.catalog, table, pin_identity)
        .await
        .unwrap()
        .unwrap()
        .protects(common::now_ms()));
}

async fn tombstone_head(store: &RoutedCatalogStore, context: CatalogContext, table: TableId) {
    let key = head_key(context.catalog, table);
    let previous = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::TableHead(mut head) = StorageRecord::decode(&key, &previous.bytes).unwrap() else {
        panic!("table head");
    };
    head.lifecycle = TableLifecycle::Tombstone;
    head.operation_fence += 1;
    head.pending_operation = Some(OperationId::random());
    let next = StorageRecord::TableHead(head).encode().unwrap();
    let key = key.encode().unwrap();
    store
        .compare_exchange(
            &key,
            Some(&previous.bytes),
            &next,
            mutation_identity(&key, Some(&previous.bytes), &next),
        )
        .await
        .unwrap();
}

async fn seed_files(store: &RoutedCatalogStore, context: CatalogContext, table: TableId, count: usize) {
    for index in 0..count {
        let payload_json = format!("{{\"index\":{index}}}");
        let file = FileRecord {
            file: FileId::random(),
            location: TableLocation {
                catalog: context.catalog,
                table,
            }
            .file(&format!("metadata/backlog-{index}.json"))
            .unwrap(),
            kind: FileKind::Metadata,
            format: ContentFormat::Json,
            length: payload_json.len() as u64,
            digest: Sha256::digest(payload_json.as_bytes()).into(),
            content: FileContent::select_inline(FileKind::Metadata, payload_json.as_bytes()).unwrap(),
            hint: None,
        };
        let key = file_key(context.catalog, file.file).encode().unwrap();
        let bytes = StorageRecord::File(Box::new(file)).encode().unwrap();
        store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
    }
}

async fn seed_purge_marker(store: &RoutedCatalogStore, context: CatalogContext, table: TableId) {
    tombstone_head(store, context, table).await;
    let key = head_key(context.catalog, table);
    let head = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &head.bytes).unwrap() else {
        panic!("table head");
    };
    let marker = TablePurgeTask {
        activation_epoch: context.activation_epoch,
        head: *head,
    };
    let encoded = marker.key().encode().unwrap();
    let bytes = StorageRecord::TablePurgeTask(Box::new(marker)).encode().unwrap();
    store
        .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
        .await
        .unwrap();
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
    let live = command(&stack, 'm', &["start-table", &identity, &table_id]);
    assert!(!live.status.success());
    assert!(String::from_utf8_lossy(&live.stderr).contains("live-table GC is disabled"));

    check_operator_pin(&stack, store.clone(), context, table).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enabled_scheduler_admits_durable_purge_markers_once() {
    let stack = common::TestIcebergStack::start().await;
    let (store, context, table) = seed_table(&stack).await;
    seed_purge_marker(store.as_ref(), context, table).await;
    let server = process::TestIcebergProcess::start_with_gc(&stack.cluster.mgmt_endpoints, true).await;
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
    drop(server);
    let restarted = process::TestIcebergProcess::start_with_gc(&stack.cluster.mgmt_endpoints, true).await;
    let resumed = repository.task(context.catalog, identity).await.unwrap().unwrap();
    assert_eq!(resumed.created_ms, task.created_ms);
    assert_eq!(resumed.head, task.head);
    drop(restarted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enabled_scheduler_admits_and_advances_completed_clear() {
    let stack = common::TestIcebergStack::start().await;
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
    let now_ms = common::now_ms();
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
    let server = process::TestIcebergProcess::start_with_gc_settings(
        &stack.cluster.mgmt_endpoints,
        true,
        &[("CROWDB_ICEBERG_GC_PAGE_ITEMS", "1")],
    )
    .await;
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
    drop(server);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the pinned PyIceberg environment"]
async fn official_sdk_foreground_progresses_under_gc_backlog() {
    let python = std::env::var_os("CROWDB_ICEBERG_E2E_PYTHON")
        .expect("run with the pinned iceberg-e2e pixi environment");
    let stack = common::TestIcebergStack::start().await;
    let (store, context, table) = seed_table(&stack).await;
    seed_files(store.as_ref(), context, table, 128).await;
    seed_purge_marker(store.as_ref(), context, table).await;
    let server = process::TestIcebergProcess::start_with_gc_settings(
        &stack.cluster.mgmt_endpoints,
        true,
        &[("CROWDB_ICEBERG_GC_PAGE_ITEMS", "1")],
    )
    .await;
    let script = r#"import sys
from concurrent.futures import ThreadPoolExecutor
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

def run(worker):
    catalog = load_catalog(f"gc-{worker}", type="rest", uri=sys.argv[1], token="w" * 32)
    namespace = (f"gc-pressure-{worker}",)
    catalog.create_namespace(namespace)
    for index in range(3):
        identifier = namespace + (f"events-{index}",)
        table = catalog.create_table(identifier, Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=True)))
        table.transaction().set_properties({"gc-probe": str(index)}).commit_transaction()
        loaded = catalog.load_table(identifier)
        assert loaded.properties["gc-probe"] == str(index)
        response = requests.get(
            f"{sys.argv[1]}/v1/namespaces/{namespace[0]}/tables/{identifier[1]}/credentials",
            headers={"Authorization": "Bearer " + "w" * 32},
            timeout=5,
        )
        response.raise_for_status()
        loaded.io.properties.update(response.json()["storage-credentials"][0]["config"])
        with loaded.io.new_input(loaded.metadata_location).open() as stream:
            assert stream.read().startswith(b"{")
        catalog.drop_table(identifier)
    catalog.drop_namespace(namespace)

with ThreadPoolExecutor(max_workers=4) as executor:
    list(executor.map(run, range(4)))
"#;
    let mut client = std::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .arg(format!("http://{}", server.address))
        .spawn()
        .unwrap();
    let repository = GcRepository::new(store);
    let identity = OperationId::from_bytes(table.as_bytes()).unwrap();
    let overlapped = tokio::time::timeout(std::time::Duration::from_secs(90), async {
        loop {
            if client.try_wait().unwrap().is_some() {
                break false;
            }
            if repository
                .task(context.catalog, identity)
                .await
                .unwrap()
                .is_some_and(|task| task.revision > 1)
            {
                break true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let status = tokio::time::timeout(std::time::Duration::from_secs(90), async {
        loop {
            if let Some(status) = client.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert!(status.success(), "official SDK foreground operations failed");
    assert!(
        overlapped,
        "GC did not advance while the SDK requests were active"
    );
}
