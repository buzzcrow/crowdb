#[path = "common/iceberg_background.rs"]
mod background;
#[path = "common/iceberg_stack.rs"]
mod common;
#[path = "common/iceberg_creation.rs"]
mod creation;
#[path = "common/iceberg_drop.rs"]
mod dropping;
#[path = "common/iceberg_fault.rs"]
mod fault;
#[path = "common/iceberg_journal.rs"]
mod journal;
#[path = "common/iceberg_namespace.rs"]
mod namespace;
#[path = "common/iceberg_process.rs"]
mod process;
#[path = "common/iceberg_property.rs"]
mod property;

use std::sync::Arc;
use std::time::Duration;

use common::{now_ms, TestIcebergStack};
use crowdb_access_iceberg::catalog::{
    CatalogAuthority, CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege,
};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};

fn request(
    action: ManagementAction,
    name: &str,
    previous: Option<(u64, &CatalogAuthority)>,
) -> ManagementRequest {
    ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::from_bytes(
                &(u128::from(previous.map_or(0, |(epoch, _)| epoch)) * 8 + u128::from(action as u8) + 1)
                    .to_be_bytes(),
            )
            .unwrap(),
            issued_ms: now_ms(),
        },
        principal: "clearer".into(),
        action,
        display_name: name.into(),
        expected_epoch: previous.map_or(0, |(epoch, _)| epoch),
        confirmation: previous
            .filter(|_| action == ManagementAction::Clear)
            .map(|(_, authority)| authority.catalog),
        capabilities: None,
    }
}

async fn execute(repository: &CatalogRepository, request: ManagementRequest) -> CatalogAuthority {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match repository
                .execute(request.clone(), ManagementPrivilege::Clear, now_ms())
                .await
            {
                Ok(authority) => return authority,
                Err(CatalogError::Busy) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(error) => panic!("management failed: {error:?}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_recovery_survives_real_chunk_kv_restart() {
    let mut stack = TestIcebergStack::start().await;
    let bounds = ClearBounds {
        request_ms: 500,
        root_lease_ms: 0,
        delegated_access_ms: 0,
        clock_skew_ms: 10,
    };
    let repository = Arc::new(CatalogRepository::new(stack.store().await, bounds).unwrap());
    let initialize = request(ManagementAction::Initialize, "original", None);
    let original = execute(&repository, initialize.clone()).await;
    let frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let second_frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    frontend.check_official_reads();
    second_frontend.check_official_reads();
    let denied = process::command(&stack.cluster.mgmt_endpoints)
        .env("CROWDB_ICEBERG_TOKEN", "r".repeat(32))
        .arg("status")
        .output()
        .unwrap();
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stderr).contains("management privilege"));
    for action in ["status", "initialize", "rename", "clear"] {
        let denied = process::command(&stack.cluster.mgmt_endpoints)
            .env("CROWDB_ICEBERG_TOKEN", "w".repeat(32))
            .arg(action)
            .output()
            .unwrap();
        assert!(!denied.status.success());
        assert!(String::from_utf8_lossy(&denied.stderr).contains("management privilege"));
    }
    let rename = request(ManagementAction::Rename, "renamed", Some((1, &original)));
    let renamed = execute(&repository, rename).await;
    assert_eq!(renamed.catalog, original.catalog);
    let clear = request(ManagementAction::Clear, "replacement", Some((1, &renamed)));
    assert!(matches!(
        repository
            .execute(clear.clone(), ManagementPrivilege::Clear, now_ms())
            .await,
        Err(CatalogError::Busy)
    ));
    drop(repository);
    stack.chunk_kv.restart().await;
    let repository = CatalogRepository::new(stack.store().await, bounds).unwrap();
    let (recovering, _) = repository.status().await.unwrap();
    if let crowdb_access_iceberg::catalog::RootState::Published(transition) = recovering.state {
        let remaining = transition.complete_after_ms.saturating_sub(now_ms());
        tokio::time::sleep(Duration::from_millis(remaining)).await;
    }
    repository.recover(now_ms()).await.unwrap();
    let replacement = execute(&repository, clear.clone()).await;
    assert_ne!(replacement.catalog, original.catalog);
    let second = request(ManagementAction::Clear, "second", Some((2, &replacement)));
    let latest = execute(&repository, second).await;
    assert_ne!(latest.catalog, replacement.catalog);
    assert_eq!(execute(&repository, clear).await, replacement);
    assert_eq!(execute(&repository, initialize).await, original);
    assert_eq!(repository.status().await.unwrap().0.context.activation_epoch, 3);
    verify_retry_scan(&stack, &repository).await;
    drop(frontend);
    drop(second_frontend);
    namespace::verify_name_index(&stack, latest.catalog).await;
    let frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let second_frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    background::verify(stack.store().await, repository.status().await.unwrap().0.context).await;
    frontend.check_official_reads();
    second_frontend.check_official_reads();
    drop(frontend);
    drop(second_frontend);
    journal::verify_recovery(&mut stack, repository.status().await.unwrap().0.context).await;
    verify_interrupted_clear(&stack, &repository).await;
    let frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let second_frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    frontend.check_official_reads();
    second_frontend.check_official_reads();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_functional_crud_survives_native_storage_and_listener_restart() {
    let mut stack = TestIcebergStack::start().await;
    let repository = CatalogRepository::new(
        stack.store().await,
        ClearBounds {
            request_ms: 300_000,
            ..ClearBounds::default()
        },
    )
    .unwrap();
    execute(
        &repository,
        request(ManagementAction::Initialize, "functional", None),
    )
    .await;
    common::activate(&repository).await;
    let frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let second_frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    frontend.check_official_client();
    second_frontend.check_official_client();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let created = client
        .post(format!("http://{}/v1/namespaces", frontend.address))
        .bearer_auth("w".repeat(32))
        .header("content-type", "application/json")
        .body(r#"{"namespace":["persisted"],"properties":{"owner":"before-restart"}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200, "{}", created.text().await.unwrap());
    verify_retained_namespace(&client, &second_frontend).await;
    drop(frontend);
    drop(second_frontend);
    stack.chunk_kv.restart().await;
    let frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let second_frontend = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    for listener in [&frontend, &second_frontend] {
        verify_retained_namespace(&client, listener).await;
        listener.check_official_client();
    }
}

async fn verify_retained_namespace(client: &reqwest::Client, listener: &process::TestIcebergProcess) {
    let response = client
        .get(format!("http://{}/v1/namespaces/persisted", listener.address))
        .bearer_auth("r".repeat(32))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["namespace"], serde_json::json!(["persisted"]));
    assert_eq!(body["properties"], serde_json::json!({"owner":"before-restart"}));
}

async fn verify_interrupted_clear(stack: &TestIcebergStack, repository: &CatalogRepository) {
    for mode in [1, 2] {
        let (root, authority) = repository.status().await.unwrap();
        let clear = request(
            ManagementAction::Clear,
            "recovered",
            Some((root.context.activation_epoch, &authority)),
        );
        let faulty = CatalogRepository::new(
            Arc::new(fault::TestFaultStore {
                inner: stack.store().await,
                mode: std::sync::atomic::AtomicU8::new(mode),
            }),
            authority.admission_bounds,
        )
        .unwrap();
        assert!(matches!(
            faulty
                .execute(clear.clone(), ManagementPrivilege::Clear, now_ms())
                .await,
            Err(CatalogError::Store(_))
        ));
        drop(faulty);
        let recovered = execute(repository, clear.clone()).await;
        assert_ne!(recovered.catalog, authority.catalog);
        assert_eq!(
            repository.status().await.unwrap().0.context.activation_epoch,
            root.context.activation_epoch + 1
        );
        assert_eq!(execute(repository, clear).await, recovered);
    }
}

async fn verify_retry_scan(stack: &TestIcebergStack, repository: &CatalogRepository) {
    use crowdb_access_iceberg::key::IcebergKey;
    use crowdb_access_iceberg::operation::{RetryAdmission, RetryLedger, RetryRecord};
    use crowdb_chunk_kv_client::MultiScanRequest;
    use crowdb_protocol::chunk_kv::ScanDirection;
    let store = stack.store().await;
    let ledger = RetryLedger::new(store.clone());
    let context = repository.status().await.unwrap().0.context;
    for _ in 0..3 {
        let request = RetryRecord {
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: now_ms(),
            },
            principal: "reader".into(),
            route: "test mutation".into(),
            digest: [1; 32],
            context,
            retained_until_ms: 0,
            status: 0,
            body: Vec::new(),
        };
        assert!(matches!(
            ledger.begin(request.clone(), now_ms()).await.unwrap(),
            RetryAdmission::New(_)
        ));
        assert!(!ledger
            .finish(request.clone(), 503, Vec::new(), now_ms())
            .await
            .unwrap());
        assert!(matches!(
            ledger.begin(request.clone(), now_ms()).await.unwrap(),
            RetryAdmission::Resume(_)
        ));
        ledger
            .finish(request.clone(), 409, b"conflict".to_vec(), now_ms())
            .await
            .unwrap();
        let RetryAdmission::Replay(result) = RetryLedger::new(stack.store().await)
            .begin(request, now_ms())
            .await
            .unwrap()
        else {
            panic!("durable result replay")
        };
        assert_eq!(result.status, 409);
        assert_eq!(result.body, b"conflict");
    }
    let range = IcebergKey::catalog_range(context.catalog);
    let mut scan = MultiScanRequest {
        start: Some(range.start.clone()),
        end: Some(range.end.clone()),
        direction: ScanDirection::Forward,
        max_items: 1,
        max_bytes: 64 * 1024,
        continuation: None,
    };
    let mut count = 0;
    loop {
        let page = store.scan(scan.clone()).await.unwrap();
        assert!(page.items.len() <= 1);
        for item in &page.items {
            assert!(range.contains(&item.key));
        }
        count += page.items.len();
        scan.continuation = page.continuation;
        if scan.continuation.is_none() {
            break;
        }
        assert!(count <= 4);
    }
    assert_eq!(count, 4);
    scan.start = None;
    assert!(store.scan(scan).await.is_err());
}
