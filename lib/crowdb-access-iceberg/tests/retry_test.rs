#[path = "common/store.rs"]
mod common;

use common::TestStore;
use crowdb_access_iceberg::catalog::{CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{
    ManagementAction, ManagementRequest, RequestIdentity, RetryAdmission, RetryLedger, RetryRecord,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[tokio::test]
async fn lost_result_reply_repairs_binding_before_replay() {
    let (store, _, request) = setup().await;
    let ledger = RetryLedger::new(store.clone());
    ledger.begin(request.clone(), 100).await.unwrap();
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(ledger
        .finish(request.clone(), 409, b"conflict".to_vec(), 101)
        .await
        .is_err());
    let ledger = RetryLedger::new(store.clone());
    assert!(matches!(
        ledger.begin(request.clone(), 102).await.unwrap(),
        RetryAdmission::Replay(_)
    ));
    let key = crowdb_access_iceberg::operation::ledger_key(
        crowdb_access_iceberg::key::SystemScope::RetryBinding,
        request.identity.operation,
    )
    .unwrap();
    let values = store.values.load();
    let record = crowdb_access_iceberg::record::StorageRecord::decode(
        &key,
        &values.get(&key.encode().unwrap()).unwrap().bytes,
    )
    .unwrap();
    let crowdb_access_iceberg::record::StorageRecord::Retry(binding) = record else {
        panic!("retry binding")
    };
    assert_eq!(binding.status, 409);
    assert!(binding.body.is_empty());
}

async fn setup() -> (Arc<TestStore>, CatalogRepository, RetryRecord) {
    let store = Arc::new(TestStore::default());
    let repository = CatalogRepository::new(
        store.clone(),
        ClearBounds {
            request_ms: 1,
            root_lease_ms: 0,
            delegated_access_ms: 0,
            clock_skew_ms: 1,
        },
    )
    .unwrap();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: 100,
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "catalog".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let record = RetryRecord {
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "reader".into(),
        route: "POST /namespaces".into(),
        digest: [1; 32],
        context: repository.status().await.unwrap().0.context,
        retained_until_ms: 0,
        status: 0,
        body: Vec::new(),
    };
    (store, repository, record)
}

#[tokio::test]
async fn final_conflicts_replay_but_server_errors_resume() {
    let (store, _, request) = setup().await;
    let ledger = RetryLedger::new(store.clone());
    assert!(matches!(
        ledger.begin(request.clone(), 100).await.unwrap(),
        RetryAdmission::New(_)
    ));
    assert!(!ledger
        .finish(request.clone(), 503, b"busy".to_vec(), 101)
        .await
        .unwrap());
    assert!(matches!(
        RetryLedger::new(store.clone())
            .begin(request.clone(), 102)
            .await
            .unwrap(),
        RetryAdmission::Resume(_)
    ));
    assert!(ledger
        .finish(request.clone(), 409, b"conflict".to_vec(), 103)
        .await
        .unwrap());
    let RetryAdmission::Replay(result) = RetryLedger::new(store).begin(request, 104).await.unwrap() else {
        panic!("expected replay")
    };
    assert_eq!(result.status, 409);
    assert_eq!(result.body, b"conflict");
}

#[tokio::test]
async fn changed_principal_digest_and_retired_domain_never_replay() {
    let (store, repository, request) = setup().await;
    let ledger = RetryLedger::new(store);
    ledger.begin(request.clone(), 100).await.unwrap();
    ledger
        .finish(request.clone(), 200, b"old".to_vec(), 101)
        .await
        .unwrap();
    let mut changed = request.clone();
    changed.principal = "another".into();
    assert!(matches!(
        ledger.begin(changed, 102).await,
        Err(CatalogError::Conflict)
    ));
    let mut changed = request.clone();
    changed.digest = [2; 32];
    assert!(matches!(
        ledger.begin(changed, 102).await,
        Err(CatalogError::Conflict)
    ));
    let clear = ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "clearer".into(),
        action: ManagementAction::Clear,
        expected_epoch: 1,
        display_name: "empty".into(),
        confirmation: Some(request.context.catalog),
        capabilities: None,
    };
    let _ = repository
        .execute(clear.clone(), ManagementPrivilege::Clear, 103)
        .await;
    repository
        .execute(clear, ManagementPrivilege::Clear, 200)
        .await
        .unwrap();
    assert!(matches!(
        ledger.begin(request.clone(), 201).await,
        Err(CatalogError::Conflict)
    ));
    let mut rebound = request;
    rebound.context = repository.status().await.unwrap().0.context;
    assert!(matches!(
        ledger.begin(rebound, 201).await,
        Err(CatalogError::Conflict)
    ));
}

#[test]
fn uuidv7_wire_keys_validate_version_and_clock() {
    let valid = "00000000-0064-7000-8000-000000000001";
    assert_eq!(RequestIdentity::parse(valid, 100).unwrap().issued_ms, 100);
    assert!(RequestIdentity::parse("00000000-0064-4000-8000-000000000001", 100).is_err());
    assert!(RequestIdentity::parse("00000001-0064-7000-8000-000000000001", 100).is_err());
    assert!(RequestIdentity::parse("not-a-uuid", 100).is_err());
}

#[tokio::test]
async fn collisions_preserve_unfinished_and_retained_results_then_admit_fresh_keys() {
    use crowdb_access_iceberg::key::SystemScope;
    use crowdb_access_iceberg::operation::{ledger_key, RETRY_WINDOW_MS};
    let (store, _, request) = setup().await;
    let ledger = RetryLedger::new(store);
    let target = ledger_key(SystemScope::RetryBinding, request.identity.operation).unwrap();
    let mut collision = request.clone();
    collision.identity.operation = (1_u128..1_000_000)
        .find_map(|number| {
            let candidate = OperationId::from_bytes(&number.to_be_bytes()).unwrap();
            (candidate != request.identity.operation
                && ledger_key(SystemScope::RetryBinding, candidate).unwrap() == target)
                .then_some(candidate)
        })
        .expect("colliding bounded slot");
    ledger.begin(request.clone(), 100).await.unwrap();
    assert!(matches!(
        ledger.begin(collision.clone(), 101).await,
        Err(CatalogError::Busy)
    ));
    let expired = 100 + RETRY_WINDOW_MS + 30_001;
    collision.identity.issued_ms = expired;
    assert!(matches!(
        ledger.begin(collision.clone(), expired).await,
        Err(CatalogError::Busy)
    ));
    ledger
        .finish(request.clone(), 204, Vec::new(), 101)
        .await
        .unwrap();
    assert!(matches!(
        ledger.begin(collision.clone(), 102).await,
        Err(CatalogError::Busy)
    ));
    assert!(matches!(
        ledger.begin(collision, expired).await.unwrap(),
        RetryAdmission::New(_)
    ));
    assert!(ledger.begin(request, expired).await.is_err());
}

#[tokio::test]
async fn large_retry_bodies_recover_after_every_page_manifest_and_binding_reply_loss() {
    use crowdb_access_iceberg::operation::PAYLOAD_PAGE_BYTES;
    let body = vec![31; PAYLOAD_PAGE_BYTES * 2 + 1];
    for lost_write in 1..=5 {
        let (store, _, mut request) = setup().await;
        request.principal = "writer".into();
        let ledger = RetryLedger::new(store.clone());
        ledger.begin(request.clone(), 100).await.unwrap();
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + lost_write, Ordering::SeqCst);
        assert!(ledger
            .finish(request.clone(), 200, body.clone(), 101)
            .await
            .is_err());
        let recovered = RetryLedger::new(store.clone());
        match recovered.begin(request.clone(), 102).await.unwrap() {
            RetryAdmission::Resume(_) => {
                recovered
                    .finish(request.clone(), 200, body.clone(), 103)
                    .await
                    .unwrap();
            }
            RetryAdmission::Replay(result) => assert_eq!(result.body, body),
            RetryAdmission::New(_) => panic!("existing identity must not be readmitted"),
        }
        let RetryAdmission::Replay(result) = recovered.begin(request.clone(), 104).await.unwrap() else {
            panic!("replay")
        };
        assert_eq!(result.body, body);
        assert_eq!(result.principal, "writer");
        let mut changed = request.clone();
        changed.principal = "reader".into();
        assert!(matches!(
            recovered.begin(changed, 104).await,
            Err(CatalogError::Conflict)
        ));
        let writes = store.writes.load(Ordering::SeqCst);
        assert!(matches!(
            recovered.finish(request, 200, vec![0; body.len()], 104).await,
            Err(CatalogError::Conflict)
        ));
        assert_eq!(store.writes.load(Ordering::SeqCst), writes);
    }
}
