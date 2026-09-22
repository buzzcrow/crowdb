#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace_recovery_store.rs"]
mod recovery_store;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use crowdb_access_iceberg::catalog::CatalogError;
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::namespace::{
    NamespaceCreateRequest, NamespaceCreator, NamespaceDropRequest, NamespaceDropper, NamespaceIdentifier,
    NamespaceProperties, NamespacePropertyRequest, NamespaceRepository, PropertyChanges,
};
use crowdb_access_iceberg::operation::RequestIdentity;
use fixture::TestNamespace;

fn identity() -> RequestIdentity {
    RequestIdentity {
        operation: OperationId::random(),
        issued_ms: 100,
    }
}

#[tokio::test]
async fn bounded_sweep_recovers_operations_without_client_retries() {
    use crowdb_access_iceberg::namespace::{NamespaceJournal, NamespacePhase, NamespaceRecovery};
    let (fixture, _) = setup().await;
    let mut identities = Vec::new();
    for index in 0..9 {
        let mut request = creation(&fixture);
        request.identifier = NamespaceIdentifier::new(vec!["parent".into(), index.to_string()]).unwrap();
        fixture
            .store
            .fail_after
            .store(fixture.store.writes.load(Ordering::SeqCst) + 3, Ordering::SeqCst);
        assert!(NamespaceCreator::new(fixture.store.clone())
            .create(&request)
            .await
            .is_err());
        identities.push(request.identity.operation);
    }
    let recovery = NamespaceRecovery::new(fixture.store.clone());
    let mut cursor = None;
    let mut visited = 0;
    for _ in 0..4 {
        let page = recovery.recover_page(fixture.context, cursor).await.unwrap();
        assert!(page.failures.is_empty(), "{:?}", page.failures);
        assert_eq!(page.deferred, 0);
        assert!(page.completed <= 4);
        visited += page.completed;
        cursor = page.continuation;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none());
    assert_eq!(visited, 9);
    let journal = NamespaceJournal::new(fixture.store.clone());
    for identity in identities {
        assert_eq!(
            journal
                .load(fixture.context, identity)
                .await
                .unwrap()
                .unwrap()
                .phase,
            NamespacePhase::Complete
        );
    }
}

#[tokio::test]
async fn recovery_cursor_rejects_foreign_catalog_and_retired_context() {
    use crowdb_access_iceberg::key::{CatalogId, CatalogScope, IcebergKey};
    use crowdb_access_iceberg::namespace::{NamespaceRecovery, NamespaceRecoveryScan};
    use crowdb_chunk_kv_client::MultiScanContinuation;
    let (fixture, _) = setup().await;
    let scan = NamespaceRecoveryScan {
        catalog: fixture.context.catalog,
        continuation: None,
    }
    .request()
    .unwrap();
    let cursor = MultiScanContinuation {
        direction: scan.direction,
        original_start: scan.start,
        original_end: scan.end,
        last_key: IcebergKey::Catalog {
            catalog: fixture.context.catalog,
            scope: CatalogScope::NamespaceOperation,
            suffix: OperationId::random().as_bytes().to_vec(),
        }
        .encode()
        .unwrap(),
        catalog_generation: 1,
    };
    assert!(NamespaceRecoveryScan {
        catalog: CatalogId::random(),
        continuation: Some(cursor)
    }
    .request()
    .is_err());
    let mut retired = fixture.context;
    retired.activation_epoch += 1;
    assert!(matches!(
        NamespaceRecovery::new(fixture.store.clone())
            .recover_page(retired, None)
            .await,
        Err(CatalogError::Conflict)
    ));
}

async fn setup() -> (TestNamespace, NamespaceIdentifier) {
    let fixture = TestNamespace::new().await;
    let parent = fixture.authority(None, &["parent"]);
    fixture.publish(&parent).await;
    (fixture, parent.identifier)
}

async fn update(fixture: &TestNamespace, identifier: NamespaceIdentifier) {
    let request = NamespacePropertyRequest {
        context: fixture.context,
        identity: identity(),
        principal: "writer".into(),
        identifier,
        changes: PropertyChanges {
            removals: Vec::new(),
            updates: BTreeMap::from([("updated".into(), "yes".into())]),
        },
    };
    let repository = NamespaceRepository::new(fixture.store.clone());
    for _ in 0..4 {
        match repository.update_properties(&request).await {
            Ok(Some(outcome)) => {
                assert_eq!(outcome.status, 200);
                let authority = repository
                    .load(fixture.context, &request.identifier)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(authority.pending_operation, None);
                assert_eq!(authority.properties.entries().get("updated").unwrap(), "yes");
                return;
            }
            Err(CatalogError::Busy) => {}
            other => panic!("unexpected property recovery: {other:?}"),
        }
    }
    panic!("property recovery made no progress");
}

fn creation(fixture: &TestNamespace) -> NamespaceCreateRequest {
    NamespaceCreateRequest {
        context: fixture.context,
        identity: identity(),
        principal: "writer".into(),
        identifier: NamespaceIdentifier::new(vec!["parent".into(), "child".into()]).unwrap(),
        properties: NamespaceProperties::default(),
    }
}

#[tokio::test]
async fn property_writer_settles_every_interrupted_parent_admission() {
    let (baseline, _) = setup().await;
    let before = baseline.store.writes.load(Ordering::SeqCst);
    NamespaceCreator::new(baseline.store.clone())
        .create(&creation(&baseline))
        .await
        .unwrap();
    let writes = baseline.store.writes.load(Ordering::SeqCst) - before;
    for offset in 1..=writes {
        let (fixture, parent) = setup().await;
        let request = creation(&fixture);
        fixture.store.fail_after.store(
            fixture.store.writes.load(Ordering::SeqCst) + offset,
            Ordering::SeqCst,
        );
        assert!(NamespaceCreator::new(fixture.store.clone())
            .create(&request)
            .await
            .is_err());
        update(&fixture, parent).await;
        assert_eq!(
            NamespaceCreator::new(fixture.store.clone())
                .create(&request)
                .await
                .unwrap()
                .status,
            200
        );
        update(&fixture, request.identifier).await;
    }
}

#[tokio::test]
async fn property_writer_settles_every_interrupted_nonempty_drop() {
    let (baseline, parent) = setup().await;
    NamespaceCreator::new(baseline.store.clone())
        .create(&creation(&baseline))
        .await
        .unwrap();
    let request = NamespaceDropRequest {
        context: baseline.context,
        identity: identity(),
        principal: "writer".into(),
        identifier: parent,
    };
    let before = baseline.store.writes.load(Ordering::SeqCst);
    NamespaceDropper::new(baseline.store.clone())
        .drop_namespace(&request)
        .await
        .unwrap();
    let writes = baseline.store.writes.load(Ordering::SeqCst) - before;
    for offset in 1..=writes {
        let (fixture, parent) = setup().await;
        NamespaceCreator::new(fixture.store.clone())
            .create(&creation(&fixture))
            .await
            .unwrap();
        let request = NamespaceDropRequest {
            context: fixture.context,
            identity: identity(),
            principal: "writer".into(),
            identifier: parent.clone(),
        };
        fixture.store.fail_after.store(
            fixture.store.writes.load(Ordering::SeqCst) + offset,
            Ordering::SeqCst,
        );
        let dropper = NamespaceDropper::new(fixture.store.clone());
        assert!(dropper.drop_namespace(&request).await.is_err());
        update(&fixture, parent).await;
        assert_eq!(
            dropper.drop_namespace(&request).await.unwrap().unwrap().status,
            409
        );
    }
}
