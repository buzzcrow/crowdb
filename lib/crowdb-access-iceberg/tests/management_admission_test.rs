#[path = "common/store.rs"]
mod common;

use std::sync::Arc;

use common::TestStore;
use crowdb_access_iceberg::catalog::{CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege};
use crowdb_access_iceberg::key::{IcebergKey, OperationId, SystemScope};
use crowdb_access_iceberg::operation::{
    ledger_key, ManagementAction, ManagementPhase, ManagementRequest, RequestIdentity,
};
use crowdb_access_iceberg::record::StorageRecord;

fn request(seed: u8, action: ManagementAction, epoch: u64, name: &str) -> ManagementRequest {
    ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::from_bytes(&[seed; 16]).unwrap(),
            issued_ms: 100,
        },
        principal: "operator".into(),
        action,
        expected_epoch: epoch,
        display_name: name.into(),
        confirmation: None,
        capabilities: None,
    }
}

#[tokio::test]
async fn concurrent_renames_from_one_root_publish_one_name_without_moving_keys() {
    let store = Arc::new(TestStore {
        fencing_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..TestStore::default()
    });
    let first = CatalogRepository::new(store.clone(), ClearBounds::default()).unwrap();
    let second = CatalogRepository::new(store.clone(), ClearBounds::default()).unwrap();
    let authority = first
        .execute(
            request(1, ManagementAction::Initialize, 0, "original"),
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let prefix = IcebergKey::catalog_range(authority.catalog);
    let left = request(2, ManagementAction::Rename, 1, "left");
    let right = request(3, ManagementAction::Rename, 1, "right");
    let (left_result, right_result) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(
            first.execute(left.clone(), ManagementPrivilege::Manage, 101),
            second.execute(right.clone(), ManagementPrivilege::Manage, 101)
        )
    })
    .await
    .unwrap();
    assert!(matches!(
        left_result,
        Ok(_) | Err(CatalogError::Conflict | CatalogError::Busy)
    ));
    assert!(matches!(
        right_result,
        Ok(_) | Err(CatalogError::Conflict | CatalogError::Busy)
    ));
    first.recover(200).await.unwrap();
    let current = first.status().await.unwrap().1;
    assert_eq!(current.name_generation, 2);
    assert_eq!(IcebergKey::catalog_range(current.catalog), prefix);
    let (winner, loser) = if current.display_name == "left" {
        (left, right)
    } else {
        assert_eq!(current.display_name, "right");
        (right, left)
    };
    assert_eq!(
        first
            .execute(winner, ManagementPrivilege::Manage, 201)
            .await
            .unwrap(),
        current
    );
    assert!(matches!(
        second.execute(loser, ManagementPrivilege::Manage, 201).await,
        Err(CatalogError::Conflict)
    ));
}

#[tokio::test]
async fn stale_clear_has_an_exact_durable_conflict_audit() {
    let store = Arc::new(TestStore::default());
    let repository = CatalogRepository::new(store.clone(), ClearBounds::default()).unwrap();
    let authority = repository
        .execute(
            request(1, ManagementAction::Initialize, 0, "original"),
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let mut clear = request(2, ManagementAction::Clear, 2, "empty");
    clear.confirmation = Some(authority.catalog);
    for now in [101, 102] {
        assert!(matches!(
            repository
                .execute(clear.clone(), ManagementPrivilege::Clear, now)
                .await,
            Err(CatalogError::Conflict)
        ));
    }
    assert_eq!(repository.status().await.unwrap().1, authority);
    let key = ledger_key(SystemScope::Audit, clear.identity.operation).unwrap();
    let values = store.values.load();
    let StorageRecord::Management(audit) =
        StorageRecord::decode(&key, &values.get(&key.encode().unwrap()).unwrap().bytes).unwrap()
    else {
        panic!("audit record")
    };
    assert_eq!(audit.phase, ManagementPhase::Conflict);
    assert_eq!(audit.request, clear);
}
