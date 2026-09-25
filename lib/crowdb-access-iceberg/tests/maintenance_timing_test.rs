#[path = "common/store.rs"]
mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crowdb_access_iceberg::catalog::{
    CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege, RootState,
};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};

#[tokio::test]
async fn maintenance_observation_includes_time_spent_awaiting_fence_cas() {
    let store = Arc::new(common::TestStore::default());
    let repository = CatalogRepository::new(store.clone(), ClearBounds::default()).unwrap();
    let mut request = ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::from_bytes(&[1; 16]).unwrap(),
            issued_ms: 100,
        },
        principal: "operator".into(),
        action: ManagementAction::Initialize,
        expected_epoch: 0,
        display_name: "catalog".into(),
        confirmation: None,
        capabilities: None,
    };
    let authority = repository
        .execute(request.clone(), ManagementPrivilege::Manage, 100)
        .await
        .unwrap();
    request.identity.operation = OperationId::from_bytes(&[2; 16]).unwrap();
    request.action = ManagementAction::Clear;
    request.expected_epoch = 1;
    request.confirmation = Some(authority.catalog);
    store.fencing_delay_ms.store(25, Ordering::SeqCst);
    assert!(matches!(
        repository.execute(request, ManagementPrivilege::Clear, 101).await,
        Err(CatalogError::Busy)
    ));
    let RootState::Published(transition) = repository.status().await.unwrap().0.state else {
        panic!("published maintenance")
    };
    assert!(transition.maintenance_observed_ms >= 126);
    assert_eq!(
        transition.complete_after_ms,
        transition.maintenance_observed_ms + 11_000
    );
}
