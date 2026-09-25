#[path = "common/store.rs"]
mod common;

use common::TestStore;
use crowdb_access_iceberg::catalog::{
    Capabilities, CatalogError, CatalogRepository, ClearBounds, ManagementPrivilege, RootState,
};
use crowdb_access_iceberg::key::OperationId;
use crowdb_access_iceberg::operation::{ManagementAction, ManagementRequest, RequestIdentity};
use std::sync::{atomic::Ordering, Arc};

fn repository(store: &Arc<TestStore>) -> CatalogRepository {
    CatalogRepository::new(
        store.clone(),
        ClearBounds {
            root_lease_ms: 0,
            request_ms: 10,
            delegated_access_ms: 0,
            clock_skew_ms: 1,
        },
    )
    .unwrap()
}

fn request(action: ManagementAction, epoch: u64, name: &str) -> ManagementRequest {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update([action as u8]);
    digest.update(epoch.to_be_bytes());
    digest.update(name.as_bytes());
    let digest = digest.finalize();
    ManagementRequest {
        identity: RequestIdentity {
            operation: OperationId::from_bytes(&digest[..16]).unwrap(),
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
async fn explicit_activation_preserves_catalog_and_replays_across_restarts() {
    let store = Arc::new(TestStore::default());
    let catalog = repository(&store);
    let initialized = catalog
        .execute(
            request(ManagementAction::Initialize, 0, "catalog"),
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let mut activate = request(ManagementAction::Activate, 1, "catalog");
    activate.capabilities = Some(Capabilities::from_bits(0x0033).unwrap());
    assert!(matches!(
        catalog
            .execute(activate.clone(), ManagementPrivilege::None, 101)
            .await,
        Err(CatalogError::Forbidden)
    ));
    let first = catalog
        .execute(activate.clone(), ManagementPrivilege::Manage, 101)
        .await
        .unwrap();
    assert_eq!(first.catalog, initialized.catalog);
    assert_eq!(first.name_generation, initialized.name_generation);
    assert_eq!(first.config_generation, initialized.config_generation + 1);
    assert_eq!(first.admission_bounds, initialized.admission_bounds);
    assert_eq!(first.capabilities.bits(), 0x0033);
    assert_eq!(catalog.status().await.unwrap().0.context.activation_epoch, 1);
    assert_eq!(
        repository(&store)
            .execute(activate.clone(), ManagementPrivilege::Manage, 102)
            .await
            .unwrap(),
        first
    );

    let mut expansion = request(ManagementAction::Activate, 1, "catalog");
    expansion.identity.operation = OperationId::random();
    expansion.capabilities = Some(Capabilities::from_bits(0x3fff).unwrap());
    let expanded = repository(&store)
        .execute(expansion, ManagementPrivilege::Manage, 103)
        .await
        .unwrap();
    assert_eq!(expanded.config_generation, first.config_generation + 1);
    assert_eq!(expanded.capabilities.bits(), 0x3fff);
    assert_eq!(
        repository(&store)
            .execute(activate, ManagementPrivilege::Manage, 104)
            .await
            .unwrap(),
        first
    );

    let mut downgrade = request(ManagementAction::Activate, 1, "catalog");
    downgrade.identity.operation = OperationId::random();
    downgrade.capabilities = Some(Capabilities::from_bits(0x0033).unwrap());
    assert!(matches!(
        repository(&store)
            .execute(downgrade, ManagementPrivilege::Manage, 105)
            .await,
        Err(CatalogError::Conflict)
    ));
    let mut clear = request(ManagementAction::Clear, 1, "replacement");
    clear.confirmation = Some(expanded.catalog);
    assert!(matches!(
        repository(&store)
            .execute(clear.clone(), ManagementPrivilege::Clear, 106)
            .await,
        Err(CatalogError::Busy)
    ));
    let RootState::Published(transition) = repository(&store).status().await.unwrap().0.state else {
        panic!("expected published maintenance");
    };
    let replacement = repository(&store)
        .execute(clear, ManagementPrivilege::Clear, transition.complete_after_ms)
        .await
        .unwrap();
    assert_ne!(replacement.catalog, expanded.catalog);
    assert_eq!(replacement.capabilities.bits(), 0);
    assert_eq!(replacement.config_generation, 1);
}

#[tokio::test]
async fn initialize_rename_clear_and_old_result_replay_preserve_identity() {
    let store = Arc::new(TestStore::default());
    let repository = repository(&store);
    let initialize = request(ManagementAction::Initialize, 0, "catalog");
    let first = repository
        .execute(initialize.clone(), ManagementPrivilege::Manage, 100)
        .await
        .unwrap();
    let renamed = repository
        .execute(
            request(ManagementAction::Rename, 1, "renamed"),
            ManagementPrivilege::Manage,
            101,
        )
        .await
        .unwrap();
    assert_eq!(renamed.catalog, first.catalog);
    assert_eq!(renamed.name_generation, 2);
    let mut clear = request(ManagementAction::Clear, 1, "empty");
    clear.confirmation = Some(first.catalog);
    assert!(matches!(
        repository
            .execute(clear.clone(), ManagementPrivilege::Clear, 102)
            .await,
        Err(CatalogError::Busy)
    ));
    let RootState::Published(transition) = repository.status().await.unwrap().0.state else {
        panic!("expected published maintenance");
    };
    assert!(transition.maintenance_observed_ms >= 102);
    assert_eq!(
        transition.complete_after_ms,
        transition.maintenance_observed_ms + 11
    );
    assert!(!transition
        .grace_elapsed(transition.complete_after_ms - 1)
        .unwrap());
    let second = repository
        .execute(
            clear.clone(),
            ManagementPrivilege::Clear,
            transition.complete_after_ms,
        )
        .await
        .unwrap();
    assert_ne!(second.catalog, first.catalog);
    assert_eq!(repository.status().await.unwrap().0.context.activation_epoch, 2);
    assert_eq!(
        repository
            .execute(clear, ManagementPrivilege::Clear, 114)
            .await
            .unwrap(),
        second
    );
    assert_eq!(
        repository
            .execute(initialize, ManagementPrivilege::Manage, 114)
            .await
            .unwrap(),
        first
    );
}

#[tokio::test]
async fn every_lost_initialize_response_recovers_on_another_server() {
    for fail_after in 1..=8 {
        let store = Arc::new(TestStore::default());
        store.fail_after.store(fail_after, Ordering::SeqCst);
        let initialize = request(ManagementAction::Initialize, 0, "catalog");
        let _ = repository(&store)
            .execute(initialize.clone(), ManagementPrivilege::Manage, 100)
            .await;
        let recovered = repository(&store)
            .execute(initialize.clone(), ManagementPrivilege::Manage, 101)
            .await
            .unwrap();
        assert_eq!(
            repository(&store)
                .execute(initialize, ManagementPrivilege::Manage, 102)
                .await
                .unwrap(),
            recovered
        );
    }
}

#[tokio::test]
async fn every_lost_activation_response_recovers_one_profile_without_replacing_tables() {
    for failure in 1..=8 {
        let store = Arc::new(TestStore::default());
        let original = repository(&store)
            .execute(
                request(ManagementAction::Initialize, 0, "catalog"),
                ManagementPrivilege::Manage,
                100,
            )
            .await
            .unwrap();
        let baseline_writes = store.writes.load(Ordering::SeqCst);
        store
            .fail_after
            .store(baseline_writes + failure, Ordering::SeqCst);
        let mut activation = request(ManagementAction::Activate, 1, "catalog");
        activation.capabilities = Some(Capabilities::from_bits(0x3fff).unwrap());
        let _ = repository(&store)
            .execute(activation.clone(), ManagementPrivilege::Manage, 101)
            .await;
        let recovered = repository(&store)
            .execute(activation.clone(), ManagementPrivilege::Manage, 102)
            .await
            .unwrap();
        assert_eq!(recovered.catalog, original.catalog);
        assert_eq!(recovered.config_generation, original.config_generation + 1);
        assert_eq!(recovered.capabilities.bits(), 0x3fff);
        assert_eq!(
            repository(&store)
                .status()
                .await
                .unwrap()
                .0
                .context
                .activation_epoch,
            1
        );
        assert_eq!(
            repository(&store)
                .execute(activation, ManagementPrivilege::Manage, 103)
                .await
                .unwrap(),
            recovered
        );
    }
}

#[tokio::test]
async fn every_lost_clear_response_preserves_one_replacement_and_persisted_grace() {
    for failure in 1..=10 {
        let store = Arc::new(TestStore::default());
        let first = repository(&store)
            .execute(
                request(ManagementAction::Initialize, 0, "catalog"),
                ManagementPrivilege::Manage,
                100,
            )
            .await
            .unwrap();
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + failure, Ordering::SeqCst);
        let mut clear = request(ManagementAction::Clear, 1, "empty");
        clear.confirmation = Some(first.catalog);
        let _ = repository(&store)
            .execute(clear.clone(), ManagementPrivilege::Clear, 101)
            .await;
        let _ = repository(&store)
            .execute(clear.clone(), ManagementPrivilege::Clear, 200)
            .await;
        let result = repository(&store)
            .execute(clear.clone(), ManagementPrivilege::Clear, 300)
            .await
            .unwrap();
        assert_ne!(result.catalog, first.catalog);
        assert_eq!(
            repository(&store)
                .status()
                .await
                .unwrap()
                .0
                .context
                .activation_epoch,
            2
        );
        assert_eq!(
            repository(&store)
                .execute(clear, ManagementPrivilege::Clear, 301)
                .await
                .unwrap(),
            result
        );
    }
}

#[tokio::test]
async fn denied_clear_and_digest_reuse_do_not_mutate() {
    let store = Arc::new(TestStore::default());
    let initialize = request(ManagementAction::Initialize, 0, "catalog");
    assert!(matches!(
        repository(&store)
            .execute(initialize.clone(), ManagementPrivilege::None, 100)
            .await,
        Err(CatalogError::Forbidden)
    ));
    let first = repository(&store)
        .execute(initialize.clone(), ManagementPrivilege::Manage, 100)
        .await
        .unwrap();
    let mut different = initialize;
    different.display_name = "different".into();
    assert!(matches!(
        repository(&store)
            .execute(different, ManagementPrivilege::Manage, 101)
            .await,
        Err(CatalogError::Conflict)
    ));
    let mut clear = request(ManagementAction::Clear, 1, "empty");
    clear.confirmation = Some(first.catalog);
    assert!(matches!(
        repository(&store)
            .execute(clear, ManagementPrivilege::Manage, 101)
            .await,
        Err(CatalogError::Forbidden)
    ));
    assert_eq!(repository(&store).status().await.unwrap().1, first);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_initializers_select_one_catalog_and_same_identity_replays() {
    for _ in 0..100 {
        concurrent_initializers().await;
    }
}

async fn concurrent_initializers() {
    let store = Arc::new(TestStore::default());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    let mut slots = std::collections::HashSet::new();
    while tasks.len() < 8 {
        let mut request = request(ManagementAction::Initialize, 0, "catalog");
        request.identity.operation = OperationId::random();
        let slot = crowdb_access_iceberg::operation::ledger_key(
            crowdb_access_iceberg::key::SystemScope::ManagementOperation,
            request.identity.operation,
        )
        .unwrap()
        .encode()
        .unwrap();
        if !slots.insert(slot) {
            continue;
        }
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let result = repository(&store)
                .execute(request.clone(), ManagementPrivilege::Manage, 100)
                .await;
            (request, result)
        }));
    }
    let mut requests = Vec::new();
    for task in tasks {
        let (request, result) = task.await.unwrap();
        match result {
            Ok(_) | Err(CatalogError::Conflict | CatalogError::Busy) => {}
            Err(error) => panic!("unexpected failure: {error}"),
        }
        requests.push(request);
    }
    repository(&store).recover(200).await.unwrap();
    let (root, authority) = repository(&store).status().await.unwrap();
    assert_eq!(root.state, RootState::Ready);
    let mut winners = 0;
    for request in requests {
        let operation = request.identity.operation;
        match repository(&store)
            .execute(request, ManagementPrivilege::Manage, 201)
            .await
        {
            Ok(result) => {
                winners += 1;
                assert_eq!(operation, root.operation);
                assert_eq!(result, authority);
            }
            Err(CatalogError::Conflict) => assert_ne!(operation, root.operation),
            Err(error) => panic!("unexpected settled outcome: {error}"),
        }
    }
    assert_eq!(winners, 1);
    assert_eq!(repository(&store).status().await.unwrap(), (root, authority));
}

#[tokio::test]
async fn delayed_fence_and_crash_restart_cannot_shorten_persisted_grace() {
    let store = Arc::new(TestStore::default());
    let bounds = ClearBounds {
        request_ms: 1000,
        root_lease_ms: 0,
        delegated_access_ms: 0,
        clock_skew_ms: 10,
    };
    let original = CatalogRepository::new(store.clone(), bounds).unwrap();
    let authority = original
        .execute(
            request(ManagementAction::Initialize, 0, "catalog"),
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let mut clear = request(ManagementAction::Clear, 1, "replacement");
    clear.confirmation = Some(authority.catalog);
    store.fencing_delay_ms.store(25, Ordering::SeqCst);
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 2, Ordering::SeqCst);
    assert!(matches!(
        original
            .execute(clear.clone(), ManagementPrivilege::Clear, 101)
            .await,
        Err(CatalogError::Store(_))
    ));
    assert_eq!(original.status().await.unwrap().0.state, RootState::Fencing);
    let restarted = repository(&store);
    assert!(matches!(restarted.recover(500).await, Err(CatalogError::Busy)));
    let RootState::Published(transition) = restarted.status().await.unwrap().0.state else {
        panic!("published maintenance")
    };
    assert!(transition.maintenance_observed_ms >= 500);
    assert_eq!(transition.bounds, bounds);
    assert_eq!(
        transition.complete_after_ms,
        transition.maintenance_observed_ms + 1010
    );
    restarted
        .execute(
            clear.clone(),
            ManagementPrivilege::Clear,
            transition.complete_after_ms,
        )
        .await
        .unwrap();
    let key = crowdb_access_iceberg::operation::ledger_key(
        crowdb_access_iceberg::key::SystemScope::Audit,
        clear.identity.operation,
    )
    .unwrap();
    let values = store.values.load();
    let crowdb_access_iceberg::record::StorageRecord::Management(audit) =
        crowdb_access_iceberg::record::StorageRecord::decode(
            &key,
            &values.get(&key.encode().unwrap()).unwrap().bytes,
        )
        .unwrap()
    else {
        panic!("audit receipt")
    };
    assert!(audit.grace_completed_ms >= transition.complete_after_ms);
    assert!(!audit.publication_proof.is_empty());
    let mut shortened = audit;
    shortened.grace_completed_ms = transition.complete_after_ms - 1;
    assert!(
        crowdb_access_iceberg::record::StorageRecord::Management(shortened)
            .encode()
            .is_err()
    );
}

#[tokio::test]
async fn explicit_clear_can_raise_delegation_bounds_without_shortening_old_reader_grace() {
    let store = Arc::new(TestStore::default());
    let old = repository(&store)
        .execute(
            request(ManagementAction::Initialize, 0, "catalog"),
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let expanded = ClearBounds {
        root_lease_ms: 0,
        request_ms: 5,
        delegated_access_ms: 50,
        clock_skew_ms: 2,
    };
    let upgraded = CatalogRepository::new(store.clone(), expanded).unwrap();
    let mut clear = request(ManagementAction::Clear, 1, "replacement");
    clear.confirmation = Some(old.catalog);
    assert!(matches!(
        upgraded
            .execute(clear.clone(), ManagementPrivilege::Clear, 100)
            .await,
        Err(CatalogError::Busy)
    ));
    let (root, authority) = upgraded.status().await.unwrap();
    let RootState::Published(transition) = root.state else {
        panic!("clear must retain maintenance fence")
    };
    assert_eq!(transition.bounds.request_ms, 10);
    assert_eq!(transition.bounds.delegated_access_ms, 50);
    assert_eq!(authority.admission_bounds, transition.bounds);
    let restarted = repository(&store);
    assert!(matches!(
        restarted
            .execute(
                clear.clone(),
                ManagementPrivilege::Clear,
                transition.complete_after_ms - 1
            )
            .await,
        Err(CatalogError::Busy)
    ));
    let ready = restarted
        .execute(clear, ManagementPrivilege::Clear, transition.complete_after_ms)
        .await
        .unwrap();
    assert_eq!(ready.admission_bounds, transition.bounds);
    assert_ne!(ready.catalog, old.catalog);
}

#[tokio::test]
async fn clear_epoch_exhaustion_and_missing_confirmation_do_not_write() {
    use crowdb_access_iceberg::key::{IcebergKey, SystemScope};
    use crowdb_access_iceberg::record::StorageRecord;
    let store = Arc::new(TestStore::default());
    let repository = repository(&store);
    let authority = repository
        .execute(
            request(ManagementAction::Initialize, 0, "catalog"),
            ManagementPrivilege::Manage,
            100,
        )
        .await
        .unwrap();
    let clear = request(ManagementAction::Clear, 1, "empty");
    let writes = store.writes.load(Ordering::SeqCst);
    assert!(repository
        .execute(clear, ManagementPrivilege::Clear, 101)
        .await
        .is_err());
    assert_eq!(store.writes.load(Ordering::SeqCst), writes);
    let mut root = repository.status().await.unwrap().0;
    root.context.activation_epoch = u64::MAX;
    let key = IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: Vec::new(),
    }
    .encode()
    .unwrap();
    let mut values = (**store.values.load()).clone();
    values.get_mut(&key).unwrap().bytes = StorageRecord::Active(root).encode().unwrap();
    store.values.store(Arc::new(values));
    let mut clear = request(ManagementAction::Clear, u64::MAX, "empty");
    clear.confirmation = Some(authority.catalog);
    assert!(repository
        .execute(clear, ManagementPrivilege::Clear, 102)
        .await
        .is_err());
    assert_eq!(store.writes.load(Ordering::SeqCst), writes);
}
