#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/rename_conflict.rs"]
mod conflict;
#[path = "common/table_creation.rs"]
#[allow(dead_code)]
mod creation;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/namespace_recovery_store.rs"]
mod namespace_recovery_store;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/table_rename.rs"]
#[allow(dead_code)]
mod rename;

use crowdb_access_iceberg::{
    catalog::CatalogError,
    key::{IcebergKey, OperationId},
    namespace::{NamespaceAuthority, NamespaceLifecycle, NamespaceRepository},
    record::StorageRecord,
    table::{TableLifecycleAction, TableLifecycles, TableRepository},
};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn repeated_namespace_conflicts_use_observed_values_without_gets() {
    let (test, request, _) = rename::setup(false).await;
    let store = std::sync::Arc::new(conflict::TestRenameConflicts::new(test.fixture.store.clone()));
    assert_eq!(
        TableLifecycles::new(store.clone())
            .execute(&request)
            .await
            .unwrap()
            .status,
        204
    );
    assert_eq!(store.injected.load(Ordering::SeqCst), 4);
    assert_no_occupancy(&test);
    assert_eq!(rename::visible(&test, &request).await, (false, true));
}

fn assert_no_occupancy(test: &creation::TestCreation) {
    for (key, value) in test.fixture.store.values.load().iter() {
        let key = IcebergKey::decode(key).unwrap();
        if let StorageRecord::NamespaceAuthority(parent) = StorageRecord::decode(&key, &value.bytes).unwrap()
        {
            assert!(parent.rename.is_none());
        }
    }
}

#[tokio::test]
async fn rename_preserves_identity_and_creates_no_journal_or_response_records() {
    for cross in [false, true] {
        let (test, request, before) = rename::setup(cross).await;
        let store = test.fixture.store.clone();
        let old_keys: std::collections::BTreeSet<_> = store.values.load().keys().cloned().collect();
        let writes = store.writes.load(Ordering::SeqCst);
        let outcome = TableLifecycles::new(store.clone())
            .execute(&request)
            .await
            .unwrap();
        assert_eq!(outcome.status, 204);
        assert!(outcome.body.is_empty());
        assert_eq!(
            store.writes.load(Ordering::SeqCst) - writes,
            if cross { 8 } else { 6 }
        );
        assert_eq!(store.values.load().len(), old_keys.len());
        let new_keys: Vec<_> = store
            .values
            .load()
            .keys()
            .filter(|key| !old_keys.contains(*key))
            .cloned()
            .collect();
        assert_eq!(new_keys.len(), 1, "only the destination name can be new");
        assert!(matches!(
            IcebergKey::decode(&new_keys[0]).unwrap(),
            IcebergKey::Catalog {
                scope: crowdb_access_iceberg::key::CatalogScope::TableName,
                ..
            }
        ));
        let head = rename::head(&test, &before).await;
        assert_eq!(head.table, before.table);
        assert_eq!(head.table_uuid, before.table_uuid);
        assert_eq!(head.generation, before.generation);
        assert_eq!(head.metadata_location, before.metadata_location);
        assert_eq!(head.metadata_digest, before.metadata_digest);
        assert_eq!(head.name_epoch, before.name_epoch + 1);
        assert_eq!(head.operation_fence, before.operation_fence + 1);
        assert!(head.pending_operation.is_none());
        assert_no_occupancy(&test);
        assert_eq!(
            TableLifecycles::new(store.clone())
                .execute(&request)
                .await
                .unwrap()
                .status,
            404
        );
        let mut recreated = test.request.clone();
        recreated.identity.operation = OperationId::random();
        test.creator().create(&recreated).await.unwrap();
        let tables = TableRepository::new(store.clone());
        let replacement = tables
            .select(request.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            TableLifecycles::new(store.clone()).execute(&request).await,
            Err(CatalogError::Conflict)
        ));
        assert_eq!(
            tables
                .select(request.context, test.parent.namespace, "events")
                .await
                .unwrap()
                .unwrap()
                .head
                .table,
            replacement.head.table
        );
        assert_no_occupancy(&test);
    }
}

#[tokio::test]
async fn occupancy_codec_binds_both_parents_and_excludes_deletion_and_legacy_markers() {
    let (test, request, before) = rename::setup(true).await;
    let store = test.fixture.store.clone();
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 2, Ordering::SeqCst);
    assert!(TableLifecycles::new(store.clone())
        .execute(&request)
        .await
        .is_err());
    store.fail_after.store(0, Ordering::SeqCst);
    let parents: Vec<NamespaceAuthority> = store
        .values
        .load()
        .iter()
        .filter_map(|(key, value)| {
            match StorageRecord::decode(&IcebergKey::decode(key).unwrap(), &value.bytes).unwrap() {
                StorageRecord::NamespaceAuthority(parent) if parent.rename.is_some() => Some(*parent),
                _ => None,
            }
        })
        .collect();
    assert_eq!(parents.len(), 2);
    assert_eq!(parents[0].rename, parents[1].rename);
    for parent in parents {
        let key = crowdb_access_iceberg::namespace::authority_key(parent.catalog, parent.namespace);
        let record = StorageRecord::NamespaceAuthority(Box::new(parent.clone()));
        assert_eq!(
            StorageRecord::decode(&key, &record.encode().unwrap()).unwrap(),
            record
        );
        for mutation in 0..3 {
            let mut bad = parent.clone();
            match mutation {
                0 => {
                    bad.lifecycle = NamespaceLifecycle::Dropping;
                    bad.pending_operation = Some(OperationId::random());
                }
                1 => bad.pending_operation = Some(OperationId::random()),
                _ => {
                    bad.rename.as_mut().unwrap().source.catalog =
                        crowdb_access_iceberg::key::CatalogId::random();
                }
            }
            assert!(StorageRecord::NamespaceAuthority(Box::new(bad)).encode().is_err());
        }
    }
    rename::repair(&test).await;
    assert_eq!(rename::head(&test, &before).await.name, before.name);
    assert_no_occupancy(&test);
}

#[tokio::test]
async fn opposing_moves_do_not_wait_on_each_other_or_lose_either_table() {
    let (mut test, first, _) = rename::setup(true).await;
    let TableLifecycleAction::Rename { namespace, .. } = &first.action else {
        panic!()
    };
    let mut create = test.request.clone();
    create.identity.operation = OperationId::random();
    create.namespace = namespace.clone();
    test.creator().create(&create).await.unwrap();
    std::sync::Arc::get_mut(&mut test.fixture.store)
        .unwrap()
        .namespace_update_barrier = Some(std::sync::Arc::new(tokio::sync::Barrier::new(2)));
    let second = crowdb_access_iceberg::table::TableLifecycleRequest {
        identity: crowdb_access_iceberg::operation::RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        namespace: namespace.clone(),
        action: TableLifecycleAction::Rename {
            namespace: test.parent.identifier.clone(),
            name: "other".into(),
        },
        ..first.clone()
    };
    let manager = TableLifecycles::new(test.fixture.store.clone());
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(
            Box::pin(manager.execute(&first)),
            Box::pin(manager.execute(&second))
        )
    })
    .await
    .unwrap();
    for result in [a, b] {
        match result {
            Ok(outcome) => assert_eq!(outcome.status, 204),
            Err(CatalogError::Busy | CatalogError::Conflict) => (),
            error => panic!("{error:?}"),
        }
    }
    rename::repair(&test).await;
    let tables = TableRepository::new(test.fixture.store.clone());
    let target = NamespaceRepository::new(test.fixture.store.clone())
        .load(first.context, namespace)
        .await
        .unwrap()
        .unwrap();
    let a_old = tables
        .select(first.context, test.parent.namespace, "events")
        .await
        .unwrap();
    let a_new = tables
        .select(first.context, target.namespace, "renamed")
        .await
        .unwrap();
    let b_old = tables
        .select(first.context, target.namespace, "events")
        .await
        .unwrap();
    let b_new = tables
        .select(first.context, test.parent.namespace, "other")
        .await
        .unwrap();
    assert_ne!(a_old.is_some(), a_new.is_some());
    assert_ne!(b_old.is_some(), b_new.is_some());
    assert_no_occupancy(&test);
}
