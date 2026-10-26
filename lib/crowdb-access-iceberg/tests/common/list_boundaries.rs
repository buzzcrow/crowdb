// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use admission::TestListMutation;
use crowdb_access_iceberg::{
    catalog::{ActiveCatalogRecord, CatalogError, RootState},
    key::{CatalogId, IcebergKey, OperationId, SystemScope},
    namespace::{authority_key, name_key, NamespaceLifecycle},
    record::StorageRecord,
};

fn mutations(test: &TestListFixture, change: u8) -> Vec<TestListMutation> {
    let store = &test.fixture.store;
    let authority = authority_key(test.leaf.catalog, test.leaf.namespace);
    let mut leaf = test.leaf.clone();
    leaf.mutation_revision += 1;
    if matches!(change, 0 | 3 | 6) {
        leaf.lifecycle = if change == 6 {
            NamespaceLifecycle::Dropping
        } else {
            NamespaceLifecycle::Tombstone
        };
        leaf.pending_operation = Some(OperationId::random());
        leaf.admission_fence += 1;
    } else if change == 1 {
        leaf.name_epoch += 1;
    } else if change == 2 {
        let mut names = leaf.identifier.components().to_vec();
        *names.last_mut().unwrap() = "changed".into();
        leaf.identifier = crowdb_access_iceberg::namespace::NamespaceIdentifier::new(names).unwrap();
    }
    if matches!(change, 4 | 5) {
        return vec![TestListMutation::new(
            store,
            &IcebergKey::System {
                scope: SystemScope::ActiveRoot,
                suffix: Vec::new(),
            },
            &StorageRecord::Active(ActiveCatalogRecord {
                context: if change == 5 {
                    test.fixture.context.replacement(CatalogId::random()).unwrap()
                } else {
                    test.fixture.context
                },
                operation: OperationId::random(),
                state: if change == 4 {
                    RootState::Fencing
                } else {
                    RootState::Ready
                },
            }),
        )];
    }
    let mut changes = vec![TestListMutation::new(
        store,
        &authority,
        &StorageRecord::NamespaceAuthority(Box::new(leaf)),
    )];
    if change == 3 {
        let names: Vec<_> = test
            .leaf
            .identifier
            .components()
            .iter()
            .map(String::as_str)
            .collect();
        let replacement = test.fixture.authority(test.leaf.parent, &names);
        let mapping_key = name_key(test.leaf.catalog, test.leaf.parent, test.leaf.identifier.name()).unwrap();
        let bytes = &store.values.load()[&mapping_key.encode().unwrap()].bytes;
        let StorageRecord::NamespaceMapping(mut mapping) =
            StorageRecord::decode(&mapping_key, bytes).unwrap()
        else {
            panic!()
        };
        mapping.namespace = replacement.namespace;
        changes.extend([
            TestListMutation::new(
                store,
                &authority_key(replacement.catalog, replacement.namespace),
                &StorageRecord::NamespaceAuthority(Box::new(replacement)),
            ),
            TestListMutation::new(store, &mapping_key, &StorageRecord::NamespaceMapping(mapping)),
        ]);
    }
    changes
}

#[tokio::test]
async fn lists_reject_a_changed_leaf_or_catalog_after_scanning_without_any_caller_mutation() {
    for change in 0..6 {
        for tables in [false, true] {
            let test = TestListFixture::new(3, tables).await;
            let store = TestListStore::new(test.fixture.store.clone(), mutations(&test, change));
            let error = if tables {
                TableLister::new(store.clone(), &[7; 32])
                    .unwrap()
                    .list(test.fixture.context, &test.leaf.identifier, limits(3), None)
                    .await
                    .unwrap_err()
            } else {
                NamespaceLister::new(store.clone(), &[7; 32])
                    .unwrap()
                    .page(test.fixture.context, Some(&test.leaf.identifier), 3, "")
                    .await
                    .unwrap_err()
            };
            if change == 4 {
                assert!(matches!(error, CatalogError::Busy), "{error:?}");
            } else {
                assert!(matches!(error, CatalogError::Conflict), "{error:?}");
            }
            assert_eq!(store.counts().2, 1);
            assert_eq!(store.counts().3, 0);
        }
    }
}

#[tokio::test]
async fn a_dropping_parent_retains_list_visibility_for_both_resource_types() {
    for tables in [false, true] {
        let test = TestListFixture::new(3, tables).await;
        let store = TestListStore::new(test.fixture.store.clone(), mutations(&test, 6));
        if tables {
            assert_eq!(
                TableLister::new(store.clone(), &[7; 32])
                    .unwrap()
                    .list(test.fixture.context, &test.leaf.identifier, limits(3), None)
                    .await
                    .unwrap()
                    .unwrap()
                    .names,
                ["a", "b", "c"]
            );
        } else {
            assert_eq!(
                NamespaceLister::new(store.clone(), &[7; 32])
                    .unwrap()
                    .page(test.fixture.context, Some(&test.leaf.identifier), 3, "")
                    .await
                    .unwrap()
                    .unwrap()
                    .namespaces
                    .len(),
                3
            );
        }
        assert_eq!(store.counts(), (13, 2, 1, 0));
    }
}

#[tokio::test]
async fn a_missing_default_namespace_returns_absence_without_creating_resources_or_scanning() {
    let test = fixture::TestNamespace::new().await;
    let namespace = test.authority(None, &["default"]).identifier;
    let store = TestListStore::new(test.store.clone(), Vec::new());
    assert!(TableLister::new(store.clone(), &[7; 32])
        .unwrap()
        .list(test.context, &namespace, limits(3), None)
        .await
        .unwrap()
        .is_none());
    assert_eq!(store.counts(), (3, 2, 0, 0));
    assert!(NamespaceLister::new(store.clone(), &[7; 32])
        .unwrap()
        .page(test.context, Some(&namespace), 3, "")
        .await
        .unwrap()
        .is_none());
    assert_eq!(store.counts(), (6, 4, 0, 0));
}

#[tokio::test]
async fn continuation_requests_recheck_the_context_and_keep_epoch_binding() {
    for tables in [false, true] {
        let test = TestListFixture::new(3, tables).await;
        let store = TestListStore::new(test.fixture.store.clone(), Vec::new());
        let namespace_lister = NamespaceLister::new(store.clone(), &[7; 32]).unwrap();
        let table_lister = TableLister::new(store.clone(), &[7; 32]).unwrap();
        let token = if tables {
            table_lister
                .list(test.fixture.context, &test.leaf.identifier, limits(1), Some(""))
                .await
                .unwrap()
                .unwrap()
                .next_page_token
                .unwrap()
        } else {
            namespace_lister
                .page(test.fixture.context, Some(&test.leaf.identifier), 1, "")
                .await
                .unwrap()
                .unwrap()
                .next_page_token
                .unwrap()
        };
        let mut context = test.fixture.context;
        context.activation_epoch += 1;
        test.fixture.root(context, RootState::Ready).await;
        for context in [test.fixture.context, context] {
            if tables {
                assert!(table_lister
                    .list(context, &test.leaf.identifier, limits(1), Some(&token))
                    .await
                    .is_err());
            } else {
                assert!(namespace_lister
                    .page(context, Some(&test.leaf.identifier), 1, &token)
                    .await
                    .is_err());
            }
        }
        assert_eq!(store.counts().3, 0);
    }
}

#[tokio::test]
async fn root_namespace_pages_keep_their_final_clear_and_replacement_checks() {
    for change in [4, 5] {
        let test = TestListFixture::new(3, false).await;
        let store = TestListStore::new(test.fixture.store.clone(), mutations(&test, change));
        let error = NamespaceLister::new(store.clone(), &[7; 32])
            .unwrap()
            .page(test.fixture.context, None, 100, "")
            .await
            .unwrap_err();
        assert_eq!(matches!(error, CatalogError::Busy), change == 4, "{error:?}");
        assert_eq!(matches!(error, CatalogError::Conflict), change == 5, "{error:?}");
        assert_eq!(store.counts(), (3, 2, 1, 0));
    }
}
