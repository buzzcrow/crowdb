use crowdb_access_iceberg::catalog::{CasOutcome, CatalogStore};
use crowdb_access_iceberg::key::{CatalogId, CatalogScope, NamespaceId, OperationId};
use crowdb_access_iceberg::namespace::{
    name_key, ChildScan, NamespaceMapping, NamespaceMappingState, NamespaceStore,
};
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;

use super::common::TestIcebergStack;

pub async fn verify_name_index(stack: &TestIcebergStack, catalog: CatalogId) {
    let store = stack.store().await;
    let parent = Some(NamespaceId::random());
    let mut entries = Vec::new();
    for name in ["a", "b", "c"] {
        let mapping = NamespaceMapping {
            catalog,
            parent,
            name: name.into(),
            namespace: NamespaceId::random(),
            name_epoch: 1,
            operation: OperationId::random(),
            state: NamespaceMappingState::Reserved,
        };
        let key = name_key(catalog, parent, name).unwrap().encode().unwrap();
        let bytes = StorageRecord::NamespaceMapping(mapping.clone()).encode().unwrap();
        assert!(matches!(
            store
                .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
        entries.push((key, bytes, mapping));
    }
    let mut scan = ChildScan {
        catalog,
        parent,
        scope: CatalogScope::NamespaceName,
        limit: 1,
        continuation: None,
    };
    let mut keys = Vec::new();
    loop {
        let page = store.scan_children(scan.clone()).await.unwrap();
        assert!(page.items.len() <= 1);
        keys.extend(page.items.into_iter().map(|item| item.key));
        scan.continuation = page.continuation;
        if scan.continuation.is_none() {
            break;
        }
        assert!(keys.len() <= entries.len());
    }
    assert_eq!(
        keys,
        entries.iter().map(|(key, _, _)| key.clone()).collect::<Vec<_>>()
    );
    assert!(store
        .scan_children(ChildScan {
            scope: CatalogScope::TableName,
            ..scan
        })
        .await
        .unwrap()
        .items
        .is_empty());
    let (key, bytes, mapping) = entries.remove(0);
    verify_conditional_recreation(store.as_ref(), key, bytes, mapping).await;
}

async fn verify_conditional_recreation(
    store: &dyn NamespaceStore,
    key: Vec<u8>,
    bytes: Vec<u8>,
    mut mapping: NamespaceMapping,
) {
    let mut mismatched = mapping.clone();
    mismatched.operation = OperationId::random();
    let wrong_bytes = StorageRecord::NamespaceMapping(mismatched).encode().unwrap();
    let wrong_identity = mutation_identity(&key, Some(&wrong_bytes), &[]);
    assert!(matches!(
        store
            .delete_mapping(&key, &wrong_bytes, wrong_identity)
            .await
            .unwrap(),
        CasOutcome::Conflict(Some(_))
    ));
    let identity = mutation_identity(&key, Some(&bytes), &[]);
    assert!(matches!(
        store.delete_mapping(&key, &bytes, identity).await.unwrap(),
        CasOutcome::Applied(_)
    ));
    assert!(store.get(&key).await.unwrap().is_none());
    mapping.namespace = NamespaceId::random();
    mapping.operation = OperationId::random();
    let replacement = StorageRecord::NamespaceMapping(mapping).encode().unwrap();
    assert!(matches!(
        store
            .compare_exchange(
                &key,
                None,
                &replacement,
                mutation_identity(&key, None, &replacement)
            )
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    store.delete_mapping(&key, &bytes, identity).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap().unwrap().bytes, replacement);
}
