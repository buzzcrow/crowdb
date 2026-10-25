// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogContext, CatalogStore, RoutedCatalogStore},
    gc::{GcLimits, GcScan, GcStore},
    key::{CatalogScope, IcebergKey, OperationId},
    operation::mutation_identity,
    record::StorageRecord,
    table::{storage_head_key, TableHead, TableLifecycle, TablePurgeTask},
};

use super::storage::TestPayloads;

pub(super) async fn age_owned_intents(store: &RoutedCatalogStore, data: &TestPayloads, now_ms: u64) {
    let owner = data.dead[1].location.table();
    let scan = GcScan {
        native: false,
        catalog: owner.catalog,
        scope: Some(CatalogScope::FileWriteIntent),
        prefix: owner.table.as_bytes().to_vec(),
        after: Vec::new(),
        items: 16,
        bytes: 128 * 1024,
    };
    let page = store.scan_gc(scan.clone()).await.unwrap();
    scan.validate_page(&page).unwrap();
    assert!(page.continuation.is_none());
    assert_eq!(page.items.len(), data.tree_roots.len());
    let retention = GcLimits::default().minimum_retention_ms;
    let deadline = now_ms.checked_sub(20 * 60 * 1000 + 60_000).unwrap();
    let created = deadline.checked_sub(retention).unwrap();
    for item in page.items {
        let key = IcebergKey::decode(&item.key).unwrap();
        let StorageRecord::FileWriteIntent(mut intent) = StorageRecord::decode(&key, &item.value).unwrap()
        else {
            panic!("file intent expected")
        };
        assert_eq!(intent.owner.table, owner);
        assert_eq!(intent.owner.file, data.dead[1].file);
        assert!(data.tree_roots.contains(&intent.root));
        assert_eq!(intent.not_before_ms, 0, "never shorten an assigned deadline");
        assert!(!intent.deleting);
        // Explicit aged fixture facts, before a worker starts; retain exact owner/root.
        intent.created_ms = created;
        intent.not_before_ms = deadline;
        let bytes = StorageRecord::FileWriteIntent(intent).encode().unwrap();
        assert!(matches!(
            store
                .compare_exchange(
                    &item.key,
                    Some(&item.value),
                    &bytes,
                    mutation_identity(&item.key, Some(&item.value), &bytes)
                )
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }
}

pub(super) async fn late_stale_marker(
    store: &RoutedCatalogStore,
    context: CatalogContext,
    data: &TestPayloads,
    dropped_ms: u64,
) -> (TablePurgeTask, TableHead) {
    let key = storage_head_key(data.live.location.table());
    let encoded = key.encode().unwrap();
    let value = store.get(&encoded).await.unwrap().unwrap();
    let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
        panic!("Ready head expected")
    };
    assert_eq!(head.lifecycle, TableLifecycle::Ready);
    let mut stale = (*head).clone();
    stale.lifecycle = TableLifecycle::Tombstone;
    stale.operation_fence += 1;
    stale.pending_operation = Some(OperationId::random());
    let marker = TablePurgeTask {
        activation_epoch: context.activation_epoch,
        head: stale,
        dropped_ms,
    };
    let mut advanced: TableHead = *head;
    advanced.generation += 1;
    let next = StorageRecord::TableHead(Box::new(advanced.clone()))
        .encode()
        .unwrap();
    assert!(matches!(
        store
            .compare_exchange(
                &encoded,
                Some(&value.bytes),
                &next,
                mutation_identity(&encoded, Some(&value.bytes), &next)
            )
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    let encoded = marker.key().encode().unwrap();
    let bytes = StorageRecord::TablePurgeTask(Box::new(marker.clone()))
        .encode()
        .unwrap();
    assert!(matches!(
        store
            .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    (marker, advanced)
}
