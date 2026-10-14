// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::{
    native::{activate, TestNativeStorage},
    now_ms,
};
use crowdb_access_iceberg::{
    catalog::{
        CatalogContext, CatalogRepository, CatalogStore, ClearBounds, ManagementPrivilege, RoutedCatalogStore,
    },
    file::{file_key, ContentFormat, FileContent, FileKind, FileRecord, TableLocation},
    key::{FileId, NamespaceId, OperationId, TableId},
    operation::{mutation_identity, ManagementAction, ManagementRequest, RequestIdentity},
    record::StorageRecord,
    table::{head_key, TableHead, TableLifecycle, TablePurgeTask},
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub(super) async fn seed_table(
    stack: &TestNativeStorage,
) -> (Arc<RoutedCatalogStore>, CatalogContext, TableId) {
    let store = stack.store().await;
    let catalog = CatalogRepository::new(
        store.clone(),
        ClearBounds {
            request_ms: 300_000,
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )
    .unwrap();
    let now = now_ms();
    catalog
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now,
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "gc-control".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            now,
        )
        .await
        .unwrap();
    activate(&catalog).await;
    let context = catalog.status().await.unwrap().0.context;
    let table = seed_head(store.as_ref(), context).await;
    (store, context, table)
}

pub(super) async fn seed_head(store: &RoutedCatalogStore, context: CatalogContext) -> TableId {
    let table = TableId::random();
    let location = TableLocation {
        storage_namespace: None,
        catalog: context.catalog,
        table,
    };
    let head = TableHead {
        catalog: context.catalog,
        table,
        namespace: NamespaceId::random(),
        name: "items".into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Ready,
        generation: 1,
        metadata_file: FileId::random(),
        metadata_location: location.file("metadata/first.json").unwrap(),
        metadata_digest: [7; 32],
        commit_binding: None,
        format_version: 1,
        table_uuid: None,
        operation_fence: 1,
        pending_operation: None,
    };
    let key = head_key(context.catalog, table).encode().unwrap();
    let bytes = StorageRecord::TableHead(Box::new(head)).encode().unwrap();
    store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    table
}

pub(super) async fn tombstone_head(store: &RoutedCatalogStore, context: CatalogContext, table: TableId) {
    let key = head_key(context.catalog, table);
    let previous = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::TableHead(mut head) = StorageRecord::decode(&key, &previous.bytes).unwrap() else {
        panic!("table head");
    };
    head.lifecycle = TableLifecycle::Tombstone;
    head.operation_fence += 1;
    head.pending_operation = Some(OperationId::random());
    let next = StorageRecord::TableHead(head).encode().unwrap();
    let key = key.encode().unwrap();
    store
        .compare_exchange(
            &key,
            Some(&previous.bytes),
            &next,
            mutation_identity(&key, Some(&previous.bytes), &next),
        )
        .await
        .unwrap();
}

pub(super) async fn seed_files(
    store: &RoutedCatalogStore,
    context: CatalogContext,
    table: TableId,
    count: usize,
) {
    for index in 0..count {
        let payload_json = format!("{{\"index\":{index}}}");
        let file = FileRecord {
            file: FileId::random(),
            location: TableLocation {
                storage_namespace: None,
                catalog: context.catalog,
                table,
            }
            .file(&format!("metadata/backlog-{index}.json"))
            .unwrap(),
            kind: FileKind::Metadata,
            format: ContentFormat::Json,
            length: payload_json.len() as u64,
            digest: Sha256::digest(payload_json.as_bytes()).into(),
            content: FileContent::select_inline(FileKind::Metadata, payload_json.as_bytes()).unwrap(),
            hint: None,
        };
        let key = file_key(context.catalog, file.file).encode().unwrap();
        let bytes = StorageRecord::File(Box::new(file)).encode().unwrap();
        store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
    }
}

pub(super) async fn seed_purge_marker(store: &RoutedCatalogStore, context: CatalogContext, table: TableId) {
    tombstone_head(store, context, table).await;
    let key = head_key(context.catalog, table);
    let head = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &head.bytes).unwrap() else {
        panic!("table head");
    };
    let marker = TablePurgeTask {
        activation_epoch: context.activation_epoch,
        head: *head,
        dropped_ms: 0,
    };
    let encoded = marker.key().encode().unwrap();
    let bytes = StorageRecord::TablePurgeTask(Box::new(marker)).encode().unwrap();
    store
        .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
        .await
        .unwrap();
}
