// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/inspection.rs"]
mod inspection;
use crowdb_access_dataset::{inspection::InspectionError, *};
use inspection::{budget, fixture, prepare, publish};
use std::sync::atomic::Ordering;
#[path = "common/publication.rs"]
mod publication;
use publication::publication_manifest;

#[tokio::test]
async fn paged_forest_excludes_prepared_candidates_without_manifest_or_payload_reads() {
    let (store, authority, identity) = fixture().await;
    let root = prepare(&authority, &identity, None, 1).await;
    publish(&authority, &identity, root).await;
    let child = prepare(&authority, &identity, Some(root), 2).await;
    publish(&authority, &identity, child).await;
    let new_root = prepare(&authority, &identity, None, 3).await;
    publish(&authority, &identity, new_root).await;
    let prepared = prepare(&authority, &identity, Some(root), 4).await;
    store.inspecting.store(true, Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    let mut after = None;
    let mut visible = Vec::new();
    loop {
        let page = authority
            .snapshot_inventory_page(&identity, after, budget(1), &cancel)
            .await
            .unwrap();
        visible.extend(page.entries.into_iter().map(|row| row.snapshot));
        after = page.after;
        if after.is_none() {
            break;
        }
    }
    visible.sort();
    let mut expected = vec![root, child, new_root];
    expected.sort();
    assert_eq!(visible, expected);
    let children = authority
        .snapshot_children_page(&identity, Some(root), None, budget(100), &cancel)
        .await
        .unwrap();
    assert_eq!(children.entries.len(), 1);
    assert_eq!(children.entries[0].snapshot, child);
    let roots = authority
        .snapshot_children_page(&identity, None, None, budget(100), &cancel)
        .await
        .unwrap();
    assert_eq!(roots.entries.len(), 2);
    assert!(matches!(
        authority
            .snapshot_inspection_summary(&identity, prepared, budget(1), &cancel)
            .await,
        Err(InspectionError::Authority(AuthorityError::SnapshotNotFound))
    ));
    assert_eq!(
        authority
            .snapshot_inspection_summary(&identity, child, budget(1), &cancel)
            .await
            .unwrap()
            .parent,
        Some(root)
    );
}
#[tokio::test]
async fn inspection_cursor_scope_cancellation_and_aggregate_budget_fail_explicitly() {
    let (store, authority, identity) = fixture().await;
    for marker in 1..=2 {
        let id = prepare(&authority, &identity, None, marker).await;
        publish(&authority, &identity, id).await;
    }
    store.inspecting.store(true, Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    let first = authority
        .snapshot_inventory_page(&identity, None, budget(1), &cancel)
        .await
        .unwrap();
    assert!(authority
        .snapshot_children_page(&identity, None, first.after, budget(1), &cancel)
        .await
        .is_err());
    let mut tiny = budget(1);
    tiny.bytes = 20;
    assert!(matches!(
        authority
            .snapshot_inventory_page(&identity, None, tiny, &cancel)
            .await,
        Err(InspectionError::Limit)
    ));
    cancel.cancel();
    assert!(matches!(
        authority
            .snapshot_inventory_page(&identity, None, budget(1), &cancel)
            .await,
        Err(InspectionError::Cancelled)
    ));
}

#[tokio::test]
async fn matrix_distinguishes_inheritance_tombstones_and_missing_without_payload_reads() {
    let (store, authority, identity) = fixture().await;
    let mut manifest: ManifestRecord = bincode::deserialize(&publication_manifest(1)).unwrap();
    manifest.schema.fields = ["marker", "hidden", "absent"]
        .into_iter()
        .map(|name| FieldDefinition {
            name: name.into(),
            required: false,
        })
        .collect();
    manifest.samples[0].sample_id = vec![0, 255, 0];
    let mut hidden = manifest.samples[0].fields[0].clone();
    hidden.name = "hidden".into();
    manifest.samples[0].fields.push(hidden);
    let parent = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            bincode::serialize(&manifest).unwrap(),
            OperationId::random(),
        )
        .await
        .unwrap();
    publish(&authority, &identity, parent).await;
    manifest.samples[0].fields = vec![FieldRecord {
        name: "hidden".into(),
        value: FieldLocator::Tombstone,
    }];
    let child = authority
        .prepare_snapshot_candidate(
            &identity,
            Some(parent),
            bincode::serialize(&manifest).unwrap(),
            OperationId::random(),
        )
        .await
        .unwrap();
    publish(&authority, &identity, child).await;
    store.inspecting.store(true, Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    let schema = authority
        .inspection_schema_page(&identity, child, None, budget(100), &cancel)
        .await
        .unwrap();
    assert_eq!(
        schema.entries.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        vec!["marker", "hidden", "absent"]
    );
    let ids = authority
        .inspection_samples_page(&identity, child, None, budget(100), &cancel)
        .await
        .unwrap();
    assert_eq!(ids.entries, vec![vec![0, 255, 0]]);
    assert!(authority
        .inspection_contains_sample(&identity, child, &[0, 255, 0], budget(1), &cancel)
        .await
        .unwrap());
    assert!(!authority
        .inspection_contains_sample(&identity, child, &[0, 255], budget(1), &cancel)
        .await
        .unwrap());
    let fields = vec!["marker".into(), "hidden".into(), "absent".into()];
    let matrix = authority
        .inspection_matrix_page(&identity, child, &fields, None, budget(20), &cancel)
        .await
        .unwrap();
    let row = &matrix.entries[0];
    assert_eq!(row.fields[0].as_ref().unwrap().source_snapshot, parent);
    assert_eq!(row.fields[0].as_ref().unwrap().length, 1);
    assert_eq!(
        row.fields[1].as_ref().unwrap().storage,
        crowdb_access_dataset::inspection::FieldStorageKind::Tombstone
    );
    assert_eq!(row.fields[1].as_ref().unwrap().source_snapshot, child);
    assert!(row.fields[2].is_none());
    assert!(authority
        .inspection_matrix_page(&identity, child, &["unknown".into()], None, budget(1), &cancel)
        .await
        .is_err());
}

#[tokio::test]
async fn dataset_discovery_reads_legacy_identity_keys_and_current_head_names() {
    let (store, authority, identity) = fixture().await;
    let snapshot = prepare(&authority, &identity, None, 1).await;
    publish(&authority, &identity, snapshot).await;
    let empty = DatasetIdentity::new(NamespacePath::root(), "empty").unwrap();
    authority.create_dataset(empty.clone()).await.unwrap();
    store.inspecting.store(true, Ordering::Relaxed);
    let page = authority
        .inspect_datasets_page(&NamespacePath::root(), None, budget(20), &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 2);
    assert_eq!(
        page.entries
            .iter()
            .find(|row| row.identity == identity)
            .unwrap()
            .latest,
        Some(snapshot)
    );
    assert_eq!(
        page.entries
            .iter()
            .find(|row| row.identity == empty)
            .unwrap()
            .latest,
        None
    );
}
