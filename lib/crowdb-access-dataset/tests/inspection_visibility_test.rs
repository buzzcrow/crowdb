// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/inspection.rs"]
#[allow(dead_code)]
mod inspection;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::{inspection::InspectionError, *};
use inspection::{budget, fixture, prepare, publish};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn visibility_uses_cas_proof_and_never_published_state_alone() {
    let (store, authority, identity) = fixture().await;
    let committed = prepare(&authority, &identity, None, 1).await;
    publish(&authority, &identity, committed).await;
    let uncommitted = prepare(&authority, &identity, None, 2).await;
    let record_key = [
        b"DS\0S\0".as_slice(),
        identity.key().unwrap().as_slice(),
        uncommitted.as_bytes(),
    ]
    .concat();
    let old = store.storage().get(&record_key).await.unwrap().unwrap();
    let (version, mut record): (u16, SnapshotRecord) = bincode::deserialize(&old.bytes).unwrap();
    record.publication.state = PublicationState::Published;
    store
        .storage()
        .compare_exchange(
            &record_key,
            Some(&old.bytes),
            &bincode::serialize(&(version, record)).unwrap(),
        )
        .await
        .unwrap();
    // Lose the secondary receipt row, retaining the successful head CAS marker.
    let receipt_key = [
        b"DS\0SC\0".as_slice(),
        identity.key().unwrap().as_slice(),
        committed.as_bytes(),
    ]
    .concat();
    store.storage().delete(&receipt_key).await.unwrap();
    store.inspecting.store(true, Ordering::Relaxed);
    let page = authority
        .snapshot_inventory_page(&identity, None, budget(20), &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].snapshot, committed);
    assert!(matches!(
        authority
            .snapshot_inspection_summary(&identity, uncommitted, budget(1), &ReadCancellation::new())
            .await,
        Err(InspectionError::Authority(AuthorityError::SnapshotNotFound))
    ));
}

#[tokio::test]
async fn matrix_cursor_binds_projection_but_survives_unrelated_head_changes() {
    let (store, authority, identity) = fixture().await;
    let mut manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(1)).unwrap();
    manifest.schema.fields.push(FieldDefinition {
        name: "optional".into(),
        required: false,
    });
    let mut second = manifest.samples[0].clone();
    second.sample_id = b"second".to_vec();
    manifest.samples.push(second);
    let snapshot = authority
        .prepare_snapshot_candidate(
            &identity,
            None,
            bincode::serialize(&manifest).unwrap(),
            OperationId::random(),
        )
        .await
        .unwrap();
    publish(&authority, &identity, snapshot).await;
    let fields = vec!["marker".into()];
    let cancel = ReadCancellation::new();
    let page = authority
        .inspection_matrix_page(&identity, snapshot, &fields, None, budget(1), &cancel)
        .await
        .unwrap();
    assert!(page.after.is_some());
    let other = prepare(&authority, &identity, None, 3).await;
    publish(&authority, &identity, other).await;
    store.inspecting.store(true, Ordering::Relaxed);
    assert!(matches!(
        authority
            .inspection_matrix_page(
                &identity,
                snapshot,
                &["optional".into()],
                page.after.clone(),
                budget(1),
                &cancel
            )
            .await,
        Err(InspectionError::Authority(AuthorityError::Invalid(
            DatasetError::CursorMismatch
        )))
    ));
    let next = authority
        .inspection_matrix_page(&identity, snapshot, &fields, page.after, budget(1), &cancel)
        .await
        .unwrap();
    assert_eq!(next.entries[0].sample_id, b"second");
}
