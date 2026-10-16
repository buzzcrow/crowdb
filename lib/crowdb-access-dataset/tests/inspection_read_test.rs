// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
use crowdb_access_dataset::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn projected_delivery_resolves_large_binary_ids_without_manifest_reads() {
    let (store, authority, identity, parent) = split_store::fixture().await;
    let mut manifest: ManifestRecord = bincode::deserialize(&publication::publication_manifest(2)).unwrap();
    let id = vec![255; 16 * 1024];
    manifest.samples[0].sample_id.clone_from(&id);
    let snapshot = authority
        .publish_snapshot(&identity, Some(parent), bincode::serialize(&manifest).unwrap())
        .await
        .unwrap();
    store.metadata_only.store(true, Ordering::Relaxed);
    let cancel = ReadCancellation::new();
    let samples = authority
        .resolve_inspection_batch_locators(
            &identity,
            snapshot,
            std::slice::from_ref(&id),
            &["marker".into()],
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(samples[0].sample_id, id);
    assert!(matches!(&samples[0].fields["marker"], FieldLocator::Inline { value, .. } if value == &[2]));
    assert!(authority
        .resolve_inspection_batch_locators(
            &identity,
            snapshot,
            &[b"missing".to_vec()],
            &["marker".into()],
            &cancel
        )
        .await
        .is_err());
    assert!(authority
        .resolve_inspection_batch_locators(
            &identity,
            snapshot,
            std::slice::from_ref(&id),
            &["unknown".into()],
            &cancel
        )
        .await
        .is_err());
    cancel.cancel();
    assert!(authority
        .resolve_inspection_batch_locators(&identity, snapshot, &[id], &["marker".into()], &cancel)
        .await
        .is_err());
}
