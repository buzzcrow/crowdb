// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/inspection_read_input.rs"]
mod inspection_read_input;

use crowdb_access_dataset::{
    inspection::InspectionError, AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity,
    NamespacePath, ReadCancellation, SnapshotId, StoreError, MAX_BATCH_SIZE,
};
use inspection_read_input::TestInputStore;
use std::sync::Arc;

#[tokio::test]
async fn malformed_explicit_inputs_fail_before_any_storage_access() {
    let identity = DatasetIdentity::new(NamespacePath::root(), "invalid-input").unwrap();
    let snapshot = SnapshotId::random();
    let cases = [
        ("empty IDs", Vec::new(), vec!["value".to_owned()]),
        (
            "duplicate IDs",
            vec![b"one".to_vec(), b"one".to_vec()],
            vec!["value".to_owned()],
        ),
        (
            "duplicate fields",
            vec![b"one".to_vec()],
            vec!["value".to_owned(), "value".to_owned()],
        ),
        (
            "too many IDs",
            (0..=MAX_BATCH_SIZE)
                .map(|index| index.to_be_bytes().to_vec())
                .collect(),
            vec!["value".to_owned()],
        ),
        (
            "too many fields",
            vec![b"one".to_vec()],
            (0..257).map(|index| format!("field-{index}")).collect(),
        ),
    ];
    for (name, ids, fields) in cases {
        let store = Arc::new(TestInputStore::default());
        let authority = DatasetAuthority::new(store.clone());
        let result = authority
            .resolve_inspection_batch_locators(&identity, snapshot, &ids, &fields, &ReadCancellation::new())
            .await;
        assert!(
            matches!(
                &result,
                Err(InspectionError::Authority(AuthorityError::Invalid(
                    DatasetError::InvalidManifest
                )))
            ),
            "{name}: {result:?}"
        );
        assert_eq!(store.calls(), 0, "{name}: storage was accessed");
    }
    let store = Arc::new(TestInputStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let result = authority
        .resolve_inspection_batch_locators(
            &identity,
            snapshot,
            &[b"one".to_vec()],
            &["value".to_owned()],
            &ReadCancellation::new(),
        )
        .await;
    assert!(matches!(
        result,
        Err(InspectionError::Store(StoreError::Rejected))
    ));
    assert_eq!(store.calls(), 1, "valid input must reach the rejecting store");
}
