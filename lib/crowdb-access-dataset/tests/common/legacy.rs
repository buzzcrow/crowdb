// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::*;
pub(super) async fn emulate_legacy(
    store: &dyn DatasetStore,
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    snapshots: &[SnapshotId],
    latest: SnapshotId,
) {
    for snapshot in snapshots {
        let receipt = authority
            .snapshot_commit(identity, *snapshot)
            .await
            .unwrap()
            .unwrap();
        for (prefix, suffix) in [
            (b"DS\0CB\0".as_slice(), snapshot.as_bytes()),
            (b"DS\0SC\0", snapshot.as_bytes()),
            (b"DS\0LC\0", snapshot.as_bytes()),
            (b"DS\0CR\0", receipt.attempt.id.as_bytes()),
        ] {
            store
                .delete(&[prefix, identity.key().unwrap().as_slice(), suffix].concat())
                .await
                .unwrap();
        }
    }
    let head_key = [b"DS\0H\0".as_slice(), identity.key().unwrap().as_slice()].concat();
    let old = store.get(&head_key).await.unwrap().unwrap();
    let bytes = bincode::serialize(&(
        1u16,
        HeadRecord {
            latest: Some(latest),
            stable: None,
        },
    ))
    .unwrap();
    assert!(matches!(
        store
            .compare_exchange(&head_key, Some(&old.bytes), &bytes)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
}
