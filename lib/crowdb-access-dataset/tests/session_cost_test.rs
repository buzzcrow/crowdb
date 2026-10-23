// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/publication.rs"]
mod publication;
use crowdb_access_dataset::*;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn concurrent_fetch_and_replay_never_write_or_renew_progress() {
    let store = access_store::TestAccessStore::shared();
    let authority = Arc::new(DatasetAuthority::new(store.clone()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "read-cost").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    authority
        .publish_snapshot(&identity, None, publication::publication_manifest(1))
        .await
        .unwrap();
    let sessions = DatasetSessions::new(authority.clone());
    let spec = SessionSpec {
        id: OperationId::random(),
        snapshot: "latest".into(),
        selection: Selection::Prefix(b"sample-".to_vec()),
        fields: vec!["marker".into()],
        batch_size: 1,
        partition: SessionPartition {
            worker: 0,
            workers: 1,
            rank: 0,
            world_size: 1,
        },
        shuffle: None,
    };
    let opened = sessions.start(&identity, spec.clone(), 10).await.unwrap();
    let writes = store.writes.load(Ordering::Relaxed);
    let batches = futures::future::join_all((0..96).map(|_| sessions.next(&identity, spec.id, 69))).await;
    let token = &batches[0].as_ref().unwrap().next;
    for batch in &batches {
        assert_eq!(&batch.as_ref().unwrap().next, token);
    }
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
    assert!(sessions.next(&identity, spec.id, 70).await.is_err());
    let restarted = DatasetSessions::new(authority);
    restarted.resume(&identity, spec.id, 70).await.unwrap();
    assert_eq!(&restarted.next(&identity, spec.id, 71).await.unwrap().next, token);
    let writes = store.writes.load(Ordering::Relaxed);
    restarted
        .acknowledge(&identity, spec.id, &opened.acknowledged, token, 71)
        .await
        .unwrap();
    assert_eq!(store.writes.load(Ordering::Relaxed), writes + 1);
    restarted
        .acknowledge(&identity, spec.id, &opened.acknowledged, token, 71)
        .await
        .unwrap();
    restarted.next(&identity, spec.id, 72).await.unwrap();
    assert_eq!(store.writes.load(Ordering::Relaxed), writes + 1);
    restarted.close(&identity, spec.id).await.unwrap();
    let writes = store.writes.load(Ordering::Relaxed);
    restarted.close(&identity, spec.id).await.unwrap();
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}
