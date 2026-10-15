// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/publication.rs"]
mod publication;
#[path = "common/reclaim_store.rs"]
mod reclaim_store;
use crowdb_access_dataset::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn old_reclaimer_cannot_unfence_a_new_reclaimers_terminal_decision() {
    let (store, authority, identity, snapshot) = reclaim_store::fixture().await;
    authority.retain_snapshot(&identity, snapshot).await.unwrap();
    store.pause_retention.store(true, Ordering::Relaxed);
    let task = {
        let authority = authority.clone();
        let identity = identity.clone();
        tokio::spawn(async move { authority.begin_reclaim(&identity, snapshot).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), store.observed.notified())
        .await
        .unwrap();
    authority.release_snapshot(&identity, snapshot).await.unwrap();
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    store.resume.notify_one();
    assert!(matches!(task.await.unwrap(), Err(AuthorityError::HeadConflict)));
    assert!(authority
        .touch_read_handle(&identity, snapshot, OperationId::random(), 1, 3600)
        .await
        .is_err());
    assert_eq!(
        authority
            .begin_reclaim(&identity, snapshot)
            .await
            .unwrap()
            .publication
            .state,
        PublicationState::Aborted
    );
}
