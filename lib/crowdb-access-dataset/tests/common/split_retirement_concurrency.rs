// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{budget, retirement_fixture::TestFixture, split_store};
use crowdb_access_dataset::{split::*, *};
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn deletion_fence_rejects_a_publisher_already_waiting_on_its_head_cas() {
    let fixture = TestFixture::new().await;
    let base = fixture
        .authority
        .inspect_split_head(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    let gate = fixture.store.pause(b"DS\0SH\0", None, false);
    let authority = fixture.authority.clone();
    let identity = fixture.identity.clone();
    let snapshot = fixture.snapshot;
    let revision = fixture.record.revision;
    let publisher = tokio::spawn(async move {
        authority
            .update_split_head(
                &identity,
                snapshot,
                &SplitHeadUpdate {
                    revision,
                    expected_revision: base.revision,
                    expected_head: base.generation,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), gate.entered())
        .await
        .unwrap();
    fixture
        .authority
        .delete_split_revision(&fixture.identity, snapshot, revision, &ReadCancellation::new())
        .await
        .unwrap();
    gate.release();
    assert!(matches!(
        publisher.await.unwrap(),
        Err(SplitLifecycleError::HeadConflict { .. })
    ));
    fixture.clock.store(2200, Ordering::Release);
    fixture.finish().await;
    let head = fixture
        .authority
        .inspect_split_head(&fixture.identity, snapshot)
        .await
        .unwrap();
    assert_ne!(head.revision, Some(revision));
    fixture.assert_terminal().await;
}

#[tokio::test]
async fn a_publisher_winning_before_the_delete_fence_restores_the_protected_root() {
    let fixture = TestFixture::new().await;
    let base = fixture
        .authority
        .inspect_split_head(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    let publisher_gate = fixture.store.pause(b"DS\0SH\0", None, false);
    let authority = fixture.authority.clone();
    let identity = fixture.identity.clone();
    let snapshot = fixture.snapshot;
    let revision = fixture.record.revision;
    let publisher = tokio::spawn(async move {
        authority
            .update_split_head(
                &identity,
                snapshot,
                &SplitHeadUpdate {
                    revision,
                    expected_revision: base.revision,
                    expected_head: base.generation,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), publisher_gate.entered())
        .await
        .unwrap();
    let closing_gate = fixture
        .store
        .pause(b"DS\0SR\0", Some(b"split-deletion-v1\0"), true);
    let authority = fixture.authority.clone();
    let identity = fixture.identity.clone();
    let deletion = tokio::spawn(async move {
        authority
            .delete_split_revision(&identity, snapshot, revision, &ReadCancellation::new())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), closing_gate.entered())
        .await
        .unwrap();
    publisher_gate.release();
    publisher.await.unwrap().unwrap();
    closing_gate.release();
    assert!(matches!(
        deletion.await.unwrap(),
        Err(SplitLifecycleError::Frozen)
    ));
    assert_eq!(
        fixture
            .authority
            .get_split_revision(
                &fixture.identity,
                snapshot,
                revision,
                budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        fixture.record
    );
    assert_eq!(
        fixture
            .authority
            .inspect_split_head(&fixture.identity, snapshot)
            .await
            .unwrap()
            .revision,
        Some(revision)
    );
}

#[tokio::test]
async fn a_delayed_rollback_cannot_reopen_a_root_activated_by_another_deleter() {
    let fixture = TestFixture::new().await;
    let other = split_store::prepared(&fixture.authority, &fixture.identity, fixture.snapshot, None).await;
    let base = fixture
        .authority
        .inspect_split_head(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    let publisher_gate = fixture.store.pause(b"DS\0SH\0", None, false);
    let authority = fixture.authority.clone();
    let identity = fixture.identity.clone();
    let snapshot = fixture.snapshot;
    let revision = fixture.record.revision;
    let publisher = tokio::spawn(async move {
        authority
            .update_split_head(
                &identity,
                snapshot,
                &SplitHeadUpdate {
                    revision,
                    expected_revision: base.revision,
                    expected_head: base.generation,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), publisher_gate.entered())
        .await
        .unwrap();
    let closing_gate = fixture
        .store
        .pause(b"DS\0SR\0", Some(b"split-deletion-v1\0"), true);
    let authority = fixture.authority.clone();
    let identity = fixture.identity.clone();
    let deletion = tokio::spawn(async move {
        authority
            .delete_split_revision(&identity, snapshot, revision, &ReadCancellation::new())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), closing_gate.entered())
        .await
        .unwrap();
    publisher_gate.release();
    publisher.await.unwrap().unwrap();
    let rollback_gate = fixture.store.pause(&fixture.root(), Some(b"{"), false);
    closing_gate.release();
    tokio::time::timeout(Duration::from_secs(2), rollback_gate.entered())
        .await
        .unwrap();
    let current = fixture
        .authority
        .inspect_split_head(&fixture.identity, snapshot)
        .await
        .unwrap();
    fixture
        .authority
        .update_split_head(
            &fixture.identity,
            snapshot,
            &SplitHeadUpdate {
                revision: other.revision,
                expected_revision: current.revision,
                expected_head: current.generation,
            },
        )
        .await
        .unwrap();
    let activated = fixture
        .authority
        .delete_split_revision(&fixture.identity, snapshot, revision, &ReadCancellation::new())
        .await
        .unwrap();
    rollback_gate.release();
    assert_eq!(deletion.await.unwrap().unwrap(), activated);
    assert!(fixture
        .authority
        .get_split_revision(
            &fixture.identity,
            snapshot,
            revision,
            budget(),
            &ReadCancellation::new()
        )
        .await
        .is_err());
    fixture.clock.store(2200, Ordering::Release);
    fixture.finish().await;
    fixture.assert_terminal().await;
}
