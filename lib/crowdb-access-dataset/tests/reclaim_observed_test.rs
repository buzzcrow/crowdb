#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/deletion.rs"]
mod deletion;
#[path = "common/reclaim_observed.rs"]
mod observed;
#[path = "common/publication.rs"]
mod publication;

use crowdb_access_dataset::*;
use std::sync::{atomic::Ordering, Arc};

fn scoped(prefix: &[u8], fixture: &deletion::TestDeletionFixture, id: &[u8]) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend(fixture.identity.key().unwrap());
    key.extend(id);
    key
}

#[tokio::test]
async fn reclaim_reuses_four_head_observations_and_a_terminal_snapshot_without_confirmation_gets() {
    let fixture = deletion::TestDeletionFixture::new().await;
    assert!(!fixture.held.samples.is_empty());
    let head_key = scoped(b"DS\0H\0", &fixture, &[]);
    let head_value = fixture.store.get(&head_key).await.unwrap().unwrap();
    let (version, head, _): (u16, HeadRecord, OperationId) = bincode::deserialize(&head_value.bytes).unwrap();
    assert_eq!(version, 4);
    let snapshot_receipt = scoped(b"DS\0SC\0", &fixture, fixture.next.as_bytes());
    fixture.store.delete(&snapshot_receipt).await.unwrap();
    let snapshot = scoped(b"DS\0S\0", &fixture, fixture.snapshot.as_bytes());
    let store = Arc::new(observed::TestReclaimObserved::new(
        fixture.store.clone(),
        snapshot,
        head.clone(),
        observed::TestSnapshotReply::Conflict,
    ));
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(fixture.clock.clone());
    let retired = authority
        .begin_reclaim(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    assert_eq!(retired.publication.state, PublicationState::Aborted);
    assert_eq!(store.head_conflicts.load(Ordering::SeqCst), 0);
    assert_eq!(store.snapshot_conflicts.load(Ordering::SeqCst), 1);
    let current = fixture.authority.inspect_head(&fixture.identity).await.unwrap();
    assert_eq!(current.latest, head.latest);
    assert_eq!(current.stable, head.stable);
    assert_ne!(
        current.generation.unwrap(),
        CommitGeneration::from_head(&head_value.bytes)
    );
    assert!(fixture.store.get(&snapshot_receipt).await.unwrap().is_none());
}

#[tokio::test]
async fn missing_and_unknown_snapshot_cas_results_are_not_reclassified_by_another_get() {
    for reply in [
        observed::TestSnapshotReply::Missing,
        observed::TestSnapshotReply::Unknown,
    ] {
        let fixture = deletion::TestDeletionFixture::new().await;
        let known = fixture.authority.inspect_head(&fixture.identity).await.unwrap();
        let snapshot = scoped(b"DS\0S\0", &fixture, fixture.snapshot.as_bytes());
        let store = Arc::new(observed::TestReclaimObserved::new(
            fixture.store.clone(),
            snapshot,
            HeadRecord {
                latest: known.latest,
                stable: known.stable,
            },
            reply,
        ));
        let authority =
            DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(fixture.clock.clone());
        let result = authority.begin_reclaim(&fixture.identity, fixture.snapshot).await;
        assert!(result.is_err());
        if matches!(reply, observed::TestSnapshotReply::Missing) {
            assert!(matches!(result, Err(AuthorityError::SnapshotNotFound)));
        } else {
            assert_eq!(
                fixture
                    .authority
                    .begin_reclaim(&fixture.identity, fixture.snapshot)
                    .await
                    .unwrap()
                    .publication
                    .state,
                PublicationState::Aborted
            );
        }
        assert_eq!(store.snapshot_conflicts.load(Ordering::SeqCst), 1);
    }
}
