// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::{
    catalog::{CatalogError, StoreError},
    file::FileRepository,
    gc::{CandidatePhase, GcLimits, GcPhase, GcRepository},
    key::OperationId,
    record::StorageRecord,
};

mod common {
    pub mod store;
    pub use store::TestStore;
    pub mod file;
    pub mod gc_store;
}
#[path = "common/file_reupload_handoff.rs"]
mod handoff;
use handoff::{TestFault, TestHandoff, TestWrite, DELETED_MS};

#[tokio::test]
async fn immediate_reupload_hands_off_old_file_without_background_discovery() {
    for warm in [false, true] {
        let fixture = TestHandoff::new(warm).await;
        assert!(fixture.candidate().is_none());
        assert_eq!(fixture.publish().await.unwrap(), fixture.replacement);
        assert_eq!(fixture.store.counts(), if warm { (8, 4) } else { (9, 5) });
        fixture.assert_handoff();
        assert_eq!(
            fixture.authority(),
            StorageRecord::File(Box::new(fixture.replacement))
        );
    }
    eprintln!("same-path reupload: existing candidate 2 GET + 2 CAS; warm handoff 8 GET + 4 CAS; cold handoff 9 GET + 5 CAS");
}

#[tokio::test]
async fn ordinary_publication_keeps_one_get_one_cas_without_gc_access() {
    let fixture = TestHandoff::new(false).await;
    let file = fixture.fixture.record("metadata/another.json", b"complete");
    assert_eq!(
        FileRepository::new(fixture.store.clone())
            .publish(fixture.fixture.context, &file)
            .await
            .unwrap(),
        file
    );
    assert_eq!(fixture.store.counts(), (1, 1));
    assert!(fixture.candidate().is_none());
}

#[tokio::test]
async fn a_conflicting_handoff_authority_cannot_replace_the_filename() {
    for (target, warm) in [
        (TestWrite::Task, false),
        (TestWrite::Claim, true),
        (TestWrite::Candidate, true),
    ] {
        let fixture = TestHandoff::new(warm).await;
        fixture.store.arm(target, TestFault::ChangedAuthority);
        assert!(fixture.publish().await.is_err());
        assert_eq!(
            fixture.authority(),
            StorageRecord::DeletedFile(Box::new(fixture.deleted))
        );
    }
}

#[tokio::test]
async fn an_existing_candidate_preserves_the_two_get_two_cas_path() {
    let fixture = TestHandoff::new(true).await;
    let gc = GcRepository::new(fixture.fixture.store.clone());
    let before = gc
        .claim_deleted_file(
            fixture.fixture.context,
            &fixture.deleted,
            DELETED_MS,
            GcLimits::default(),
        )
        .await
        .unwrap();
    fixture.store.reset_counts();
    fixture.publish().await.unwrap();
    assert_eq!(fixture.store.counts(), (2, 2));
    assert_eq!(fixture.assert_handoff(), before);
}

#[tokio::test]
async fn each_uncertain_handoff_write_stops_before_canonical_replacement_and_restarts_safely() {
    for (target, warm, counts) in [
        (TestWrite::Task, false, (5, 2)),
        (TestWrite::Claim, true, (6, 2)),
        (TestWrite::Candidate, true, (8, 3)),
    ] {
        let fixture = TestHandoff::new(warm).await;
        fixture.store.arm(target, TestFault::Unknown);
        assert!(matches!(
            fixture.publish().await,
            Err(CatalogError::Store(StoreError::Response))
        ));
        assert_eq!(fixture.store.counts(), counts);
        assert_eq!(
            fixture.authority(),
            StorageRecord::DeletedFile(Box::new(fixture.deleted.clone()))
        );
        match target {
            TestWrite::Task => {
                assert!(fixture.claim().is_none());
                assert!(fixture.candidate().is_none());
            }
            TestWrite::Claim => {
                assert_eq!(fixture.claim().unwrap().file, fixture.deleted.file);
                assert!(fixture.candidate().is_none());
            }
            TestWrite::Candidate => {
                fixture.assert_handoff();
            }
            TestWrite::Replacement => unreachable!(),
        }
        // A fresh repository uses only durable facts after the interrupted call.
        assert_eq!(fixture.publish().await.unwrap(), fixture.replacement);
        fixture.assert_handoff();
    }
}

#[tokio::test]
async fn definitive_conflicts_use_the_observed_task_claim_and_advanced_candidate() {
    for (target, warm, counts) in [
        (TestWrite::Task, false, (9, 5)),
        (TestWrite::Claim, true, (8, 4)),
        (TestWrite::Candidate, true, (8, 4)),
    ] {
        let fixture = TestHandoff::new(warm).await;
        fixture.store.arm(target, TestFault::Winner);
        assert_eq!(fixture.publish().await.unwrap(), fixture.replacement);
        assert_eq!(fixture.store.counts(), counts);
        let candidate = fixture.assert_handoff();
        match target {
            TestWrite::Claim => assert_eq!(candidate.generation, 7),
            TestWrite::Candidate => {
                assert_eq!(candidate.revision, 2);
                assert_eq!(candidate.phase, CandidatePhase::Deleting);
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn missing_or_rejected_handoff_conflicts_keep_the_only_deleted_file_fact() {
    for target in [TestWrite::Task, TestWrite::Claim, TestWrite::Candidate] {
        for fault in [TestFault::EmptyConflict, TestFault::Rejected] {
            let fixture = TestHandoff::new(!matches!(target, TestWrite::Task)).await;
            fixture.store.arm(target, fault);
            assert!(fixture.publish().await.is_err());
            assert_eq!(
                fixture.authority(),
                StorageRecord::DeletedFile(Box::new(fixture.deleted.clone()))
            );
            assert_eq!(fixture.publish().await.unwrap(), fixture.replacement);
            fixture.assert_handoff();
        }
    }
}

#[tokio::test]
async fn failed_or_unknown_replacement_keeps_the_old_file_independently_discoverable() {
    for fault in [
        TestFault::EmptyConflict,
        TestFault::Rejected,
        TestFault::Unknown,
        TestFault::Winner,
    ] {
        let fixture = TestHandoff::new(true).await;
        fixture.store.arm(TestWrite::Replacement, fault);
        assert!(fixture.publish().await.is_err());
        assert_eq!(fixture.store.counts(), (8, 4));
        fixture.assert_handoff();
        match fault {
            TestFault::Unknown | TestFault::Winner => assert_eq!(
                fixture.authority(),
                StorageRecord::File(Box::new(fixture.replacement.clone()))
            ),
            _ => assert_eq!(
                fixture.authority(),
                StorageRecord::DeletedFile(Box::new(fixture.deleted.clone()))
            ),
        }
        assert_eq!(fixture.publish().await.unwrap(), fixture.replacement);
        fixture.assert_handoff();
    }
}

#[tokio::test]
async fn replacement_cannot_reuse_the_deleted_physical_file_identity() {
    let fixture = TestHandoff::new(true).await;
    let mut replacement = fixture.replacement.clone();
    replacement.file = fixture.deleted.file.file;
    assert!(matches!(
        FileRepository::new(fixture.store.clone())
            .publish(fixture.fixture.context, &replacement)
            .await,
        Err(CatalogError::Conflict)
    ));
    assert_eq!(fixture.store.counts(), (1, 1));
    assert!(fixture.candidate().is_none());
    assert_eq!(
        fixture.authority(),
        StorageRecord::DeletedFile(Box::new(fixture.deleted))
    );
}

#[tokio::test]
async fn observed_stopped_tasks_retain_independent_cleanup_for_reopening_or_operator_resume() {
    for fault in [
        TestFault::CompleteTask,
        TestFault::PausedTask,
        TestFault::QuarantinedTask,
    ] {
        let fixture = TestHandoff::new(false).await;
        fixture.store.arm(TestWrite::Task, fault);
        fixture.publish().await.unwrap();
        assert_eq!(fixture.store.counts(), (9, 5));
        let candidate = fixture.assert_handoff();
        let gc = GcRepository::new(fixture.fixture.store.clone());
        let identity = OperationId::from_bytes(b"multipart-gc-v01").unwrap();
        let stopped = gc
            .task(fixture.fixture.context.catalog, identity)
            .await
            .unwrap()
            .unwrap();
        let resumed = match fault {
            TestFault::CompleteTask => {
                assert_eq!(stopped.phase, GcPhase::Complete);
                // The scheduler's independent admission lane does this every tick.
                gc.admit_multipart(fixture.fixture.context, DELETED_MS + 1, GcLimits::default())
                    .await
                    .unwrap()
            }
            TestFault::PausedTask => {
                assert!(stopped.paused);
                gc.pause(&stopped, false).await.unwrap()
            }
            TestFault::QuarantinedTask => {
                assert_eq!(stopped.phase, GcPhase::Quarantined);
                gc.retry(&stopped).await.unwrap()
            }
            _ => unreachable!(),
        };
        assert!(!resumed.paused);
        assert_eq!(resumed.phase, GcPhase::Discover);
        assert_eq!(candidate.task, resumed.identity);
        assert_eq!(fixture.assert_handoff(), candidate);
    }
}
