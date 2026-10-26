// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common {
    pub mod store;
    pub use store::TestStore;
    pub mod file;
    pub mod gc_store;
}
#[path = "common/gc_inline_progress.rs"]
mod inline;

use crowdb_access_iceberg::{
    catalog::{CatalogError, StoreError},
    file::{canonical_file_key, FileContent, FileRecord},
    gc::{CandidatePhase, GcStalledReason, GcWorkError, TreeReclaimCursor},
    key::{FileId, StorageKey},
    record::StorageRecord,
    table::storage_head_key,
};
use inline::{TestFault, TestInline, TestStage, NOW};
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn one_inline_sweep_step_completes_without_a_deleting_checkpoint_or_block_io() {
    let test = TestInline::new().await;
    let next = test.worker().step(&test.task, NOW).await.unwrap();
    let candidate = test.durable_candidate();
    eprintln!(
        "inline sweep G/S/C/D={:?}, phase={:?}, deleted={}",
        test.store.counts(),
        candidate.phase,
        next.deleted
    );
    assert_eq!(candidate.phase, CandidatePhase::Complete);
    assert_eq!(candidate.completed_round, test.task.sweep_round);
    assert!(test.file().is_none());
    assert_eq!(next.deleted, 1);
    assert_eq!(next.reclaimed_bytes, test.candidate.file.length);
    assert_eq!(test.store.counts(), (9, 1, 2, 1));
    assert_eq!(test.blocks.0.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn unknown_resource_or_progress_results_stop_without_confirmation_reads_and_recover() {
    for (stage, counts) in [
        (TestStage::Delete, (7, 1, 0, 1)),
        (TestStage::Candidate, (8, 1, 1, 1)),
        (TestStage::Task, (9, 1, 2, 1)),
    ] {
        for fault in [TestFault::UnknownBefore, TestFault::UnknownAfter] {
            let test = TestInline::new().await;
            test.store.arm(stage, fault);
            let result = test.worker().step(&test.task, NOW).await;
            assert!(test.store.fault_consumed(), "{stage:?}/{fault:?}");
            assert!(
                matches!(
                    result,
                    Err(GcWorkError::Catalog(CatalogError::Store(StoreError::Response)))
                ),
                "{stage:?}/{fault:?}: {result:?}"
            );
            assert_eq!(test.store.counts(), counts, "{stage:?}/{fault:?}");
            assert_eq!(test.store.following_gets(), (0, 0));
            assert_eq!(
                test.file().is_some(),
                stage == TestStage::Delete && fault == TestFault::UnknownBefore
            );
            let completed = stage == TestStage::Task
                || (stage == TestStage::Candidate && fault == TestFault::UnknownAfter);
            assert_eq!(
                test.durable_candidate().phase == CandidatePhase::Complete,
                completed
            );
            assert_eq!(
                test.durable_task().deleted,
                u64::from(stage == TestStage::Task && fault == TestFault::UnknownAfter)
            );
            assert!(test
                .record(&StorageKey::Legacy(test.candidate.claim_key()))
                .is_some());
            test.assert_no_block_io();
            test.store.reset();
            let current = test.durable_task();
            let next = test.worker().step(&current, NOW).await.unwrap();
            let retry_counts = match (stage, fault) {
                (TestStage::Delete, TestFault::UnknownBefore) => (9, 1, 2, 1),
                (TestStage::Delete, TestFault::UnknownAfter)
                | (TestStage::Candidate, TestFault::UnknownBefore) => (9, 1, 2, 0),
                (TestStage::Candidate, TestFault::UnknownAfter)
                | (TestStage::Task, TestFault::UnknownBefore) => (7, 1, 1, 0),
                (TestStage::Task, TestFault::UnknownAfter) => (5, 1, 1, 0),
                _ => unreachable!(),
            };
            assert_eq!(test.store.counts(), retry_counts, "{stage:?}/{fault:?}");
            assert_eq!(next.deleted, 1);
            test.recover().await;
            assert!(test.file().is_none());
        }
    }
}

#[tokio::test]
async fn run_wrapper_retry_bookkeeping_is_counted_separately_from_the_step() {
    for (stage, counts) in [
        (TestStage::Delete, (8, 1, 1, 1)),
        (TestStage::Candidate, (9, 1, 2, 1)),
        (TestStage::Task, (10, 1, 3, 1)),
    ] {
        for fault in [TestFault::UnknownBefore, TestFault::UnknownAfter] {
            let test = TestInline::new().await;
            test.store.arm(stage, fault);
            let next = test.worker().run(&test.task, NOW).await.unwrap();
            assert!(test.store.fault_consumed(), "{stage:?}/{fault:?}");
            assert_eq!(test.store.counts(), counts, "{stage:?}/{fault:?}");
            assert_eq!(test.store.following_gets(), (1, 0));
            if stage == TestStage::Task && fault == TestFault::UnknownAfter {
                assert_eq!(next.stalled, GcStalledReason::None);
                assert_eq!(next.deleted, 1);
            } else {
                assert_eq!(next.stalled, GcStalledReason::Storage);
                assert!(next.retry_at_ms > NOW);
                assert_eq!(next.deleted, 0);
            }
            assert_ne!(next.phase, crowdb_access_iceberg::gc::GcPhase::Complete);
            test.recover().await;
        }
    }
}

#[tokio::test]
async fn exact_and_advanced_cas_observations_preserve_once_only_accounting_without_reload() {
    for stage in [TestStage::Delete, TestStage::Candidate, TestStage::Task] {
        for fault in [TestFault::ConflictSame, TestFault::ConflictAdvanced] {
            let test = TestInline::new().await;
            test.store.arm(stage, fault);
            let result = test.worker().step(&test.task, NOW).await;
            assert!(test.store.fault_consumed(), "{stage:?}/{fault:?}");
            if fault == TestFault::ConflictSame {
                assert_eq!(result.unwrap().deleted, 1);
                assert_eq!(test.store.counts(), (9, 1, 2, 1));
            } else {
                assert!(
                    matches!(result, Err(GcWorkError::Catalog(CatalogError::Conflict))),
                    "{stage:?}: {result:?}"
                );
                let counts = match stage {
                    TestStage::Delete => (7, 1, 0, 1),
                    TestStage::Candidate => (8, 1, 1, 1),
                    TestStage::Task => (9, 1, 2, 1),
                };
                assert_eq!(test.store.counts(), counts);
            }
            assert_eq!(test.store.following_gets().1, 0);
            let replacement = test.file();
            test.recover().await;
            if stage == TestStage::Delete && fault == TestFault::ConflictAdvanced {
                assert_ne!(replacement.as_ref().unwrap().file, test.candidate.file.file);
                assert_eq!(test.file(), replacement);
            } else {
                assert!(test.file().is_none());
            }
        }
    }
}

fn changed(file: &FileRecord, keep_identity: bool) -> FileRecord {
    let bytes = b"{\"replacement\":true}";
    let mut next = file.clone();
    if !keep_identity {
        next.file = FileId::random();
    }
    next.length = bytes.len() as u64;
    next.digest = Sha256::digest(bytes).into();
    next.content = FileContent::select_inline(next.kind, bytes).unwrap();
    next
}

#[tokio::test]
async fn replacement_identity_survives_but_same_identity_changed_content_is_rejected() {
    for keep_identity in [false, true] {
        let test = TestInline::new().await;
        let replacement = changed(&test.candidate.file, keep_identity);
        test.install(
            &canonical_file_key(&replacement.location),
            &StorageRecord::File(Box::new(replacement.clone())),
        )
        .await;
        let result = test.worker().step(&test.task, NOW).await;
        if keep_identity {
            assert!(matches!(
                result,
                Err(GcWorkError::Catalog(CatalogError::Conflict))
            ));
            assert_eq!(test.store.counts(), (7, 1, 0, 0));
            assert_eq!(test.durable_candidate().phase, CandidatePhase::Retained);
        } else {
            assert_eq!(result.unwrap().deleted, 1);
            assert_eq!(test.store.counts(), (9, 1, 2, 0));
            test.recover().await;
        }
        assert_eq!(test.file(), Some(replacement));
        test.assert_no_block_io();
    }
}

#[tokio::test]
async fn changed_head_and_missing_or_wrong_claim_reject_before_canonical_deletion() {
    for guard in ["generation", "fence", "missing-claim", "wrong-claim"] {
        let test = TestInline::new().await;
        let key = storage_head_key(test.candidate.file.location.table());
        if matches!(guard, "generation" | "fence") {
            let Some(StorageRecord::TableHead(mut head)) = test.record(&key) else {
                panic!("head")
            };
            if guard == "generation" {
                head.generation += 1;
            } else {
                head.operation_fence += 1;
            }
            test.install(&key, &StorageRecord::TableHead(head)).await;
        } else if guard == "missing-claim" {
            let key = test.candidate.claim_key().encode().unwrap();
            test.store.inner.values.rcu(|values| {
                let mut next = (**values).clone();
                next.remove(&key);
                next
            });
        } else {
            let mut claim = test.candidate.clone();
            claim.file = changed(&claim.file, true);
            test.install(
                &StorageKey::Legacy(claim.claim_key()),
                &StorageRecord::GcCandidate(Box::new(claim)),
            )
            .await;
        }
        assert!(test.worker().step(&test.task, NOW).await.is_err(), "{guard}");
        assert_eq!(
            test.store.counts(),
            (
                if matches!(guard, "generation" | "fence") {
                    5
                } else {
                    6
                },
                1,
                0,
                0
            )
        );
        assert_eq!(test.file(), Some(test.candidate.file.clone()));
        assert_eq!(test.durable_candidate().phase, CandidatePhase::Retained);
        test.assert_no_block_io();
    }
}

#[tokio::test]
async fn both_fixed_deadlines_still_prevent_early_inline_deletion() {
    for task_deadline in [false, true] {
        let mut test = TestInline::new().await;
        if task_deadline {
            test.task.not_before_ms = NOW + 1;
            test.install(
                &StorageKey::Legacy(test.task.key()),
                &StorageRecord::GcTask(Box::new(test.task.clone())),
            )
            .await;
            assert!(matches!(
                test.worker().step(&test.task, NOW).await,
                Err(GcWorkError::Catalog(CatalogError::Busy))
            ));
            assert_eq!(test.store.counts(), (4, 0, 0, 0));
        } else {
            let mut candidate = test.candidate.clone();
            candidate.not_before_ms = NOW + 1;
            test.install(
                &StorageKey::Legacy(candidate.key()),
                &StorageRecord::GcCandidate(Box::new(candidate)),
            )
            .await;
            let next = test.worker().step(&test.task, NOW).await.unwrap();
            assert_eq!(next.stalled, GcStalledReason::Retention);
            assert_eq!(next.retry_at_ms, NOW + 1);
            assert_eq!(test.store.counts(), (7, 1, 1, 0));
        }
        assert!(test.file().is_some());
        assert_eq!(test.durable_candidate().phase, CandidatePhase::Retained);
        test.assert_no_block_io();
        test.store.reset();
        let next = test.worker().step(&test.durable_task(), NOW + 1).await.unwrap();
        assert_eq!(next.deleted, 1);
        assert_eq!(test.store.counts(), (9, 1, 2, 1));
        test.recover().await;
    }
}

#[tokio::test]
async fn active_resource_cleanup_cannot_delete_a_published_inline_file() {
    let mut test = TestInline::new().await;
    test.task.kind = crowdb_access_iceberg::gc::GcTaskKind::MultipartCleanup;
    test.task.head = None;
    test.task.fenced = false;
    test.install(
        &StorageKey::Legacy(test.task.key()),
        &StorageRecord::GcTask(Box::new(test.task.clone())),
    )
    .await;
    assert!(matches!(
        test.worker().step(&test.task, NOW).await,
        Err(GcWorkError::Catalog(CatalogError::Busy))
    ));
    assert_eq!(test.durable_candidate().phase, CandidatePhase::Retained);
    assert_eq!(test.file(), Some(test.candidate.file.clone()));
    assert_eq!(test.store.counts(), (5, 1, 0, 0));
    test.assert_no_block_io();
}

#[tokio::test]
async fn even_empty_chunk_descriptors_keep_the_existing_deleting_checkpoint() {
    let test = TestInline::new().await;
    let mut candidate = test.candidate.clone();
    candidate.file.length = 0;
    candidate.file.digest = Sha256::digest([]).into();
    candidate.file.content = FileContent::Chunks { root: None };
    candidate.cursor = TreeReclaimCursor::new(&candidate.file).unwrap();
    for key in [candidate.claim_key(), candidate.key()] {
        test.install(
            &StorageKey::Legacy(key),
            &StorageRecord::GcCandidate(Box::new(candidate.clone())),
        )
        .await;
    }
    let next = test.worker().step(&test.task, NOW).await.unwrap();
    assert_eq!(next.deleted, 0);
    assert_eq!(test.durable_candidate().phase, CandidatePhase::Deleting);
    assert_eq!(test.store.counts(), (8, 1, 2, 0));
    assert!(test.file().is_some());
    test.assert_no_block_io();
}
