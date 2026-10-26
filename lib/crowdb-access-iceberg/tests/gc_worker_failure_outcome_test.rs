// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common {
    pub mod store;
    pub use store::TestStore;
    pub mod gc_store;
}
#[path = "common/gc_admission.rs"]
#[allow(dead_code)]
mod admission;
#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/gc_failure.rs"]
mod failure;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;

use crowdb_access_iceberg::{
    catalog::{CatalogError, StoreError},
    gc::{GcLimits, GcStalledReason, GcWorkError, GcWorker},
};
use failure::{TestFailure, TestFault};
use std::sync::Arc;

fn worker(test: &TestFailure) -> GcWorker {
    GcWorker::new(
        test.repository(),
        Arc::new(blocks::TestBlocks::default()),
        GcLimits::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn a_lost_defer_reply_is_an_error_without_a_confirmation_read_or_false_success() {
    for retired in [true, false] {
        let test = TestFailure::new(retired, TestFault::Unknown).await;
        let result = worker(&test).run(&test.task, 10_000).await;
        assert!(
            matches!(
                result,
                Err(GcWorkError::Catalog(CatalogError::Store(StoreError::Response)))
            ),
            "{result:?}"
        );
        assert_eq!(test.counts(), (1, 1, 0));
        assert_eq!(test.total_reads(), 4);
        let durable = test.durable().unwrap();
        assert_eq!(durable.revision, test.task.revision + 1);
        assert_eq!(durable.stalled, GcStalledReason::Storage);
    }
}

#[tokio::test]
async fn normal_deferral_and_advanced_winners_need_no_task_reload_or_repeated_cas() {
    for fault in [TestFault::Applied, TestFault::Advanced] {
        for retired in [true, false] {
            let test = TestFailure::new(retired, fault).await;
            let result = worker(&test).run(&test.task, 10_000).await.unwrap();
            assert_eq!(test.counts(), (1, 1, 0));
            assert_eq!(test.total_reads(), 4);
            assert_eq!(test.durable(), Some(result.clone()));
            assert_eq!(result.paused, matches!(fault, TestFault::Advanced));
        }
    }
}

#[tokio::test]
async fn wrong_context_kind_and_head_observations_are_not_adopted_as_success() {
    for retired in [true, false] {
        let faults = if retired {
            vec![TestFault::WrongContext, TestFault::WrongKind]
        } else {
            vec![
                TestFault::WrongContext,
                TestFault::WrongKind,
                TestFault::WrongHead,
            ]
        };
        for fault in faults {
            let test = TestFailure::new(retired, fault).await;
            let result = worker(&test).run(&test.task, 10_000).await;
            assert!(
                matches!(result, Err(GcWorkError::Catalog(CatalogError::Conflict))),
                "{result:?}"
            );
            assert_eq!(test.counts(), (1, 1, 0));
        }
    }
}

#[tokio::test]
async fn rejected_or_missing_defer_results_remain_errors_without_confirmation_reads() {
    for fault in [TestFault::Rejected, TestFault::Empty] {
        let test = TestFailure::new(true, fault).await;
        let result = worker(&test).run(&test.task, 10_000).await;
        assert!(matches!(result, Err(GcWorkError::Catalog(_))), "{result:?}");
        assert_eq!(test.counts(), (1, 1, 0));
    }
}

#[tokio::test]
async fn retirement_owner_handoffs_reuse_known_state_and_never_confirm_an_unknown_update() {
    for fault in [TestFault::Applied, TestFault::Unknown] {
        let mut test = TestFailure::new(true, fault).await;
        let owner = test.install_retirement_owner().await;
        let result = worker(&test).run(&test.task, 10_000).await;
        if matches!(fault, TestFault::Applied) {
            assert_eq!(result.unwrap(), owner);
        } else {
            assert!(
                matches!(
                    result,
                    Err(GcWorkError::Catalog(CatalogError::Store(StoreError::Response)))
                ),
                "{result:?}"
            );
        }
        assert_eq!(test.counts(), (1, 1, 0));
        assert_eq!(test.durable(), Some(owner));
        assert_eq!(test.total_reads(), 6);
    }
}

#[tokio::test]
async fn independent_operator_controls_keep_their_strict_stale_update_contract() {
    let test = TestFailure::new(true, TestFault::Advanced).await;
    assert!(matches!(
        test.repository().pause(&test.task, true).await,
        Err(CatalogError::Conflict)
    ));
    assert_eq!(test.counts(), (0, 1, 0));
}
