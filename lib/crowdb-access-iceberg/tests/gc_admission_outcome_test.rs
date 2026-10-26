// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_iceberg::catalog::{CatalogError, StoreError};

mod common {
    pub mod store;
    pub use store::TestStore;
    pub mod gc_store;
}
#[path = "common/gc_admission.rs"]
mod fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
use fixture::{TestAdmission, TestFault};

#[tokio::test]
async fn task_creation_unknown_fails_this_call_and_a_fresh_admission_recovers_the_same_task() {
    for retired in [false, true] {
        let fixture = TestAdmission::new(retired).await;
        fixture.store.arm(TestFault::Unknown);
        assert!(matches!(
            fixture.admit().await,
            Err(CatalogError::Store(StoreError::Response))
        ));
        assert_eq!(fixture.store.counts(), (5, 1));
        assert_eq!(fixture.durable(), Some(fixture.proposed.clone()));
        fixture.store.reset_counts();
        assert_eq!(
            fixture.admit_at(fixture::NOW_MS + 1_000).await.unwrap(),
            fixture.proposed
        );
        assert_eq!(fixture.store.counts(), if retired { (4, 0) } else { (3, 0) });
    }
}

#[tokio::test]
async fn applied_task_creation_and_existing_admission_keep_the_original_rpc_counts() {
    for retired in [false, true] {
        let fixture = TestAdmission::new(retired).await;
        assert_eq!(fixture.admit().await.unwrap(), fixture.proposed);
        assert_eq!(fixture.store.counts(), (5, 1));
        fixture.store.reset_counts();
        assert_eq!(fixture.admit().await.unwrap(), fixture.proposed);
        assert_eq!(fixture.store.counts(), if retired { (4, 0) } else { (3, 0) });
    }
    eprintln!("GC admission retired/purge cold: 5 GET + 1 CAS; observed conflict/unknown: 6 GET + 1 CAS -> 5 GET + 1 CAS; existing task retired 4 GET / purge 3 GET");
}

#[tokio::test]
async fn definite_create_conflicts_use_the_advanced_observed_task_without_a_get() {
    for retired in [false, true] {
        let fixture = TestAdmission::new(retired).await;
        fixture.store.arm(TestFault::Advanced);
        let task = fixture.admit().await.unwrap();
        assert_eq!(fixture.store.counts(), (5, 1));
        assert_eq!(task.revision, 2);
        assert_eq!(task.discovery_scope, 1);
        assert_eq!(task.retry_at_ms, fixture::NOW_MS + 1_000);
        assert_eq!(fixture.durable(), Some(task.clone()));
        fixture.store.reset_counts();
        assert_eq!(fixture.admit().await.unwrap(), task);
        assert_eq!(fixture.store.counts(), if retired { (4, 0) } else { (3, 0) });
    }
}

#[tokio::test]
async fn observed_tasks_with_changed_context_kind_or_purge_head_are_rejected_without_a_get() {
    for (retired, faults) in [
        (true, vec![TestFault::WrongContext, TestFault::WrongKind]),
        (
            false,
            vec![
                TestFault::WrongContext,
                TestFault::WrongKind,
                TestFault::WrongHead,
            ],
        ),
    ] {
        for fault in faults {
            let fixture = TestAdmission::new(retired).await;
            fixture.store.arm(fault);
            assert!(matches!(fixture.admit().await, Err(CatalogError::Conflict)));
            assert_eq!(fixture.store.counts(), (5, 1));
            let observed = fixture.durable().unwrap();
            assert_ne!(observed, fixture.proposed);
            fixture.store.reset_counts();
            assert!(matches!(fixture.admit().await, Err(CatalogError::Conflict)));
            assert_eq!(fixture.store.counts(), if retired { (4, 0) } else { (3, 0) });
            assert_eq!(fixture.durable(), Some(observed));
        }
    }
}

#[tokio::test]
async fn empty_conflicts_and_rejected_create_calls_stop_without_confirmation_reads() {
    for retired in [false, true] {
        for fault in [TestFault::Empty, TestFault::Rejected] {
            let fixture = TestAdmission::new(retired).await;
            fixture.store.arm(fault);
            let result = fixture.admit().await;
            match fault {
                TestFault::Empty => assert!(matches!(result, Err(CatalogError::Conflict))),
                TestFault::Rejected => {
                    assert!(matches!(result, Err(CatalogError::Store(StoreError::Budget))));
                }
                _ => unreachable!(),
            }
            assert_eq!(fixture.store.counts(), (5, 1));
            assert!(fixture.durable().is_none());
            assert_eq!(fixture.admit().await.unwrap(), fixture.proposed);
        }
    }
}
