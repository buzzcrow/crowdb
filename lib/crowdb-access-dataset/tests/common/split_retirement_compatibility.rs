// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::retirement_fixture::TestFixture;
use crowdb_access_dataset::{split::*, *};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn closing_preserves_the_original_one_mib_revision_record_boundary() {
    let fixture = TestFixture::new().await;
    let mut record = fixture.record.clone();
    record.splits.truncate(1);
    let split = &mut record.splits[0];
    split.pages.clear();
    split.count = 0;
    let limit = usize::try_from(MAX_SPLIT_RECORD_BYTES).unwrap();
    let mut low = 0usize;
    let mut high = limit / 50;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        set_pages(&mut record, middle);
        if record.encode().is_ok() {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    set_pages(&mut record, low);
    let mut gap = limit - record.encode().unwrap().len();
    for page in record.splits[0].pages.iter_mut().rev() {
        for byte in &mut page.sha256 {
            if gap > 0 {
                let added = gap.min(2);
                *byte = if added == 2 { 100 } else { 10 };
                gap -= added;
            }
        }
        if gap == 0 {
            break;
        }
    }
    let original = record.encode().unwrap();
    assert_eq!(original.len(), limit);
    let old = fixture.store.inner.get(&fixture.root()).await.unwrap().unwrap();
    assert!(matches!(
        fixture
            .store
            .inner
            .compare_exchange(&fixture.root(), Some(&old.bytes), &original)
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    let result = fixture
        .authority
        .delete_split_revision(
            &fixture.identity,
            fixture.snapshot,
            record.revision,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.reclaim_after, Some(2200));
    let closing = fixture.store.inner.get(&fixture.root()).await.unwrap().unwrap();
    assert!(closing.bytes.len() > limit);
    assert!(closing.bytes.len() <= limit + 1024);
    fixture.clock.store(2200, Ordering::Release);
    fixture.finish().await;
    fixture.assert_terminal().await;
    reject_oversized_record(&fixture, record).await;
}

async fn reject_oversized_record(fixture: &TestFixture, mut record: SplitRevisionRecord) {
    let limit = usize::try_from(MAX_SPLIT_RECORD_BYTES).unwrap();
    let byte = record.splits[0]
        .pages
        .iter_mut()
        .flat_map(|page| &mut page.sha256)
        .find(|byte| **byte == 0)
        .unwrap();
    *byte = 10;
    assert_eq!(serde_json::to_vec(&record).unwrap().len(), limit + 1);
    assert!(record.encode().is_err());
    let bad = serde_json::to_vec(&serde_json::json!({
        "phase": "retiring", "reclaim_after": 2200, "record": record,
    }))
    .unwrap();
    let mut oversized = b"split-deletion-v1\0".to_vec();
    oversized.extend(bad);
    assert!(oversized.len() < limit + 1024);
    let old = fixture.store.inner.get(&fixture.root()).await.unwrap().unwrap();
    fixture
        .store
        .inner
        .compare_exchange(&fixture.root(), Some(&old.bytes), &oversized)
        .await
        .unwrap();
    let before = fixture.store.counts();
    assert!(matches!(
        fixture
            .authority
            .advance_split_revision_reclaim(&mut fixture.cursor(), &ReadCancellation::new())
            .await,
        Err(SplitLifecycleError::Split(SplitError::Limit))
    ));
    assert_eq!(fixture.store.counts().1, before.1);
}

fn set_pages(record: &mut SplitRevisionRecord, count: usize) {
    record.splits[0].pages = (0..count)
        .map(|index| MembershipPageBinding {
            page: u32::try_from(index).unwrap(),
            count: 1,
            sha256: [0; 32],
        })
        .collect();
    record.splits[0].count = u64::try_from(count).unwrap();
}

#[tokio::test]
async fn legacy_heads_upgrade_to_a_fresh_fail_closed_generation() {
    for wrapped in [false, true] {
        let fixture = TestFixture::new().await;
        let legacy = SplitHead {
            version: 1,
            snapshot: fixture.snapshot,
            revision: Some(SplitRevisionId::random()),
            fence: OperationId::random(),
        };
        let bytes = if wrapped {
            serde_json::to_vec(&serde_json::json!({
                "version": 2,
                "head": legacy,
                "attempt": CommitAttempt {
                    version: 1,
                    id: OperationId::random(),
                    target: CommitTarget::SplitRevision {
                        snapshot: fixture.snapshot,
                        revision: legacy.revision.unwrap(),
                    },
                    expected_head: None,
                },
            }))
            .unwrap()
        } else {
            legacy.encode().unwrap()
        };
        let key = key::head(&fixture.identity, fixture.snapshot).unwrap();
        assert!(matches!(
            fixture
                .store
                .inner
                .compare_exchange(&key, None, &bytes)
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
        fixture
            .authority
            .delete_split_revision(
                &fixture.identity,
                fixture.snapshot,
                fixture.record.revision,
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
        let stored = fixture.store.inner.get(&key).await.unwrap().unwrap();
        let upgraded = SplitHead::decode(&stored.bytes).unwrap();
        assert_eq!(upgraded.version, 3);
        assert_eq!(upgraded.revision, legacy.revision);
        assert_ne!(upgraded.fence, legacy.fence);
        assert_ne!(
            CommitGeneration::from_head(&stored.bytes),
            CommitGeneration::from_head(&bytes)
        );
        // The historical plain codec admits only version one; wrapped v2 also
        // requires an attempt and cannot decode this new plain-head resource.
        let old: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
        assert_ne!(old["version"], 1);
        assert!(old.get("head").is_none());
        let mut future = upgraded;
        future.version = 4;
        let corrupt = serde_json::to_vec(&future).unwrap();
        assert!(SplitHead::decode(&corrupt).is_err());
    }
}
