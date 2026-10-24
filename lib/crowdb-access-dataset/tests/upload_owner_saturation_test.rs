// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/upload_saturation.rs"]
mod saturation;
#[path = "common/upload.rs"]
mod upload;

use crowdb_access_dataset::{
    CasOutcome, DatasetStore, MetadataScanBudget, ReadCancellation, StoreError, UploadError,
    UploadExpiryCursor, UploadState,
};
use md5::{Digest, Md5};
use saturation::{canonical, historical, owner, TestSaturation};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn saturated_retirement_fences_old_writers_and_preserves_existing_owner_retries() {
    let test = TestSaturation::new(4096, false).await;
    let original = test.raw().await;
    let before = test.store.counts();
    assert!(matches!(
        test.uploads.claim(&test.key, owner(0xc0), 4).await,
        Err(UploadError::Capacity)
    ));
    assert_eq!(test.store.delta(before), (1, 0, 0));
    let before = test.store.counts();
    test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
    assert_eq!(test.store.delta(before), (1, 1, 0));
    let fenced = test.raw().await;
    let value = test.value().await;
    assert_eq!(value["version"], 5);
    assert_eq!(value["retired_owners"].as_array().unwrap().len(), 4096);
    assert_eq!(value["owners"], json!([owner(0xb1)]));
    let old: serde_json::Value = serde_json::from_slice(&original).unwrap();
    assert!(matches!(old["version"].as_u64(), Some(2..=4)));
    assert!(
        !matches!(value["version"].as_u64(), Some(2..=4)),
        "old decoders reject this final fence"
    );
    let mut stale = old;
    stale["owners"] = json!([owner(0xb0), owner(0xb1), owner(0xc0)]);
    let CasOutcome::Conflict(Some(observed)) = test.stale_exchange(&original, &canonical(stale)).await else {
        panic!()
    };
    assert_eq!(observed.bytes, fenced);
    let restarted = test.restart();
    for identity in [owner(0xb1), owner(0xb1)] {
        let before = test.store.counts();
        assert_eq!(restarted.claim(&test.key, identity, 5).await.unwrap().version, 5);
        assert_eq!(test.store.delta(before), (1, 0, 0));
    }
    assert_denied(&test, owner(0xb0), false).await;
    assert_denied(&test, historical(0), true).await;
    assert_denied(&test, historical(4095), true).await;
    assert_eq!(
        restarted.read_part(&test.key, 0).await.unwrap(),
        b"data".as_slice()
    );
    assert!(matches!(
        restarted.abort(&test.key).await,
        Err(UploadError::Conflict)
    ));
    assert_eq!(test.raw().await, fenced);
    restarted.retire_owner(&test.key, owner(0xb1)).await.unwrap();
    assert_eq!(
        restarted.metadata(&test.key).await.unwrap().state,
        UploadState::Complete
    );
    let aborted = restarted.abort(&test.key).await.unwrap();
    assert_eq!(aborted.version, 5);
    assert!(aborted.cleanup_complete);
}

#[tokio::test]
async fn reaching_capacity_invalidates_a_paused_claim_without_a_confirmation_get() {
    let test = TestSaturation::new(4095, false).await;
    test.store.rival.store(1, Ordering::Relaxed);
    let before = test.store.counts();
    assert!(matches!(
        test.uploads.claim(&test.key, owner(0xc0), 4).await,
        Err(UploadError::Capacity)
    ));
    assert_eq!(test.store.delta(before), (1, 1, 0));
    let value = test.value().await;
    assert_eq!(value["version"], 4);
    assert_eq!(value["owners"], json!([owner(0xb1)]));
    assert_eq!(value["retired_owners"].as_array().unwrap().len(), 4096);
    assert_denied(&test, owner(0xb0), true).await;
    assert_denied(&test, owner(0xc0), false).await;
}

#[tokio::test]
async fn absent_retirement_at_capacity_fences_a_legacy_claim_before_cleanup_reports_success() {
    let test = TestSaturation::new(4096, false).await;
    let original = test.raw().await;
    let before = test.store.counts();
    test.uploads.retire_owner(&test.key, owner(0xc0)).await.unwrap();
    assert_eq!(test.store.delta(before), (1, 1, 0));
    let fenced = test.raw().await;
    let value = test.value().await;
    assert_eq!(value["version"], 5);
    assert_eq!(value["owners"], json!([owner(0xb0), owner(0xb1)]));
    assert_eq!(value["retired_owners"].as_array().unwrap().len(), 4096);
    let mut old: serde_json::Value = serde_json::from_slice(&original).unwrap();
    old["owners"] = json!([owner(0xb0), owner(0xb1), owner(0xc0)]);
    let CasOutcome::Conflict(Some(observed)) = test.stale_exchange(&original, &canonical(old)).await else {
        panic!()
    };
    assert_eq!(observed.bytes, fenced);
    let before = test.store.counts();
    test.restart().retire_owner(&test.key, owner(0xc0)).await.unwrap();
    assert_eq!(test.store.delta(before), (1, 0, 0));
    assert_denied(&test, owner(0xc0), false).await;
}

#[tokio::test]
async fn saturated_retirement_reuses_a_competing_release_observation() {
    let test = TestSaturation::new(4096, false).await;
    test.store.rival.store(2, Ordering::Relaxed);
    let before = test.store.counts();
    test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
    assert_eq!(test.store.delta(before), (1, 2, 0));
    let value = test.value().await;
    assert_eq!(value["version"], 5);
    assert_eq!(value["owners"], json!([]));
    assert_eq!(value["state"], "complete");
    for identity in [owner(0xb0), owner(0xb1)] {
        assert_denied(&test, identity, false).await;
    }
}

#[tokio::test]
async fn unknown_saturated_retirements_restart_and_expire_a_large_record_without_new_rpc_phases() {
    let test = TestSaturation::new(4096, true).await;
    assert_eq!(test.key.len(), 65_529);
    assert!(test.raw().await.len() > 256 * 1024);
    for identity in [owner(0xb0), owner(0xb1)] {
        test.store.lose_reply.store(true, Ordering::Relaxed);
        let before = test.store.counts();
        assert!(matches!(
            test.restart().retire_owner(&test.key, identity).await,
            Err(UploadError::Store(_))
        ));
        assert_eq!(test.store.delta(before), (1, 1, 0));
        let before = test.store.counts();
        test.restart().retire_owner(&test.key, identity).await.unwrap();
        assert_eq!(test.store.delta(before), (1, 0, 0));
        assert_denied(&test, identity, false).await;
    }
    assert_eq!(
        test.restart().metadata(&test.key).await.unwrap().state,
        UploadState::Complete
    );
    let small = MetadataScanBudget {
        items: 1,
        bytes: 256 * 1024,
        timeout: Duration::from_secs(2),
    };
    assert!(matches!(
        test.store
            .get_bounded(&test.key, small, &ReadCancellation::new())
            .await,
        Err(StoreError::Limit)
    ));
    let mut cursor = UploadExpiryCursor::default();
    let step = test.restart().expire_step(2000, &mut cursor).await.unwrap();
    assert_eq!(step.range_notifications, 1);
    assert_eq!(test.store.budget.load(Ordering::Relaxed), 352 * 1024);
    let record = test.restart().metadata(&test.key).await.unwrap();
    assert_eq!(record.version, 5);
    assert_eq!(record.state, UploadState::Aborted);
    assert!(record.cleanup_complete);
    assert!(test.restart().read_part(&test.key, 0).await.is_err());
    assert_denied(&test, owner(0xb0), true).await;
    assert_denied(&test, owner(0xb1), true).await;
}

#[tokio::test]
async fn all_upload_mutations_preserve_the_saturation_version() {
    let test = TestSaturation::new(4096, false).await;
    test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
    let restarted = test.restart();
    assert_eq!(restarted.complete(&test.key, 6).await.unwrap().version, 5);
    assert_eq!(
        restarted
            .create(&test.key, 4, 4, Md5::digest(b"data").into(), 1000)
            .await
            .unwrap()
            .version,
        5
    );
    restarted
        .put_part(&test.key, 0, bytes::Bytes::from_static(b"data"), 6)
        .await
        .unwrap();
    restarted.release(&test.key, owner(0xb1)).await.unwrap();
    assert_eq!(restarted.complete(&test.key, 6).await.unwrap().version, 5);
    assert_eq!(restarted.abort(&test.key).await.unwrap().version, 5);
    assert_eq!(test.value().await["version"], 5);
    let before = test.store.counts();
    restarted.retire_owner(&test.key, owner(0xc0)).await.unwrap();
    assert_eq!(test.store.delta(before), (1, 0, 0));
    assert_denied(&test, owner(0xb1), true).await;
}

#[tokio::test]
async fn absent_retirement_below_capacity_keeps_its_negative_fence_and_v5_requires_a_full_list() {
    let test = TestSaturation::new(4095, false).await;
    test.uploads.retire_owner(&test.key, owner(0xc0)).await.unwrap();
    assert_eq!(test.value().await["version"], 4);
    assert_denied(&test, owner(0xc0), true).await;
    let mut value = test.value().await;
    value["version"] = json!(5);
    value["retired_owners"].as_array_mut().unwrap().pop();
    test.install(value).await;
    assert!(matches!(
        test.restart().metadata(&test.key).await,
        Err(UploadError::Integrity)
    ));
}

#[tokio::test]
async fn canonical_maximum_owner_record_and_legal_metadata_key_fit_the_fixed_point_budget() {
    let test = TestSaturation::new(4096, true).await;
    let mut value = test.value().await;
    value["owners"] = json!((0_u16..256)
        .map(|index| {
            let mut bytes = [254; 16];
            bytes[15] = u8::try_from(index).unwrap();
            crowdb_access_dataset::OperationId::from_bytes(&bytes).unwrap()
        })
        .collect::<Vec<_>>());
    value["expires_at"] = json!(u64::MAX);
    test.install(value).await;
    let length = test.raw().await.len();
    assert!(length + test.key.len() > 256 * 1024);
    assert!(length <= 4352 * 66 + 1024);
    assert!(length + test.key.len() < 352 * 1024);
    let budget = MetadataScanBudget {
        items: 1,
        bytes: 352 * 1024,
        timeout: Duration::from_secs(2),
    };
    assert!(test
        .store
        .get_bounded(&test.key, budget, &ReadCancellation::new())
        .await
        .unwrap()
        .is_some());
    assert_eq!(test.restart().metadata(&test.key).await.unwrap().version, 4);
}

#[tokio::test]
async fn noncanonical_oversized_authorities_return_a_visible_expiry_limit() {
    let test = TestSaturation::new(4096, false).await;
    for identity in [owner(0xb0), owner(0xb1)] {
        test.uploads.retire_owner(&test.key, identity).await.unwrap();
    }
    let mut bytes = test.raw().await;
    bytes.extend(std::iter::repeat_n(b' ', 100_000));
    test.install_bytes(&bytes).await;
    assert!(matches!(
        test.restart()
            .expire_step(2000, &mut UploadExpiryCursor::default())
            .await,
        Err(UploadError::Store(StoreError::Limit))
    ));
    assert_eq!(test.store.budget.load(Ordering::Relaxed), 352 * 1024);
    assert_eq!(test.raw().await, bytes);
    let record = test.restart().metadata(&test.key).await.unwrap();
    assert_eq!(record.version, 5);
    assert_eq!(record.state, UploadState::Complete);
    assert!(!record.cleanup_complete);
}

async fn assert_denied(test: &TestSaturation, identity: crowdb_access_dataset::OperationId, conflict: bool) {
    let before = test.store.counts();
    let result = test.restart().claim(&test.key, identity, 7).await;
    assert!(
        matches!(
            (&result, conflict),
            (Err(UploadError::Conflict), true) | (Err(UploadError::Capacity), false)
        ),
        "{result:?}"
    );
    assert_eq!(test.store.delta(before), (1, 0, 0));
}
