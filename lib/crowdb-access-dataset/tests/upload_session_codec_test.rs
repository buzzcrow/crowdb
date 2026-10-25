// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/upload_session_codec.rs"]
mod codec;
#[path = "common/upload.rs"]
mod upload;

use bytes::Bytes;
use codec::{assert_binary, historical, owner, TestCodecFixture, HEADER_BYTES, MAGIC};
use crowdb_access_dataset::{CasOutcome, DatasetStore, DatasetUploads, UploadError, UploadRecord};
use md5::{Digest, Md5};
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};
use upload::{TestPayload, TestStore};

#[tokio::test]
async fn retiring_a_saturated_resource_is_lossless_and_bounded_without_extra_rpc() {
    let test = TestCodecFixture::new(4, 4096).await;
    let before = test.raw().await;
    assert!(before.len() > 200_000);
    test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
    assert_eq!(test.store.counts(), (1, 1));
    let bytes = test.raw().await;
    assert_eq!(bytes.len(), 65_604);
    assert!(bytes.starts_with(MAGIC));
    assert!(serde_json::from_slice::<UploadRecord>(&bytes).is_err());
    let mut expected = test.legacy.clone();
    expected["version"] = json!(5);
    expected["owners"] = json!([owner(0xb1)]);
    assert_eq!(test.public().await, expected);
    for identity in [historical(0), historical(4095)] {
        let before = test.store.counts();
        assert!(matches!(
            test.restart().claim(&test.key, identity, 5).await,
            Err(UploadError::Conflict)
        ));
        assert_eq!(test.store.counts(), (before.0 + 1, before.1));
    }
    for identity in [owner(0xb0), owner(0xc0)] {
        assert!(matches!(
            test.restart().claim(&test.key, identity, 5).await,
            Err(UploadError::Capacity)
        ));
    }
    assert_eq!(
        test.restart()
            .claim(&test.key, owner(0xb1), 5)
            .await
            .unwrap()
            .version,
        5
    );
    let before = test.store.counts();
    test.restart().retire_owner(&test.key, owner(0xb0)).await.unwrap();
    assert_eq!(test.store.counts(), (before.0 + 1, before.1));
    assert_eq!(test.raw().await, bytes);
}

#[tokio::test]
async fn every_session_writer_uses_the_codec_while_part_entries_remain_json() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    let key = b"DS\0U\0writers";
    uploads
        .create(key, 4, 4, Md5::digest(b"data").into(), 1000)
        .await
        .unwrap();
    assert_binary(&store, key).await;
    uploads
        .put_part(key, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    let mut part = key.to_vec();
    part.extend_from_slice(b"\0P\0\0\0\0");
    let entry = store.get(&part).await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&entry.bytes).unwrap()["state"],
        "Accepted"
    );
    uploads.complete(key, 2).await.unwrap();
    assert_binary(&store, key).await;
    uploads.claim(key, owner(0xb0), 3).await.unwrap();
    assert_binary(&store, key).await;
    uploads.release(key, owner(0xb0)).await.unwrap();
    assert_binary(&store, key).await;
    uploads.claim(key, owner(0xb1), 3).await.unwrap();
    assert_binary(&store, key).await;
    uploads.retire_owner(key, owner(0xb1)).await.unwrap();
    assert_binary(&store, key).await;
    payloads.defer_reclaim.store(true, Ordering::Relaxed);
    let aborted = uploads.abort(key).await.unwrap();
    assert_eq!(aborted.version, 4);
    assert!(!aborted.cleanup_complete);
    assert_binary(&store, key).await;
    payloads.defer_reclaim.store(false, Ordering::Relaxed);
    let completed = uploads.abort(key).await.unwrap();
    assert!(completed.cleanup_complete);
    assert_binary(&store, key).await;
    uploads
        .create(key, 4, 4, Md5::digest(b"data").into(), 1000)
        .await
        .unwrap();
    assert_binary(&store, key).await;
    assert_eq!(uploads.metadata(key).await.unwrap(), completed);
}

#[tokio::test]
async fn old_json_versions_and_accepted_zero_identities_keep_the_same_public_values() {
    for version in 2..=5 {
        let test = TestCodecFixture::new(version, if version == 5 { 4096 } else { 0 }).await;
        assert_eq!(
            test.restart().metadata(&test.key).await.unwrap().version,
            version.max(4)
        );
        test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
        assert!(test.raw().await.starts_with(MAGIC));
        assert_eq!(
            test.restart().metadata(&test.key).await.unwrap().version,
            version.max(4)
        );
    }
    for retired in [false, true] {
        let test = TestCodecFixture::new(4, 0).await;
        let mut value = test.legacy.clone();
        let zero = [0_u8; 16];
        if retired {
            value["retired_owners"] = json!([zero]);
        } else {
            value["owners"] = json!([zero, owner(0xb0), owner(0xb1)]);
        }
        test.install_json(&value).await;
        test.uploads.release(&test.key, owner(0xb0)).await.unwrap();
        assert!(test.raw().await.starts_with(MAGIC));
        value["owners"] = if retired {
            json!([owner(0xb1)])
        } else {
            json!([zero, owner(0xb1)])
        };
        let expected: UploadRecord = serde_json::from_value(value).unwrap();
        assert_eq!(test.public().await, serde_json::to_value(expected).unwrap());
    }
}

#[tokio::test]
async fn live_identity_order_and_duplicates_survive_lossless_storage_migration() {
    let test = TestCodecFixture::new(4, 2).await;
    let zero = [0_u8; 16];
    let mut expected = test.legacy.clone();
    expected["owners"] = json!([owner(0xb1), owner(0xb0), owner(0xb1), zero]);
    test.install_json(&expected).await;
    test.uploads.release(&test.key, owner(0xb0)).await.unwrap();
    expected["owners"] = json!([owner(0xb1), owner(0xb1), zero]);
    assert_eq!(test.public().await, expected);
    let raw = test.raw().await;
    assert!(raw.starts_with(MAGIC));
    let before = test.store.counts();
    test.restart().claim(&test.key, owner(0xb1), 5).await.unwrap();
    assert_eq!(test.store.counts(), (before.0 + 1, before.1));
    assert_eq!(test.raw().await, raw);
}

#[tokio::test]
async fn the_maximum_resource_keeps_every_id_and_has_an_exact_fixed_size() {
    let test = TestCodecFixture::new(4, 4096).await;
    let mut expected = test.legacy.clone();
    expected["owners"] = json!(vec![owner(0xb1); 256]);
    test.install_json(&expected).await;
    test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
    let raw = test.raw().await;
    assert!(raw.starts_with(MAGIC));
    assert_eq!(raw.len(), HEADER_BYTES + 16 * (4096 + 256));
    expected["version"] = json!(5);
    assert_eq!(test.public().await, expected);
}

#[tokio::test]
async fn legacy_json_keeps_the_entire_original_rejection_contract() {
    let test = TestCodecFixture::new(4, 0).await;
    let original = test.legacy.clone();
    let mut cases = Vec::new();
    for (name, value) in [
        ("version", json!(1)),
        ("version", json!(6)),
        ("length", json!(0)),
        ("part_size", json!(0)),
        ("part_size", json!(4 * 1024 * 1024 + 1)),
        ("owners", json!(vec![owner(0xb1); 257])),
        (
            "retired_owners",
            json!((0..4097).map(historical).collect::<Vec<_>>()),
        ),
        ("retired_owners", json!([historical(1), historical(0)])),
        ("retired_owners", json!([historical(0), historical(0)])),
        ("retired_owners", json!([owner(0xb0)])),
    ] {
        let mut value_record = original.clone();
        value_record[name] = value;
        cases.push(value_record);
    }
    let mut legacy = original.clone();
    legacy["version"] = json!(2);
    legacy["retired_owners"] = json!([historical(0)]);
    cases.push(legacy);
    let mut saturated = original.clone();
    saturated["version"] = json!(5);
    saturated["retired_owners"] = json!((0..4095).map(historical).collect::<Vec<_>>());
    cases.push(saturated);
    let mut too_many_parts = original;
    too_many_parts["part_size"] = json!(1);
    too_many_parts["length"] = json!(10_001);
    cases.push(too_many_parts);
    for value in cases {
        test.install_json(&value).await;
        assert!(matches!(
            test.restart().metadata(&test.key).await,
            Err(UploadError::Integrity)
        ));
    }
}

#[tokio::test]
async fn unknown_codec_transitions_stop_without_confirmation_and_retry_from_durable_bytes() {
    for fault in [1, 2] {
        let test = TestCodecFixture::new(5, 4096).await;
        let old = test.raw().await;
        test.store.fault.store(fault, Ordering::Relaxed);
        assert!(matches!(
            test.uploads.retire_owner(&test.key, owner(0xb0)).await,
            Err(UploadError::Store(_))
        ));
        assert_eq!(test.store.counts(), (1, 1));
        if fault == 1 {
            assert_eq!(test.raw().await, old);
        } else {
            assert!(test.raw().await.starts_with(MAGIC));
        }
        let before = test.store.counts();
        test.restart().retire_owner(&test.key, owner(0xb0)).await.unwrap();
        assert_eq!(
            test.store.counts(),
            (before.0 + 1, before.1 + u64::from(fault == 1))
        );
        assert_eq!(test.public().await["owners"], json!([owner(0xb1)]));
    }
}

#[tokio::test]
async fn paused_json_cas_cannot_restore_an_owner_after_binary_retirement() {
    let test = TestCodecFixture::new(5, 4096).await;
    let old = test.raw().await;
    let mut proposed = test.legacy.clone();
    proposed["owners"] = json!([owner(0xb0), owner(0xb1), owner(0xc0)]);
    let proposed = serde_json::to_vec(&serde_json::from_value::<UploadRecord>(proposed).unwrap()).unwrap();
    test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
    let outcome = test
        .store
        .inner
        .compare_exchange(&test.key, Some(&old), &proposed)
        .await
        .unwrap();
    let CasOutcome::Conflict(Some(observed)) = outcome else {
        panic!()
    };
    assert_eq!(observed.bytes, test.raw().await);
    assert!(serde_json::from_slice::<UploadRecord>(&observed.bytes).is_err());
    assert!(matches!(
        test.restart().claim(&test.key, owner(0xb0), 5).await,
        Err(UploadError::Capacity)
    ));
}

#[tokio::test]
async fn conflict_reuses_the_observed_binary_record_without_a_get_or_lost_release() {
    let test = TestCodecFixture::new(5, 4096).await;
    test.store.fault.store(3, Ordering::Relaxed);
    test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
    assert_eq!(test.store.counts(), (1, 2));
    assert_eq!(test.public().await["owners"], json!([]));
    assert_eq!(test.public().await["state"], "complete");
    assert!(test.raw().await.starts_with(MAGIC));
}

#[tokio::test]
async fn a_legacy_json_claim_wins_before_conversion_and_survives_the_observed_retry() {
    let test = TestCodecFixture::new(4, 2).await;
    test.store.fault.store(4, Ordering::Relaxed);
    test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
    assert_eq!(test.store.counts(), (1, 2));
    let value = test.public().await;
    assert_eq!(value["owners"], json!([owner(0xb1), owner(0xc0)]));
    assert_eq!(
        value["retired_owners"],
        json!([historical(0), historical(1), owner(0xb0)])
    );
    assert_eq!(value["state"], "claimed");
    assert!(test.raw().await.starts_with(MAGIC));
}

#[tokio::test]
async fn retirement_keeps_the_negative_owner_set_sorted_at_every_insertion_position() {
    for target in [historical(0), historical(3), historical(8)] {
        let test = TestCodecFixture::new(4, 0).await;
        let mut expected = test.legacy.clone();
        let mut retired = vec![historical(2), historical(4), historical(6)];
        expected["retired_owners"] = json!(retired);
        test.install_json(&expected).await;
        test.uploads.retire_owner(&test.key, target).await.unwrap();
        assert_eq!(test.store.counts(), (1, 1));
        retired.push(target);
        retired.sort();
        expected["retired_owners"] = json!(retired);
        assert_eq!(test.public().await, expected);
    }
}

#[tokio::test]
async fn competing_retirement_recomputes_the_insertion_position_from_observed_bytes() {
    let test = TestCodecFixture::new(4, 0).await;
    let mut expected = test.legacy.clone();
    expected["retired_owners"] = json!([historical(2), historical(4), historical(6)]);
    test.install_json(&expected).await;
    test.store.fault.store(5, Ordering::Relaxed);
    test.uploads.retire_owner(&test.key, historical(5)).await.unwrap();
    assert_eq!(test.store.fault.load(Ordering::Relaxed), 0);
    assert_eq!(test.store.counts(), (1, 2));
    expected["retired_owners"] = json!([
        historical(1),
        historical(2),
        historical(4),
        historical(5),
        historical(6)
    ]);
    assert_eq!(test.public().await, expected);
    assert!(test.raw().await.starts_with(MAGIC));
}

#[tokio::test]
async fn malformed_binary_header_counts_length_and_identity_sets_are_rejected() {
    let test = TestCodecFixture::new(4, 2).await;
    test.uploads.retire_owner(&test.key, owner(0xb0)).await.unwrap();
    let good = test.raw().await;
    let mut invalid = vec![
        good[..7].to_vec(),
        good[..HEADER_BYTES - 1].to_vec(),
        good[..good.len() - 1].to_vec(),
    ];
    for (offset, value) in [(7, 7), (8, 6), (8, 2), (8, 5), (10, 4), (11, 2)] {
        let mut bytes = good.clone();
        bytes[offset] = value;
        invalid.push(bytes);
    }
    for (offset, value) in [(48, 257_u16), (50, 4097_u16), (50, 0_u16)] {
        let mut bytes = good.clone();
        bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
        invalid.push(bytes);
    }
    let mut trailing = good.clone();
    trailing.push(0);
    invalid.push(trailing);
    let mut zero_length = good.clone();
    zero_length[12..20].fill(0);
    invalid.push(zero_length);
    let mut zero_part = good.clone();
    zero_part[20..24].fill(0);
    invalid.push(zero_part);
    let retired = HEADER_BYTES + 16;
    let mut duplicate = good.clone();
    duplicate.copy_within(retired..retired + 16, retired + 16);
    invalid.push(duplicate);
    let mut descending = good.clone();
    descending[retired..retired + 16].copy_from_slice(&good[retired + 16..retired + 32]);
    descending[retired + 16..retired + 32].copy_from_slice(&good[retired..retired + 16]);
    invalid.push(descending);
    let mut overlap = good.clone();
    overlap.copy_within(retired..retired + 16, HEADER_BYTES);
    invalid.push(overlap);
    for bytes in invalid {
        test.install(&bytes).await;
        assert!(matches!(
            test.restart().metadata(&test.key).await,
            Err(UploadError::Integrity)
        ));
    }
}
