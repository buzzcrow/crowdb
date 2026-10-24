// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/upload_owner_cost.rs"]
mod cost;
#[path = "common/upload.rs"]
mod upload;

use cost::{add_owner, canonical, owner, retire, TestOwnerCost, TestOwnerFixture};
use crowdb_access_dataset::{UploadError, UploadState};
use serde_json::Value;

const PROFILES: [u32; 4] = [1, 64, 256, 4096];

#[tokio::test]
async fn retiring_an_owner_counts_the_whole_record_and_preserves_restart_fences_at_each_size() {
    let mut sizes = Vec::new();
    for count in PROFILES {
        let test = TestOwnerFixture::new(count - 1, 0, false).await;
        let before = test.raw().await;
        let expected = retire(&before, test.victim);
        test.uploads.retire_owner(&test.key, test.victim).await.unwrap();
        assert_eq!(test.raw().await, expected);
        assert_eq!(
            test.store.cost(),
            TestOwnerCost {
                gets: 1,
                exchanges: 1,
                read_bytes: before.len() as u64,
                observation_bytes: 0,
                expected_bytes: before.len() as u64,
                value_bytes: expected.len() as u64,
            }
        );
        println!(
            "retired_owners={count} normal={:?} final_record_bytes={}",
            test.store.cost(),
            expected.len()
        );
        sizes.push(expected.len());
        verify_fences(&test, count).await;
    }
    assert_eq!(sizes, [309, 4016, 15_499, 245_784]);
}

#[tokio::test]
async fn retirement_recomputes_from_four_real_competing_claims_without_gets_or_lost_owners() {
    for count in PROFILES {
        let test = TestOwnerFixture::new(count - 1, 4, false).await;
        let initial = test.raw().await;
        let mut current = initial.clone();
        let mut expected_bytes = 0;
        let mut value_bytes = 0;
        for attempt in 0..=4 {
            expected_bytes += current.len() as u64;
            let proposed = retire(&current, test.victim);
            value_bytes += proposed.len() as u64;
            current = if attempt == 4 {
                proposed
            } else {
                add_owner(&current, owner(0xc0, attempt))
            };
        }
        test.uploads.retire_owner(&test.key, test.victim).await.unwrap();
        assert_eq!(test.raw().await, current);
        assert_eq!(
            test.store.cost(),
            TestOwnerCost {
                gets: 1,
                exchanges: 5,
                read_bytes: initial.len() as u64,
                observation_bytes: expected_bytes - initial.len() as u64,
                expected_bytes,
                value_bytes,
            }
        );
        println!(
            "retired_owners={count} four_retirement_conflicts={:?}",
            test.store.cost()
        );
        verify_fences(&test, count).await;
        let value: Value = serde_json::from_slice(&test.raw().await).unwrap();
        assert_eq!(value["owners"].as_array().unwrap().len(), 5);
    }
}

#[tokio::test]
async fn claims_recompute_from_observations_without_copying_stale_owner_sets_at_each_size() {
    for count in PROFILES {
        let test = TestOwnerFixture::new(count, 4, false).await;
        let initial = test.raw().await;
        let target = owner(0xd0, 0);
        if count == 4096 {
            assert!(matches!(
                test.uploads.claim(&test.key, target, 6).await,
                Err(UploadError::Capacity)
            ));
            assert_eq!(
                test.store.cost(),
                TestOwnerCost {
                    gets: 1,
                    read_bytes: initial.len() as u64,
                    ..TestOwnerCost::default()
                }
            );
            assert_eq!(test.raw().await, initial);
            continue;
        }
        let mut current = initial.clone();
        let mut expected_bytes = 0;
        let mut value_bytes = 0;
        for attempt in 0..=4 {
            expected_bytes += current.len() as u64;
            let proposed = add_owner(&current, target);
            value_bytes += proposed.len() as u64;
            current = if attempt == 4 {
                proposed
            } else {
                add_owner(&current, owner(0xc0, attempt))
            };
        }
        test.uploads.claim(&test.key, target, 6).await.unwrap();
        assert_eq!(test.raw().await, current);
        assert_eq!(
            test.store.cost(),
            TestOwnerCost {
                gets: 1,
                exchanges: 5,
                read_bytes: initial.len() as u64,
                observation_bytes: expected_bytes - initial.len() as u64,
                expected_bytes,
                value_bytes,
            }
        );
        println!(
            "retired_owners={count} four_claim_conflicts={:?}",
            test.store.cost()
        );
        let restarted = test.restart();
        let before = test.store.cost();
        assert!(matches!(
            restarted.claim(&test.key, owner(0xa0, count - 1), 7).await,
            Err(UploadError::Conflict)
        ));
        assert_eq!(
            test.store.cost().since(before),
            TestOwnerCost {
                gets: 1,
                read_bytes: current.len() as u64,
                ..TestOwnerCost::default()
            }
        );
        assert_eq!(
            restarted.read_part(&test.key, 0).await.unwrap(),
            b"data".as_slice()
        );
    }
}

#[tokio::test]
async fn lost_retirement_reply_stops_without_get_and_restart_uses_the_final_fence() {
    let test = TestOwnerFixture::new(4095, 0, true).await;
    let initial = test.raw().await;
    let expected = retire(&initial, test.victim);
    assert!(matches!(
        test.uploads.retire_owner(&test.key, test.victim).await,
        Err(UploadError::Store(_))
    ));
    assert_eq!(
        test.store.cost(),
        TestOwnerCost {
            gets: 1,
            exchanges: 1,
            read_bytes: initial.len() as u64,
            observation_bytes: 0,
            expected_bytes: initial.len() as u64,
            value_bytes: expected.len() as u64,
        }
    );
    assert_eq!(test.raw().await, expected);
    verify_fences(&test, 4096).await;
    let before = test.store.cost();
    let mut final_record: Value = serde_json::from_slice(&expected).unwrap();
    final_record["version"] = serde_json::json!(5);
    final_record["owners"] = serde_json::json!([]);
    final_record["state"] = serde_json::json!("complete");
    let final_record = canonical(final_record);
    test.restart().retire_owner(&test.key, test.live).await.unwrap();
    assert_eq!(
        test.store.cost().since(before),
        TestOwnerCost {
            gets: 1,
            exchanges: 1,
            read_bytes: expected.len() as u64,
            observation_bytes: 0,
            expected_bytes: expected.len() as u64,
            value_bytes: final_record.len() as u64,
        }
    );
    assert_eq!(test.raw().await, final_record);
}

async fn verify_fences(test: &TestOwnerFixture, retired: u32) {
    let restarted = test.restart();
    let bytes = test.raw().await;
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        value["retired_owners"].as_array().unwrap().len(),
        retired as usize
    );
    let mut identities = vec![test.victim];
    if retired > 1 {
        identities.extend([
            owner(0xa0, 0),
            owner(0xa0, (retired - 2) / 2),
            owner(0xa0, retired - 2),
        ]);
    }
    for identity in identities {
        let before = test.store.cost();
        assert!(matches!(
            restarted.claim(&test.key, identity, 7).await,
            Err(UploadError::Conflict)
        ));
        restarted.retire_owner(&test.key, identity).await.unwrap();
        assert_eq!(
            test.store.cost().since(before),
            TestOwnerCost {
                gets: 2,
                read_bytes: 2 * bytes.len() as u64,
                ..TestOwnerCost::default()
            }
        );
    }
    assert!(matches!(
        restarted.abort(&test.key).await,
        Err(UploadError::Conflict)
    ));
    assert_eq!(
        restarted.metadata(&test.key).await.unwrap().state,
        UploadState::Claimed
    );
    assert_eq!(
        restarted.read_part(&test.key, 0).await.unwrap(),
        b"data".as_slice()
    );
    assert_eq!(test.raw().await, bytes);
}
