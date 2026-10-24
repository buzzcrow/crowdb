// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/publication_payload.rs"]
mod common;
use common::{input, response, TestPublicationHttp};
use crowdb_access_dataset::{FieldLocator, OperationId, SnapshotId, UploadState};
use md5::{Digest, Md5};
use reqwest::Client;
use serde_json::json;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn repeated_payload_fields_use_two_metadata_reads_and_one_claim_per_unique_upload() {
    for (fields, unique) in [(1, 1), (40, 1), (40, 3)] {
        let test = TestPublicationHttp::start().await;
        let client = Client::new();
        let mut uploads = Vec::new();
        for marker in 1..=unique {
            uploads.push(test.upload(marker, true).await);
        }
        let input = input(&uploads, fields, 32);
        let before = test.store.upload_reads.load(Ordering::Relaxed);
        let claims = test.store.claim_attempts.load(Ordering::Relaxed);
        let complete = test.store.complete_attempts.load(Ordering::Relaxed);
        let url = format!("{}/snapshots/publish", test.root);
        let published = response(client.post(&url).json(&input), 200).await;
        assert_eq!(
            test.store.upload_reads.load(Ordering::Relaxed) - before,
            2 * u64::from(unique)
        );
        assert_eq!(
            test.store.claim_attempts.load(Ordering::Relaxed) - claims,
            u64::from(unique)
        );
        assert_eq!(test.store.complete_attempts.load(Ordering::Relaxed) - complete, 1);
        println!(
            "v1 payload publication fields={fields} uploads={unique}: upload GET {} -> {}, claim CAS={unique}",
            u64::from(fields) + u64::from(unique), 2 * u64::from(unique)
        );
        let snapshot =
            SnapshotId::from_bytes(&hex::decode(published["id"].as_str().unwrap()).unwrap()).unwrap();
        let manifest = test
            .authority
            .get_manifest(&test.identity, snapshot)
            .await
            .unwrap();
        for (index, sample) in manifest.samples.iter().enumerate() {
            let marker = u8::try_from(index % usize::from(unique)).unwrap() + 1;
            let data = vec![marker; usize::from(marker) + 4];
            let FieldLocator::Chunk { length, md5, .. } = sample.fields[0].value else {
                panic!("payload locator missing")
            };
            assert_eq!(length, data.len() as u64);
            assert_eq!(md5.as_slice(), Md5::digest(&data).as_slice());
        }
        let before = test.store.upload_reads.load(Ordering::Relaxed);
        let claims = test.store.claim_attempts.load(Ordering::Relaxed);
        assert_eq!(response(client.post(&url).json(&input), 200).await, published);
        assert_eq!(
            test.store.upload_reads.load(Ordering::Relaxed) - before,
            2 * u64::from(unique)
        );
        assert_eq!(test.store.claim_attempts.load(Ordering::Relaxed), claims);
        drop(client);
        test.stop().await;
    }
}

#[tokio::test]
async fn malformed_repeated_fields_are_rejected_before_metadata_or_ownership() {
    let test = TestPublicationHttp::start().await;
    let client = Client::new();
    let upload = test.upload(1, true).await;
    let mut invalid = input(std::slice::from_ref(&upload), 2, 32);
    invalid["manifest"]["samples"][1]["fields"]["value"]["upload"] = json!("not-an-upload");
    let before = test.store.mutations();
    let reads = test.store.upload_reads.load(Ordering::Relaxed);
    response(
        client
            .post(format!("{}/snapshots/publish", test.root))
            .json(&invalid),
        400,
    )
    .await;
    let duplicate = format!(
        r#"{{"operation":"{}","parent":null,"manifest":{{"schema":[{{"name":"value","required":true}}],"samples":[{{"id":"AA==","fields":{{"value":{{"kind":"payload","upload":"{upload}"}},"value":{{"kind":"payload","upload":"{upload}"}}}}}}]}}}}"#,
        "20".repeat(16)
    );
    response(
        client
            .post(format!("{}/snapshots/publish", test.root))
            .header("content-type", "application/json")
            .body(duplicate),
        400,
    )
    .await;
    assert_eq!(test.store.mutations(), before);
    assert_eq!(test.store.upload_reads.load(Ordering::Relaxed), reads);
    assert_eq!(test.store.claim_attempts.load(Ordering::Relaxed), 0);
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn incomplete_second_upload_is_not_hidden_by_repeated_valid_fields() {
    let test = TestPublicationHttp::start().await;
    let client = Client::new();
    let valid = test.upload(1, true).await;
    let incomplete = test.upload(2, false).await;
    let mut input = input(&[valid], 4, 32);
    input["manifest"]["samples"][3]["fields"]["value"]["upload"] = incomplete.into();
    let before = test.store.mutations();
    let reads = test.store.upload_reads.load(Ordering::Relaxed);
    response(
        client
            .post(format!("{}/snapshots/publish", test.root))
            .json(&input),
        409,
    )
    .await;
    assert_eq!(test.store.upload_reads.load(Ordering::Relaxed) - reads, 2);
    assert_eq!(test.store.mutations(), before);
    assert_eq!(test.store.claim_attempts.load(Ordering::Relaxed), 0);
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn later_request_rechecks_aborted_upload_instead_of_reusing_cached_metadata() {
    let test = TestPublicationHttp::start().await;
    let client = Client::new();
    let upload = test.upload(1, true).await;
    let first = input(std::slice::from_ref(&upload), 8, 32);
    response(
        client
            .post(format!("{}/snapshots/publish", test.root))
            .json(&first),
        200,
    )
    .await;
    let location = test.location(&upload);
    test.uploads
        .retire_owner(&location, OperationId::from_bytes(&[32; 16]).unwrap())
        .await
        .unwrap();
    test.uploads.abort(&location).await.unwrap();
    let mut next = input(&[upload], 8, 33);
    next["parent"] = hex::encode([32; 16]).into();
    let before = test.store.mutations();
    let reads = test.store.upload_reads.load(Ordering::Relaxed);
    response(
        client
            .post(format!("{}/snapshots/publish", test.root))
            .json(&next),
        409,
    )
    .await;
    assert_eq!(test.store.upload_reads.load(Ordering::Relaxed) - reads, 1);
    assert_eq!(test.store.mutations(), before);
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn claim_cas_still_rejects_invalidation_after_resolution() {
    let test = TestPublicationHttp::start().await;
    let client = Client::new();
    let upload = test.upload(1, true).await;
    let input = input(&[upload], 8, 32);
    let reads = test.store.upload_reads.load(Ordering::Relaxed);
    test.store.abort_at_claim.store(true, Ordering::Relaxed);
    response(
        client
            .post(format!("{}/snapshots/publish", test.root))
            .json(&input),
        409,
    )
    .await;
    assert_eq!(test.store.upload_reads.load(Ordering::Relaxed) - reads, 2);
    assert_eq!(test.store.claim_attempts.load(Ordering::Relaxed), 1);
    assert_eq!(test.store.complete_attempts.load(Ordering::Relaxed), 0);
    assert!(test.authority.latest(&test.identity).await.unwrap().is_none());
    assert!(test
        .authority
        .get_snapshot(&test.identity, SnapshotId::from_bytes(&[32; 16]).unwrap())
        .await
        .is_err());
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn metadata_and_claim_unknown_outcomes_stop_without_confirmation_or_completion() {
    for claim in [false, true] {
        let test = TestPublicationHttp::start().await;
        let client = Client::new();
        let upload = test.upload(1, true).await;
        let input = input(std::slice::from_ref(&upload), 8, 32);
        if claim {
            test.store.lose_claim_reply.store(true, Ordering::Relaxed);
        } else {
            test.store.fail_metadata.store(true, Ordering::Relaxed);
        }
        let reads = test.store.upload_reads.load(Ordering::Relaxed);
        let mutations = test.store.mutations();
        response(
            client
                .post(format!("{}/snapshots/publish", test.root))
                .json(&input),
            503,
        )
        .await;
        assert!(test.store.unknown.load(Ordering::Relaxed));
        assert_eq!(test.store.operations_after_unknown.load(Ordering::Relaxed), 0);
        assert_eq!(
            test.store.upload_reads.load(Ordering::Relaxed) - reads,
            if claim { 2 } else { 1 }
        );
        assert_eq!(test.store.complete_attempts.load(Ordering::Relaxed), 0);
        test.store.unknown.store(false, Ordering::Relaxed);
        if claim {
            assert_eq!(
                test.uploads
                    .metadata(&test.location(&upload))
                    .await
                    .unwrap()
                    .state,
                UploadState::Claimed
            );
            assert!(test.authority.latest(&test.identity).await.unwrap().is_none());
        } else {
            assert_eq!(test.store.mutations(), mutations);
        }
        drop(client);
        test.stop().await;
    }
}
