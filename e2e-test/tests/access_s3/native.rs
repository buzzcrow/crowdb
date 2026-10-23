// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod parity;

use crate::common::native_s3::{context, payload, TestNativeS3};
use crate::common::native_s3::{multipart, transport};
use crowdb_access_s3::S3ErrorCode;
use crowdb_sdk_native::s3::{
    ByteRange, CopyOptions, EtagSelection, GetOptions, ListOptions, LocalityOptions, ReadPreconditions,
};
use crowdb_sdk_native::Error;

const SIZES: &[usize] = &[0, 7, 513, 65_502, 65_503, (1 << 20) + 13, 2 << 20, (4 << 20) + 31];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned protected storage services without Access Server"]
async fn complete_corpus_survives_storage_restart_without_access_or_credentials() {
    let addresses = crate::common::management::addresses(12);
    let verify = std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("verify");
    if !verify {
        write_corpus(&addresses).await;
    }
    let fixture = TestNativeS3::connect(&addresses, b"native-internal").await;
    for &length in SIZES {
        let key = format!("size-{length:08}");
        assert_eq!(
            fixture.read(key.as_bytes(), GetOptions::default()).await.1,
            payload(length)
        );
    }
    assert_eq!(
        fixture.read(b"framed-small", GetOptions::default()).await.1,
        payload(513)
    );
    assert_eq!(
        fixture.read(b"framed-large", GetOptions::default()).await.1,
        payload((2 << 20) + 17)
    );
    multipart::verify(&fixture).await;
    assert_topology_and_range(&fixture).await;
    let topology = fixture.runtime.topology().unwrap();
    transport::assert_selected(&fixture.runtime);
    let resources = fixture.runtime.resources().clone();
    assert_eq!(resources.status().compatibility_copy_bytes, 0);
    assert_eq!(resources.status().held_bytes, 0);
    assert_eq!(resources.status().requests, 0);
    fixture.assert_pressure_and_shutdown().await;
    let status = resources.status();
    assert!(status.held_bytes_high_water <= 8 << 20);
    assert!(status.producer_bytes_high_water <= 8 << 20);
    let execution = std::env::var("CROWDB_NATIVE_S3_EXECUTION").unwrap_or_else(|_| "native".into());
    let phase = if verify { "verify" } else { "write" };
    let evidence = serde_json::json!({
        "execution": execution, "phase": phase, "transport": if transport::enabled() { "rdma" } else { "tcp" }, "access_server": false,
        "credential_lookup": false, "sizes": SIZES, "errors": 0,
        "held_bytes_high_water": status.held_bytes_high_water,
        "producer_bytes_high_water": status.producer_bytes_high_water,
        "compatibility_copy_bytes": status.compatibility_copy_bytes,
        "catalog_generation": topology.catalog().generation(),
        "metadata_transport": serde_json::from_slice::<serde_json::Value>(&topology.metadata_transport_json).unwrap(),
        "data_transport": topology.data_transport_json.as_ref().map(|json| serde_json::from_slice::<serde_json::Value>(json).unwrap()),
    });
    let artifacts = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    std::fs::write(
        artifacts.join(format!("native-s3-{execution}-{phase}.json")),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
}

async fn write_corpus(addresses: &[String]) {
    let fixture = TestNativeS3::connect(addresses, b"native-internal").await;
    fixture
        .client
        .create_bucket(context(), fixture.bucket)
        .await
        .unwrap();
    for &length in SIZES {
        let key = format!("size-{length:08}");
        let metadata = fixture.put(key.as_bytes(), payload(length)).await;
        assert_eq!(metadata.logical_length, length as u64);
    }
    fixture.put_framed(b"framed-small", 513).await;
    fixture.put_framed(b"framed-large", (2 << 20) + 17).await;
    Box::pin(assert_conditions_copy_and_listing(&fixture)).await;
    Box::pin(multipart::exercise(&fixture)).await;
    let resources = fixture.runtime.resources().clone();
    assert!(resources.status().held_bytes_high_water <= 8 << 20);
    assert!(resources.status().producer_bytes_high_water <= 8 << 20);
    assert_eq!(resources.status().compatibility_copy_bytes, 0);
    transport::assert_selected(&fixture.runtime);
    let evidence = serde_json::json!({
        "held_bytes_high_water": resources.status().held_bytes_high_water,
        "producer_bytes_high_water": resources.status().producer_bytes_high_water,
        "compatibility_copy_bytes": resources.status().compatibility_copy_bytes,
        "data_transport": transport::data_status(&fixture.runtime),
    });
    let artifacts = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let execution = std::env::var("CROWDB_NATIVE_S3_EXECUTION").unwrap_or_else(|_| "native".into());
    std::fs::write(
        artifacts.join(format!("producer-{execution}.json")),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
    // Open small strips retain their input views for mirror repair. Closing
    // the owned pool seals those strips and must release every such owner.
    fixture.runtime.shutdown().await.unwrap();
    assert_eq!(resources.status().held_bytes, 0);
    assert_eq!(resources.status().producer_bytes, 0);
}

async fn assert_conditions_copy_and_listing(fixture: &TestNativeS3) {
    let key = b"size-00000513";
    let selected = fixture
        .client
        .resolve_object(context(), fixture.bucket, key)
        .await
        .unwrap();
    let conditions = ReadPreconditions {
        if_match: Some(EtagSelection::Values(vec!["wrong".into()])),
        ..ReadPreconditions::default()
    };
    assert!(matches!(
        fixture
            .client
            .get_object(
                context(),
                fixture.bucket,
                key,
                &GetOptions {
                    conditions,
                    ..GetOptions::default()
                }
            )
            .await,
        Err(Error::S3(S3ErrorCode::PreconditionFailed))
    ));
    fixture.put(b"temporary", b"original".to_vec()).await;
    let source = fixture
        .client
        .resolve_object(context(), fixture.bucket, b"temporary")
        .await
        .unwrap();
    fixture.put(b"temporary", b"replacement".to_vec()).await;
    fixture
        .client
        .copy_resolved(
            context(),
            &source,
            fixture.bucket,
            b"captured-copy",
            CopyOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture.read(b"captured-copy", GetOptions::default()).await.1,
        b"original"
    );
    assert_eq!(selected.metadata().logical_length, 513);
    let mut options = ListOptions {
        prefix: b"size-".to_vec(),
        limit: 2,
        ..ListOptions::default()
    };
    let mut listed = Vec::new();
    loop {
        let page = fixture
            .client
            .list_objects(context(), fixture.bucket, &options)
            .await
            .unwrap();
        listed.extend(page.objects.into_iter().map(|object| object.key));
        options.continuation = page.next_continuation;
        if options.continuation.is_none() {
            break;
        }
    }
    assert_eq!(listed.len(), SIZES.len());
    assert!(listed.windows(2).all(|pair| pair[0] < pair[1]));
    let mut deletion = fixture
        .client
        .delete_objects(
            context(),
            fixture.bucket,
            vec![b"temporary".to_vec(), b"missing".to_vec()],
        )
        .await
        .unwrap();
    while let Some(result) = deletion.next().await.unwrap() {
        assert!(result.1.is_ok());
    }
}

async fn assert_topology_and_range(fixture: &TestNativeS3) {
    let key = b"framed-large";
    let selected = fixture
        .client
        .resolve_object(context(), fixture.bucket, key)
        .await
        .unwrap();
    let range = ByteRange {
        start: 65_499,
        end: 65_521,
    };
    let view = fixture
        .client
        .object_locality(
            context(),
            &selected,
            &LocalityOptions {
                range: Some(range),
                limit: 1,
                ..LocalityOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(view.object, *selected.metadata());
    assert!(view.extents.iter().all(|extent| !extent.strips.is_empty()));
    let fresh = fixture.runtime.refresh_topology(context()).await.unwrap();
    assert!(fresh.services.iter().all(|service| !service.instances.is_empty()));
    let result = fixture
        .read(
            key,
            GetOptions {
                range: Some(range),
                ..GetOptions::default()
            },
        )
        .await
        .1;
    assert_eq!(result, payload((2 << 20) + 17)[65_499..65_521]);
}
