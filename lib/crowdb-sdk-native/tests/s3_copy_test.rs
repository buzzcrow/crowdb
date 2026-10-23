// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/write_fixture.rs"]
mod fixture;
#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/write_storage.rs"]
mod write_storage;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crowdb_access_s3::S3ErrorCode;
use crowdb_sdk_native::s3::{CopyMetadata, CopyOptions, EtagSelection, ReadPreconditions, UserMetadata};
use crowdb_sdk_native::Error;
use fixture::{context, fixture, options, read};

#[tokio::test]
async fn copy_keeps_selected_generation_and_replaces_metadata_only_when_requested() {
    let (runtime, client, _) = fixture(None).await;
    let mut source = options(8);
    source.object.content_type = "application/source".into();
    source.object.user_metadata =
        UserMetadata::from_values(BTreeMap::from([("owner".into(), "original".into())])).unwrap();
    let original = client
        .put_vec(context(10), b"bucket", b"source", b"original".to_vec(), source)
        .await
        .unwrap();
    let selected = client
        .resolve_object(context(11), b"bucket", b"source")
        .await
        .unwrap();
    client
        .put_vec(context(12), b"bucket", b"source", b"new".to_vec(), options(3))
        .await
        .unwrap();
    let copied = client
        .copy_resolved(context(13), &selected, b"bucket", b"old", CopyOptions::default())
        .await
        .unwrap();
    assert_eq!(read(&client, b"old").await, b"original");
    assert_eq!(copied.content_type, original.content_type);
    assert_eq!(copied.user_metadata, original.user_metadata);
    assert_eq!(copied.checksum, original.checksum);
    assert_eq!(
        copied.revision,
        client
            .resolve_object(context(14), b"bucket", b"old")
            .await
            .unwrap()
            .metadata()
            .revision
    );
    assert_eq!(
        client
            .copy_resolved(
                context(15),
                &selected,
                b"bucket",
                b"source",
                CopyOptions::default()
            )
            .await,
        Err(Error::S3(S3ErrorCode::InvalidRequest))
    );
    let replacement = CopyOptions {
        metadata: Some(CopyMetadata {
            content_type: "application/replaced".into(),
            user_metadata: UserMetadata::default(),
        }),
        ..CopyOptions::default()
    };
    let copied = client
        .copy_resolved(context(16), &selected, b"bucket", b"source", replacement)
        .await
        .unwrap();
    assert_eq!(copied.content_type, "application/replaced");
    assert!(copied.user_metadata.values().is_empty());
    assert_eq!(read(&client, b"source").await, b"original");
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn copy_conditions_and_corrupt_source_stop_before_destination_publication() {
    let (runtime, client, backend) = fixture(None).await;
    client
        .put_vec(
            context(10),
            b"bucket",
            b"source",
            b"original".to_vec(),
            options(8),
        )
        .await
        .unwrap();
    let before = backend.writes.load(Ordering::Acquire);
    let conditions = CopyOptions {
        source_conditions: ReadPreconditions {
            if_match: Some(EtagSelection::Values(vec!["wrong".into()])),
            ..ReadPreconditions::default()
        },
        ..CopyOptions::default()
    };
    assert_eq!(
        client
            .copy_object(
                context(11),
                b"bucket",
                b"source",
                b"bucket",
                b"condition",
                conditions
            )
            .await,
        Err(Error::S3(S3ErrorCode::PreconditionFailed))
    );
    assert_eq!(backend.writes.load(Ordering::Acquire), before);
    let entry = backend.data.front().unwrap();
    let key = *entry.key();
    let mut corrupt = entry.value().to_vec();
    corrupt[14] ^= 1;
    drop(entry);
    backend.data.insert(key, corrupt.into());
    assert_eq!(
        client
            .copy_object(
                context(12),
                b"bucket",
                b"source",
                b"bucket",
                b"corrupt",
                CopyOptions::default()
            )
            .await,
        Err(Error::S3(S3ErrorCode::ServiceUnavailable))
    );
    assert!(matches!(
        client.resolve_object(context(13), b"bucket", b"corrupt").await,
        Err(Error::S3(S3ErrorCode::NoSuchKey))
    ));
    assert_eq!(runtime.resources().status().requests, 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn copied_data_remains_recoverable_after_a_lost_publication_reply() {
    let loss = Arc::new(AtomicBool::new(false));
    let (runtime, client, backend) = fixture(Some(loss.clone())).await;
    client
        .put_vec(
            context(10),
            b"bucket",
            b"source",
            b"original".to_vec(),
            options(8),
        )
        .await
        .unwrap();
    loss.store(true, Ordering::Release);
    assert_eq!(
        client
            .copy_object(
                context(51),
                b"bucket",
                b"source",
                b"bucket",
                b"uncertain",
                CopyOptions::default()
            )
            .await,
        Err(Error::UncertainMutation {
            request_id: 51,
            reason: crowdb_sdk_native::error::MutationInterruption::Transport
        })
    );
    loss.store(false, Ordering::Release);
    assert_eq!(read(&client, b"uncertain").await, b"original");
    assert_eq!(backend.deletes.load(Ordering::Acquire), 0);
    runtime.shutdown().await.unwrap();
}
