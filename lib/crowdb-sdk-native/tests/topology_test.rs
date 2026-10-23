// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/storage.rs"]
mod storage;
#[path = "common/topology.rs"]
mod topology;

use std::num::NonZeroU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_sdk_native::error::{TopologyFailureKind, TopologySource};
use crowdb_sdk_native::{Error, OperationContext};

fn context(id: u64) -> OperationContext {
    OperationContext::new(
        NonZeroU64::new(id).unwrap(),
        Instant::now() + Duration::from_secs(1),
    )
}

#[tokio::test]
async fn retained_catalog_views_do_not_regress_and_independent_refresh_age_is_unknown() {
    let source = Arc::new(topology::TestCatalogSource {
        generation: 1.into(),
        unavailable: false.into(),
    });
    let metadata = metadata::client_with_source(source.clone()).await;
    let runtime = storage::runtime_with_metadata(metadata.clone());
    let old = runtime.topology().unwrap();
    assert_eq!(old.catalog().generation(), 1);
    assert!(old.catalog_age().is_none());
    source.generation.store(2, Ordering::Release);
    metadata.refresh_catalog().await.unwrap();
    let observed = runtime.topology().unwrap();
    assert_eq!(observed.catalog().generation(), 2);
    assert_eq!(observed.catalog().route(b"key").unwrap().owner.instance_id, 2);
    assert!(observed.catalog_age().is_none());
    let refreshed = runtime.refresh_catalog(context(1)).await.unwrap();
    assert!(refreshed.catalog_age().is_some());
    source.generation.store(1, Ordering::Release);
    assert!(matches!(
        runtime.refresh_catalog(context(2)).await,
        Err(Error::Topology {
            origin: TopologySource::Catalog,
            kind: TopologyFailureKind::Invalid,
            ..
        })
    ));
    assert_eq!(runtime.topology().unwrap().catalog().generation(), 2);
    assert_eq!(old.catalog().generation(), 1);
    assert_eq!(observed.catalog().generation(), 2);
    source.unavailable.store(true, Ordering::Release);
    assert!(matches!(
        runtime.refresh_catalog(context(3)).await,
        Err(Error::Topology {
            origin: TopologySource::Catalog,
            kind: TopologyFailureKind::Unavailable,
            ..
        })
    ));
    runtime.shutdown().await.unwrap();
    assert_eq!(old.catalog().route(b"key").unwrap().owner.instance_id, 1);
    assert_eq!(refreshed.catalog().generation(), 2);
}

#[tokio::test]
async fn retained_view_cannot_pin_s3_operations_to_a_moved_owner() {
    use crowdb_access_s3::metadata::TenantId;
    use crowdb_sdk_native::s3::S3Client;

    let source = Arc::new(topology::TestCatalogSource {
        generation: 1.into(),
        unavailable: false.into(),
    });
    let route = Arc::new(metadata::TestRouteChange {
        generation: 1.into(),
        ..Default::default()
    });
    let metadata = metadata::client_with_route(source.clone(), route.clone()).await;
    let runtime = storage::runtime_with_metadata(metadata);
    let client = S3Client::new(runtime.clone(), TenantId::new(b"tenant".to_vec()).unwrap()).unwrap();
    client.create_bucket(context(10), b"bucket").await.unwrap();
    client.head_bucket(context(11), b"bucket").await.unwrap();
    let old = runtime.topology().unwrap();
    source.generation.store(2, Ordering::Release);
    route.generation.store(2, Ordering::Release);
    client.head_bucket(context(12), b"bucket").await.unwrap();
    assert_eq!(route.rejected.load(Ordering::Acquire), 1);
    assert_eq!(route.accepted.load(Ordering::Acquire), 2);
    let current = runtime.topology().unwrap();
    assert_eq!(current.catalog().generation(), 2);
    assert_eq!(
        current.catalog().route(b"key").unwrap().owner.rpc_endpoint,
        "test-owner-2"
    );
    assert_eq!(old.catalog().generation(), 1);
    assert_eq!(
        old.catalog().route(b"key").unwrap().owner.rpc_endpoint,
        "test-owner-1"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn dataset_keeps_snapshot_selection_when_stale_owner_refreshes() {
    use crowdb_access_dataset::{
        DatasetIdentity, FieldDefinition, FieldLocator, FieldRecord, ManifestRecord, NamespacePath,
        SampleRecord, SchemaRecord, SnapshotId,
    };
    use crowdb_sdk_native::dataset::{BatchRequest, DatasetClient};
    use md5::{Digest, Md5};

    let source = Arc::new(topology::TestCatalogSource {
        generation: 1.into(),
        unavailable: false.into(),
    });
    let route = Arc::new(metadata::TestRouteChange {
        generation: 1.into(),
        ..Default::default()
    });
    let metadata = metadata::client_with_route(source.clone(), route.clone()).await;
    let runtime = storage::runtime_with_metadata(metadata);
    let client = DatasetClient::new(
        &runtime,
        DatasetIdentity::new(NamespacePath::root(), "routed").unwrap(),
    )
    .unwrap();
    client.create(context(20)).await.unwrap();
    let snapshot = client
        .control(context(21), |s| async move {
            let manifest = ManifestRecord {
                version: 1,
                snapshot: SnapshotId::random(),
                schema: SchemaRecord {
                    version: 1,
                    fields: vec![FieldDefinition {
                        name: "v".into(),
                        required: true,
                    }],
                },
                samples: vec![SampleRecord {
                    sample_id: b"id".to_vec(),
                    fields: vec![FieldRecord {
                        name: "v".into(),
                        value: FieldLocator::Inline {
                            value: b"selected".to_vec(),
                            md5: Md5::digest(b"selected").into(),
                        },
                    }],
                }],
            };
            Ok(s.authority
                .publish_snapshot(&s.identity, None, bincode::serialize(&manifest).unwrap())
                .await?)
        })
        .await
        .unwrap();
    let old = runtime.topology().unwrap();
    source.generation.store(2, Ordering::Release);
    route.generation.store(2, Ordering::Release);
    let mut read = client
        .read_batch(
            context(22),
            BatchRequest {
                snapshot,
                sample_ids: vec![b"id".to_vec()],
                fields: vec!["v".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(read.header.snapshot, snapshot);
    assert_eq!(
        read.body.next().await.unwrap().unwrap().buffer.as_slice(),
        b"selected"
    );
    assert!(read.body.next().await.unwrap().is_none());
    assert!(route.rejected.load(Ordering::Acquire) >= 1);
    assert_eq!(route.accepted.load(Ordering::Acquire), 2);
    assert_eq!(runtime.topology().unwrap().catalog().generation(), 2);
    assert_eq!(old.catalog().generation(), 1);
    runtime.shutdown().await.unwrap();
}
