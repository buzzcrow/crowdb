// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/read_storage.rs"]
mod storage;

use crowdb_sdk_native::error::{TopologyFailureKind, TopologySource};
use crowdb_sdk_native::s3::{ByteRange, FragmentRole, GetOptions, LocalityOptions};
use crowdb_sdk_native::Error;
use std::sync::atomic::Ordering;
use std::time::Duration;
use storage::{context, TestFixture};

#[tokio::test]
async fn locality_is_generation_bound_bounded_and_keeps_layout_revisions_independent() {
    let fixture = TestFixture::new(8192).await;
    let mut record = fixture.record(0);
    let first: Vec<crowdb_protocol::chunkdb::rpc::Location> =
        bincode::deserialize(&record.data_reference).unwrap();
    let mut second = first[0].clone();
    second.logical_offset = 8;
    record.logical_length = 16;
    record.data_length = 16;
    let mut integrity = crowdb_access_s3::integrity::SinglePartIntegrity::default();
    integrity.update(&bytes::Bytes::from_static(b"originaloriginal"));
    (record.etag, record.checksum) = integrity.finish();
    record.data_reference = bincode::serialize(&vec![first[0].clone(), second]).unwrap();
    let object_revision = fixture.publish(&record).await;
    let selected = fixture
        .client
        .resolve_object(context(2), b"bucket", b"key")
        .await
        .unwrap();
    for options in [
        LocalityOptions {
            limit: 0,
            ..LocalityOptions::default()
        },
        LocalityOptions {
            range: Some(ByteRange { start: 0, end: 17 }),
            ..LocalityOptions::default()
        },
    ] {
        assert!(matches!(
            fixture
                .client
                .object_locality(context(20), &selected, &options)
                .await,
            Err(Error::S3(_))
        ));
    }
    assert_eq!(fixture.backend.queries.load(Ordering::Relaxed), 0);
    fixture.backend.revision.store(7, Ordering::Release);
    let options = LocalityOptions {
        range: Some(ByteRange { start: 2, end: 12 }),
        limit: 1,
        ..LocalityOptions::default()
    };
    let first = fixture
        .client
        .object_locality(context(3), &selected, &options)
        .await
        .unwrap();
    assert_eq!(first.object.revision, object_revision);
    assert_eq!(first.extents.len(), 1);
    assert_eq!(first.extents[0].logical, ByteRange { start: 2, end: 8 });
    assert_eq!(first.extents[0].layout.revision, 7);
    assert_eq!(
        first.extents[0].strips[0].fragments[0].role,
        FragmentRole::MirrorReplica(0)
    );
    assert!(first.extents[0].strips[0].fragments[0].disk.is_none());
    assert_eq!(first.next_index, Some(1));
    assert_eq!(first.topology.catalog().generation(), 1);
    fixture.publish(&fixture.record(1)).await;
    fixture.backend.revision.store(8, Ordering::Release);
    let next = fixture
        .client
        .object_locality(
            context(4),
            &selected,
            &LocalityOptions {
                start_index: first.next_index.unwrap(),
                ..options
            },
        )
        .await
        .unwrap();
    assert_eq!(next.object.revision, object_revision);
    assert_eq!(next.extents[0].logical, ByteRange { start: 8, end: 12 });
    assert_eq!(next.extents[0].layout.revision, 8);
    assert_eq!(first.extents[0].layout.revision, 7);
    assert_eq!(next.next_index, None);
    let before = fixture.backend.queries.load(Ordering::Relaxed);
    let together = fixture
        .client
        .object_locality(context(5), &selected, &LocalityOptions::default())
        .await
        .unwrap();
    assert_eq!(together.extents.len(), 2);
    assert!(std::sync::Arc::ptr_eq(
        &together.extents[0].layout,
        &together.extents[1].layout
    ));
    assert_eq!(fixture.backend.queries.load(Ordering::Relaxed), before + 1);
    assert_eq!(fixture.backend.reads.load(Ordering::Relaxed), 0);
    fixture.runtime.shutdown().await.unwrap();
    assert_eq!(first.extents[0].layout.revision, 7);
}

#[tokio::test]
async fn expired_views_remain_inspectable_and_cannot_force_a_deleted_layout_into_execution() {
    let fixture = TestFixture::new(4096).await;
    fixture.publish(&fixture.record(0)).await;
    let selected = fixture
        .client
        .resolve_object(context(2), b"bucket", b"key")
        .await
        .unwrap();
    fixture.backend.validity_ms.store(100, Ordering::Release);
    let old = fixture
        .client
        .object_locality(context(3), &selected, &LocalityOptions::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert!(matches!(
        old.check_layout_freshness(),
        Err(Error::Topology {
            origin: TopologySource::ChunkLayout,
            kind: TopologyFailureKind::Stale,
            ..
        })
    ));
    fixture.backend.deleted.store(true, Ordering::Release);
    assert!(matches!(
        fixture
            .client
            .object_locality(context(4), &selected, &LocalityOptions::default())
            .await,
        Err(Error::Topology {
            kind: TopologyFailureKind::Stale,
            ..
        })
    ));
    let mut read = fixture
        .client
        .get_resolved(context(5), &selected, &GetOptions::default())
        .unwrap();
    assert!(matches!(read.body.next().await, Err(Error::Read { .. })));
    assert_eq!(fixture.backend.reads.load(Ordering::Relaxed), 0);
    assert_eq!(
        old.extents[0].layout.state,
        crowdb_protocol::chunkdb::rpc::ChunkState::Sealed as i32
    );
}
