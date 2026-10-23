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
