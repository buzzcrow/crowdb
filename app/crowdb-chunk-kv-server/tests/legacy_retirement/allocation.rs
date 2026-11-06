// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overflowing_physical_layout_is_corruption_before_drain_or_delete() {
    let metadata = metadata::TestMetadata::start().await;
    let owner = process(&metadata.client, 10).await;
    let old = open(metadata.client.clone(), 0, 1, &entry(7), owner.clone())
        .await
        .unwrap();
    old.store().publish(42, 0, 7, 1, manifest()).unwrap();
    old.store().publish(42, 1, 7, 2, &typed_root()).unwrap();
    expire(&metadata.client, &owner).await;
    let current = process(&metadata.client, 20).await;
    let physical = Arc::new(TestPhysical::new());
    physical.set_physical_layout(u32::MAX, u32::MAX);
    assert_eq!(
        collect(
            metadata.client.clone(),
            current,
            physical.clone(),
            physical.clone(),
            64,
        )
        .await,
        Err(CtError::Corruption)
    );
    assert_eq!(physical.drains.load(Ordering::Relaxed), 0);
    assert_eq!(physical.deletes.load(Ordering::Relaxed), 0);
    assert!(!physical.all_deleted());
    assert_eq!(
        old.store().load_legacy_migration(42, 1).unwrap().unwrap(),
        manifest()
    );
}
