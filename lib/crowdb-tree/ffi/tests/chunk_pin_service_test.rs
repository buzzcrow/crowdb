// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{chunk::pin_service::ChunkPinReleaseService, CtError, TreeRuntime};
#[tokio::test]
async fn pin_service_has_no_native_runtime_owner_and_joins_its_host_task() {
    let runtime = TreeRuntime::new(Default::default()).unwrap();
    let service = ChunkPinReleaseService::new(&runtime, 64).unwrap();
    drop(runtime);
    let before = service.resource_usage().unwrap();
    assert_eq!(before.limit, 64);
    assert_eq!(before.used, 0);
    assert_eq!(before.waiters, 0);
    assert_eq!(before.denied_host, 0);
    assert_eq!(service.resource_usage().unwrap(), before);
    service.shutdown().await;
    assert_eq!(service.resource_usage().unwrap(), before);
    service.shutdown().await;
}
#[tokio::test]
async fn invalid_pin_capacity_does_not_consume_host_queue_context() {
    let runtime = TreeRuntime::new(Default::default()).unwrap();
    assert!(matches!(
        ChunkPinReleaseService::new(&runtime, 0),
        Err(CtError::InvalidArgument)
    ));
    let valid = ChunkPinReleaseService::new(&runtime, 1).unwrap();
    valid.shutdown().await;
}
