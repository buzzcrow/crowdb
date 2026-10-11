// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn small_write_scales_out_on_queued_object_count() {
    let mut configured = policy();
    configured.max_pipelines = 2;
    configured.max_batch_objects = 1;
    configured.max_batch_bytes = 4 * KIB;
    configured.scale_out_queue_bytes = configured.memory_budget;
    configured.scale_out_queue_objects = 2;
    let stack = E2eStack::start(configured).await;
    let completed = concurrent_writes(&stack, 64, 4 * KIB).await;

    wait_for_metrics(&stack.client, || stack.client.small_write_metrics().scale_out > 0).await;
    assert_eq!(completed.len(), 64);
    assert_eq!(stack.client.small_write_metrics().failed, 0);
    stack.client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn small_write_carry_does_not_underflow_queue_pressure() {
    let mut configured = policy();
    configured.max_pipelines = 2;
    configured.max_batch_objects = configured.queue_capacity;
    configured.max_batch_bytes = 64 * KIB;
    configured.scale_out_queue_bytes = configured.memory_budget;
    configured.scale_out_queue_objects = configured.queue_capacity;
    configured.conversion_enabled = false;
    let stack = E2eStack::start(configured).await;

    let completed = concurrent_writes(&stack, 64, 40 * KIB).await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(completed.len(), 64);
    assert_eq!(stack.client.small_write_metrics().scale_out, 0);
    stack.client.shutdown_small_writes().await.unwrap();
}
