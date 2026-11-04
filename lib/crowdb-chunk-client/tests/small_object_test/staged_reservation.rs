// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test(start_paused = true)]
async fn lease_renewal_preserves_the_unpublished_first_strip() {
    let mut configured = policy();
    configured.conversion_enabled = true;
    configured.memory_budget = 64 * 1024 * 1024;
    configured.chunk_capacity = 2 * 1024 * 1024;
    configured.small_strip_prefetch_count = 2;
    let renewal = configured.writer_lease / 2;
    let (client, allocator, disk) = client(configured);
    let mut writer = client.prepare_small_write(16).await.unwrap();
    for _ in 0..1000 {
        if allocator.reserve_calls.load(Ordering::Relaxed) > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(allocator.reserve_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        allocator
            .state
            .lock()
            .unwrap()
            .chunks
            .values()
            .next()
            .unwrap()
            .capacity,
        0
    );
    tokio::time::sleep(renewal + Duration::from_millis(1)).await;
    for _ in 0..1000 {
        if allocator.snapshot().2 > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        allocator.snapshot().2 > 0,
        "the empty writer must renew before submission"
    );
    {
        let state = allocator.state.lock().unwrap();
        let published = state.chunks.values().next().unwrap();
        assert_eq!(published.capacity, 0);
        assert!(
            published.strips.is_empty(),
            "renewal must not publish an unwritten strip"
        );
    }
    writer
        .on_data(Bytes::from_static(b"0123456789abcdef"))
        .await
        .unwrap();
    let locations = writer.finish_durable().await.unwrap();
    assert_eq!(locations.len(), 1);
    assert_eq!(locations[0].offset, 0);
    for (_, _, image) in disk.writes.lock().unwrap().iter() {
        let frame = parse_frame(image, locations[0].chunk_id.unwrap()).unwrap();
        assert_eq!(frame.payload, b"0123456789abcdef");
    }
    assert_eq!(disk.calls(), 3);
    client.shutdown_small_writes().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_idle_prepared_replacement_still_writes_at_zero_after_renewal() {
    let mut configured = policy();
    configured.conversion_enabled = true;
    configured.memory_budget = 64 * 1024 * 1024;
    configured.chunk_capacity = 2 * 1024 * 1024;
    configured.small_strip_prefetch_count = 2;
    let renewal = configured.writer_lease / 2;
    let (client, allocator, _) = client(configured);
    let payload = Bytes::from(vec![0x5a; MAX_SMALL]);
    let mut writer = client.prepare_small_write(MAX_SMALL).await.unwrap();
    writer.on_data(payload.clone()).await.unwrap();
    let original = writer.finish_durable().await.unwrap()[0].chunk_id;
    for _ in 0..1000 {
        if allocator.snapshot().0 == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        allocator.snapshot().0,
        2,
        "a replacement must be prepared before renewal"
    );
    tokio::time::sleep(renewal + Duration::from_millis(1)).await;
    {
        let state = allocator.state.lock().unwrap();
        assert!(state.advances >= 2, "current and prepared chunks must both renew");
        let replacement = state.chunks.get(&(original.unwrap().high, 2)).unwrap();
        assert!(replacement.modify_ts >= 3, "the prepared replacement must renew");
        assert_eq!(replacement.capacity, 0);
        assert!(replacement.strips.is_empty());
    }
    for _ in 1..32 {
        let mut writer = client.prepare_small_write(MAX_SMALL).await.unwrap();
        writer.on_data(payload.clone()).await.unwrap();
        assert_eq!(writer.finish_durable().await.unwrap()[0].chunk_id, original);
    }
    let mut writer = client.prepare_small_write(16).await.unwrap();
    writer
        .on_data(Bytes::from_static(b"replacement-data"))
        .await
        .unwrap();
    let location = writer.finish_durable().await.unwrap().remove(0);
    assert_ne!(location.chunk_id, original);
    assert_eq!(location.offset, 0);
    client.shutdown_small_writes().await.unwrap();
}
