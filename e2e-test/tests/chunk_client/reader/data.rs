// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn ec_range_read_recovers_only_the_requested_bytes() {
    let stack = E2eStack::start(small_policy(2)).await;
    let data = test_data(12 * MIB);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), large_policy())
        .write_stream(data.as_slice())
        .await
        .unwrap();
    let chunk = stack.query_chunk(&result.locations[0]).await;
    let Some(Strip::EcStrip(ec)) = &chunk.strips[0].strip else {
        panic!("large write did not produce EC");
    };
    let failed = ec.segments[1].disk_id.unwrap();
    let (reader, fault) = reader_with_failures(&stack, vec![failed]).await;
    let start = MIB as u64 + 123;
    let end = start + 16 * KIB as u64;
    let actual = reader
        .read_range(&result.locations, start, end)
        .await
        .unwrap()
        .concat();
    assert_eq!(
        actual,
        data[usize::try_from(start).unwrap()..usize::try_from(end).unwrap()]
    );
    let direct = stack
        .read_segment(&ec.segments[0], MIB as u64, 0, u32::try_from(64 * KIB).unwrap())
        .await;
    let frame = parse_frame(&direct, result.locations[0].chunk_id.unwrap()).unwrap();
    assert_eq!(frame.payload, &data[..frame.payload.len()]);
    assert!(fault.max_read.load(Ordering::Relaxed) <= 64 * KIB);
    assert_eq!(
        reader.read_object(&result.locations).await.unwrap().concat(),
        data
    );

    let after = stack.query_chunk(&result.locations[0]).await;
    assert!(after.strips[0].unavailable_segments.is_empty());
    let Some(Strip::EcStrip(current)) = &after.strips[0].strip else {
        panic!("large write did not remain EC");
    };
    assert!(current.segments.contains(&ec.segments[1]));
    let metrics = stack.repair_metrics().await;
    assert_eq!(metrics["tasks_admitted"].as_u64(), Some(0));
    assert_eq!(metrics["memory_bytes"].as_u64(), Some(0));
    assert_eq!(metrics["memory_limit_bytes"].as_u64(), Some(64 * MIB as u64));
}
