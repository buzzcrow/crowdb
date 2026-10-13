// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn large_write_replaces_failed_data_and_parity_segments_end_to_end() {
    let stack = E2eStack::start(small_policy()).await;
    let (allocator, disk_writer) = real_parts(&stack).await;
    for fail_on in [1, 5] {
        let fault = Arc::new(FailWriteCall {
            inner: disk_writer.clone(),
            calls: AtomicUsize::new(0),
            fail_on,
            persistent: false,
            failed_segment: Mutex::new(None),
            segments: Mutex::new(Vec::new()),
            failure_delay: Duration::ZERO,
            failure_pending: AtomicBool::new(false),
            successes_during_failure: AtomicUsize::new(0),
        });
        let client =
            ChunkIoClient::from_parts_with_small_policy(allocator.clone(), fault.clone(), small_policy())
                .unwrap();
        let data = make_test_data(4 * MIB);
        let result = client
            .prepare_large_write(Some(data.len() as u64), policy(16 * MIB as u64))
            .write_stream(data.as_slice())
            .await
            .unwrap();
        let failed = fault.failed_segment.lock().unwrap().expect("injected segment");
        let chunk = stack.query_chunk(&result.locations[0]).await;
        let Some(Strip::EcStrip(ec)) = &chunk.strips[0].strip else {
            panic!("large write did not produce EC");
        };
        assert!(!ec.segments.contains(&failed));
        let initial = fault.segments.lock().unwrap()[..5].to_vec();
        assert_eq!(
            initial
                .iter()
                .filter(|segment| ec.segments.contains(segment))
                .count(),
            4
        );
        assert_eq!(client.large_write_repair_metrics().repaired_segments, 1);
        assert_eq!(
            client.read_object(&result.locations).await.unwrap().concat(),
            data
        );
        assert_ec_parity(&stack, &chunk, &result.locations[0]).await;
    }
}
