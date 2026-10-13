// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn transient_mirror_read_failure_does_not_mark_segment_unavailable() {
    let stack = E2eStack::start(small_policy(2)).await;
    let data = Bytes::from(test_data(8 * KIB));
    let mut writer = stack.client.prepare_small_write(data.len()).await.unwrap();
    writer.on_data(data.clone()).await.unwrap();
    let location = writer.on_finish().await.unwrap().remove(0);
    let before = stack.query_chunk(&location).await;
    let strip = before
        .strips
        .iter()
        .find(|strip| u64::from(strip.chunk_offset) * KIB as u64 <= location.offset)
        .unwrap();
    let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
        panic!("small write did not produce a mirror strip");
    };
    // Exhaust both protected replicas so the transient failure reaches the
    // caller, while neither replica acquires a durable corruption marker.
    let failed = mirror
        .segments
        .iter()
        .map(|segment| segment.disk_id.unwrap())
        .collect();
    let reader = reader_with_transient_failure(&stack, failed).await;
    assert!(reader.read_object(std::slice::from_ref(&location)).await.is_err());
    let after = stack.query_chunk(&location).await;
    assert!(after.strips[0].unavailable_segments.is_empty());
    assert_eq!(
        stack
            .client
            .read_object(std::slice::from_ref(&location))
            .await
            .unwrap()
            .concat(),
        data
    );
    stack.client.shutdown_small_writes().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn chunkdb_restart_admits_verified_corruption_and_repairs_full_shard() {
    let mut options = ChunkdbStartOptions {
        repair_enabled: false,
        ..ChunkdbStartOptions::default()
    };
    let mut stack = E2eStack::start_with_chunkdb_options(small_policy(2), options).await;
    let data = test_data(4 * MIB);
    let result = stack
        .client
        .prepare_large_write(Some(data.len() as u64), large_policy())
        .write_stream(data.as_slice())
        .await
        .unwrap();
    let before = stack.query_chunk(&result.locations[0]).await;
    let Some(Strip::EcStrip(ec)) = &before.strips[0].strip else {
        panic!("large write did not produce EC");
    };
    let failed_segment = ec.segments[0];
    let failed_disk = failed_segment.disk_id.unwrap();
    let (reader, fault) = reader_with_corruption(&stack, vec![failed_disk]).await;
    let length = 16 * KIB as u64;
    assert_eq!(
        reader
            .read_range(&result.locations, 0, length)
            .await
            .unwrap()
            .concat(),
        data[..16 * KIB]
    );
    assert!(fault.max_read.load(Ordering::Relaxed) <= 64 * KIB);
    let marked = stack.query_chunk(&result.locations[0]).await;
    let Some(Strip::EcStrip(marked_ec)) = &marked.strips[0].strip else {
        panic!("large write did not remain EC");
    };
    assert!(
        marked.strips[0].unavailable_segments.contains(&failed_segment)
            || !marked_ec.segments.contains(&failed_segment),
        "verified corruption must be marked or already repaired"
    );

    options.repair_enabled = true;
    stack.crash_and_restart_chunkdb_with_options(options).await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(35);
    loop {
        let repaired = stack.query_chunk(&result.locations[0]).await;
        let Some(Strip::EcStrip(current)) = &repaired.strips[0].strip else {
            panic!("large write did not remain EC");
        };
        if !current.segments.contains(&failed_segment) && repaired.strips[0].unavailable_segments.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "restarted chunkdb did not reconstruct the full failed shard: {:?}; metrics: {}",
            repaired.strips[0],
            stack.repair_metrics().await
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (after_restart, _) = reader_with_failures(&stack, vec![failed_disk]).await;
    assert_eq!(
        after_restart
            .read_range(&result.locations, 0, length)
            .await
            .unwrap()
            .concat(),
        data[..16 * KIB]
    );
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn routed_full_fragment_requests_share_one_durable_repair() {
    let stack = E2eStack::start_with_chunkdb_options(
        small_policy(2),
        ChunkdbStartOptions {
            repair_enabled: false,

            ..ChunkdbStartOptions::default()
        },
    )
    .await;
    let data = test_data(4 * MIB);
    let write = stack
        .client
        .prepare_large_write(Some(data.len() as u64), large_policy())
        .write_stream(data.as_slice())
        .await
        .unwrap();
    let chunk = stack.query_chunk(&write.locations[0]).await;
    let chunk_id = chunk.id.unwrap();
    let old = chunk.strips[0].clone();
    let Some(Strip::EcStrip(ec)) = &old.strip else {
        panic!("large write did not produce EC");
    };
    let failed = ec.segments[0];
    let expected = stack
        .read_segment(&failed, MIB as u64, 0, u32::try_from(MIB).unwrap())
        .await;
    let (client, _) = real_parts(&stack).await;
    // Seed the durable marker as though a prior verified frame read reported
    // corruption; this isolates the full-block rendezvous from detection.
    let mut marked = old.clone();
    marked.unavailable_segments.push(failed);
    let updated = client
        .replace_chunk_strip_range(ReplaceChunkStripRangeRequest {
            chunk_id: Some(chunk_id),
            expected_modify_ts: chunk.modify_ts,
            start_index: 0,
            old_strips: vec![old],
            replacement_strips: vec![marked],
            operation_id: Some(ChunkId { high: 0x171, low: 1 }),
        })
        .await
        .unwrap()
        .chunk
        .unwrap();
    let request = AdHocEcRecoveryRequest {
        version: 1,
        chunk_id: Some(chunk_id),
        expected_modify_ts: updated.modify_ts,
        strip_sequence: updated.strips[0].strip_sequence,
        failed_segment: Some(failed),
        operation_id: Some(ChunkId { high: 0x171, low: 2 }),
        request_full_block: true,
    };
    let stale = AdHocEcRecoveryRequest {
        expected_modify_ts: updated.modify_ts.saturating_sub(1),
        ..request.clone()
    };
    assert_eq!(
        client.ad_hoc_ec_recovery(stale).await.unwrap().disposition,
        AdHocEcRecoveryDisposition::Stale
    );
    let (first, second) = tokio::join!(
        client.ad_hoc_ec_recovery(request.clone()),
        client.ad_hoc_ec_recovery(request)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert!(matches!(
        first.disposition,
        AdHocEcRecoveryDisposition::Started | AdHocEcRecoveryDisposition::Coalesced
    ));
    assert!(matches!(
        second.disposition,
        AdHocEcRecoveryDisposition::Started | AdHocEcRecoveryDisposition::Coalesced
    ));
    assert_eq!(first.data, expected);
    assert_eq!(second.data, expected);
    let metrics = stack.repair_metrics().await;
    assert_eq!(metrics["ad_hoc_starts"].as_u64(), Some(1));
    assert_eq!(metrics["tasks_admitted"].as_u64(), Some(1));
}
