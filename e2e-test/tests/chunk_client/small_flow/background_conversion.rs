// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
#[allow(clippy::too_many_lines)]
async fn automatic_chunkdb_scan_converts_three_groups_and_preserves_tail() {
    let mut configured = policy();
    configured.chunk_capacity = 32 * MIB as u64;
    configured.conversion_enabled = false;
    let stack = E2eStack::start_with_chunkdb_options(
        configured,
        ChunkdbStartOptions {
            conversion_enabled: true,
            conversion_min_seal_age_secs: 10,
            conversion_scan_interval_secs: 1,
            ..ChunkdbStartOptions::default()
        },
    )
    .await;
    let mut object_groups = Vec::new();
    for value in 31_u8..57 {
        object_groups.push(write_full_small_strip(&stack.client, value).await);
    }
    let locations: Vec<Location> = object_groups
        .iter()
        .flat_map(|group| group.iter().map(|(_, location)| location.clone()))
        .collect();
    assert!(locations
        .iter()
        .all(|location| location.chunk_id == locations[0].chunk_id));
    let before = stack.query_chunk(&locations[0]).await;
    let mut data = Vec::with_capacity(object_groups.len());
    for group in &object_groups {
        data.push(read_data_image(&stack, &before, &group[0].1).await);
    }
    stack.client.shutdown_small_writes().await.unwrap();
    let (chunkdb, _) = real_write_parts(&stack).await;
    let too_young = chunkdb
        .trigger_conversion_batch(TriggerConversionBatchRequest {
            filter: ConversionFilter {
                sealed_only: true,
                max_chunks: 10,
            },
        })
        .await
        .expect("batch conversion trigger");
    assert_eq!(too_young.accepted_chunks, 0);

    let converted = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let chunk = stack.query_chunk(&locations[0]).await;
            if chunk
                .strips
                .iter()
                .filter(|strip| matches!(strip.strip, Some(Strip::EcStrip(_))))
                .count()
                == 3
            {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("automatic conversion scan did not finish");
    assert_eq!(converted.state, ChunkState::Sealed as i32);
    assert_eq!(converted.strips.len(), 5);
    assert!(converted.strips[3..]
        .iter()
        .all(|strip| matches!(strip.strip, Some(Strip::MirrorStrip(_)))));
    for group in 0..3 {
        let ec_strip = &converted.strips[group];
        let Some(Strip::EcStrip(ec)) = &ec_strip.strip else {
            panic!("converted group {group} is not EC");
        };
        assert_eq!((ec.data_num, ec.code_num, ec.segments.len()), (8, 4, 12));
        let unit_bytes = u64::from(ec_strip.unit_kb) * KIB as u64;
        let group_data = &data[group * 8..group * 8 + 8];
        for (segment, expected) in ec.segments[..8].iter().zip(group_data) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
                .await;
            assert_eq!(&actual, expected.as_ref());
        }
        let refs: Vec<&[u8]> = group_data.iter().map(Bytes::as_ref).collect();
        let parity = encode_parity_from_shards(EcScheme::new(8, 4), &refs).unwrap();
        for (segment, expected) in ec.segments[8..].iter().zip(parity) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
                .await;
            assert_eq!(actual, expected);
        }
    }
    for group in &object_groups[24..] {
        for (expected, location) in group {
            assert_mirror_data(&stack, &converted, location, expected).await;
        }
    }
    for group in &object_groups {
        for (expected, location) in group {
            assert_eq!(
                stack
                    .client
                    .read_object(std::slice::from_ref(location))
                    .await
                    .unwrap()
                    .concat(),
                *expected
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires the owned release storage containers"]
async fn chunkdb_restart_recovers_an_inflight_conversion_claim() {
    let mut configured = policy();
    configured.chunk_capacity = 32 * MIB as u64;
    configured.conversion_enabled = false;
    let mut stack = E2eStack::start_with_chunkdb_options(
        configured,
        ChunkdbStartOptions {
            conversion_enabled: true,
            conversion_min_seal_age_secs: 5,
            conversion_scan_interval_secs: 1,
            conversion_max_bandwidth_mbps: 1,
            conversion_task_lease_secs: 1,
            ..ChunkdbStartOptions::default()
        },
    )
    .await;
    let mut object_groups = Vec::new();
    for value in 71_u8..87 {
        object_groups.push(write_full_small_strip(&stack.client, value).await);
    }
    let locations: Vec<Location> = object_groups
        .iter()
        .flat_map(|group| group.iter().map(|(_, location)| location.clone()))
        .collect();
    let before = stack.query_chunk(&locations[0]).await;
    let mut data = Vec::with_capacity(object_groups.len());
    for group in &object_groups {
        data.push(read_data_image(&stack, &before, &group[0].1).await);
    }
    stack.client.shutdown_small_writes().await.unwrap();
    stack.wait_for_conversion_active().await;
    stack.crash_and_restart_chunkdb().await;

    let recovered = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let chunk = stack.query_chunk(&locations[0]).await;
            if chunk
                .strips
                .iter()
                .filter(|strip| matches!(strip.strip, Some(Strip::EcStrip(_))))
                .count()
                == 2
            {
                break chunk;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("restarted chunkdb did not recover conversion task");
    assert_eq!(recovered.strips.len(), 2);
    for (group, strip) in recovered.strips.iter().enumerate() {
        let Some(Strip::EcStrip(ec)) = &strip.strip else {
            panic!("recovered group {group} is not EC");
        };
        let group_data = &data[group * 8..group * 8 + 8];
        let unit_bytes = u64::from(strip.unit_kb) * KIB as u64;
        for (segment, expected) in ec.segments[..8].iter().zip(group_data) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
                .await;
            assert_eq!(&actual, expected.as_ref());
        }
        let refs: Vec<&[u8]> = group_data.iter().map(Bytes::as_ref).collect();
        let parity = encode_parity_from_shards(EcScheme::new(8, 4), &refs).unwrap();
        for (segment, expected) in ec.segments[8..].iter().zip(parity) {
            let actual = stack
                .read_segment(segment, unit_bytes, 0, u32::try_from(MIB).unwrap())
                .await;
            assert_eq!(actual, expected);
        }
    }
}
