// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn append_offset_prefetch_survives_cursor_progress_and_live_writer_protects_group() {
    assert!(
        std::env::var("CROWDB_KV_SERVER_BIN").is_ok() || common::cluster::crowdb_kv_server_bin().is_some(),
        "build crowdb-kv-server before reservation acceptance"
    );
    let cluster = KvCluster::start().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let writer_epoch = 79;
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::Repo,
            writer_epoch,
            30_000,
        )
        .await
        .unwrap();
    let chunk_id = chunk.id.unwrap();
    let group_id = ChunkId { high: 197, low: 1 };
    let fence = ReservationFence {
        expected_modify_ts: chunk.modify_ts,
        writer_epoch,
        lease_generation: 1,
        lease_ms: 30_000,
    };
    let spec = ReserveGroupSpec {
        reservation_offset_kb: None,
        strip_size: 1,
        strip_count: 2,
        copy_count: 3,
        conversion_data_num: 0,
        conversion_code_num: 0,
    };
    let reserved = harness
        .handler
        .reserve_strip_group(&chunk_id, &group_id, fence, spec)
        .await
        .unwrap();
    let group = reserved.group.unwrap();
    let advanced = harness
        .handler
        .advance_chunk_write(&chunk_id, writer_epoch, reserved.chunk.modify_ts, 1, None, 30_000)
        .await
        .unwrap();
    let append_offset = group.strips.last().unwrap().chunk_offset + group.strips.last().unwrap().capacity;
    let next = harness
        .handler
        .reserve_strip_group(
            &chunk_id,
            &ChunkId { high: 197, low: 2 },
            ReservationFence {
                expected_modify_ts: reserved.chunk.modify_ts,
                ..fence
            },
            ReserveGroupSpec {
                reservation_offset_kb: Some(append_offset),
                ..spec
            },
        )
        .await
        .unwrap();
    assert_eq!(next.group.unwrap().strips[0].chunk_offset, append_offset);
    assert_eq!(next.chunk.acknowledged_cursor, advanced.acknowledged_cursor);
    assert_eq!(
        next.chunk.strips.len(),
        1,
        "hidden groups do not enter readable layout"
    );
    assert_live_writer_protects_group(&harness, chunk_id, group_id, group, fence).await;
}

async fn assert_live_writer_protects_group(
    harness: &ChunkdbHarness,
    chunk_id: ChunkId,
    group_id: ChunkId,
    mut group: crowdb_protocol::chunkdb::rpc::StripReservationGroup,
    fence: ReservationFence,
) {
    group.lease_deadline_ms = 0;
    harness.store.put_reservation_group(&group).await.unwrap();
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    assert_eq!(
        harness
            .handler
            .recover_expired_reservation_group(&chunk_id, &group_id, now)
            .await
            .unwrap(),
        ReservationRecovery::Active
    );
    let first = &group.strips[0];
    let consumed = harness
        .handler
        .mutate_strip_reservation(
            &chunk_id,
            &group_id,
            fence,
            ReservationUpdate {
                strip_sequence: first.strip_sequence,
                action: StripReservationAction::Consume,
                acknowledged_cursor: u64::from(first.chunk_offset + first.capacity) * 1024,
                closed_strip_sequence: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        consumed.group.unwrap().states[0],
        StripReservationState::Consumed as i32
    );
}

pub(super) async fn confirm_group(
    harness: &ChunkdbHarness,
    chunk_id: &ChunkId,
    group_id: &ChunkId,
    fence: &mut ReservationFence,
    group: crowdb_protocol::chunkdb::rpc::StripReservationGroup,
) {
    for strip in &group.strips {
        let cursor = u64::from(strip.chunk_offset + strip.capacity) * 1024;
        harness
            .handler
            .mutate_strip_reservation(
                chunk_id,
                group_id,
                *fence,
                ReservationUpdate {
                    strip_sequence: strip.strip_sequence,
                    action: StripReservationAction::Consume,
                    acknowledged_cursor: cursor,
                    closed_strip_sequence: Some(strip.strip_sequence),
                },
            )
            .await
            .unwrap();
        let confirmed = harness
            .handler
            .mutate_strip_reservation(
                chunk_id,
                group_id,
                *fence,
                ReservationUpdate {
                    strip_sequence: strip.strip_sequence,
                    action: StripReservationAction::Confirm,
                    acknowledged_cursor: cursor,
                    closed_strip_sequence: Some(strip.strip_sequence),
                },
            )
            .await
            .unwrap();
        fence.expected_modify_ts = confirmed.chunk.modify_ts;
    }
}
