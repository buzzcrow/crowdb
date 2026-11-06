// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;
#[path = "common/repair_targets.rs"]
mod repair_targets;

use crowdb_chunkdb::repair::decode_payload;
use crowdb_chunkdb::task::SegmentOwnerResolver;
use crowdb_paxos_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::chunk_task::{ChunkTaskState, RepairTargetPhase};
use crowdb_protocol::chunkdb::rpc::{SegmentOwnerDisposition, Strip};
use crowdb_protocol::key::{BinaryKey, BusyBlockKey};
use repair_targets::TestRepairTargets;
use std::sync::Arc;

#[tokio::test]
async fn expired_unpublished_repair_target_is_replaced_without_freeing_its_allocation() {
    let mut test = TestRepairTargets::start().await;
    test.stop_target(true).await;
    let task = test.execute().await;
    assert_eq!(task.state, ChunkTaskState::Completed, "{}", task.last_error);
    let chunk = test
        .harness
        .handler
        .query_chunk(&test.chunk.id.unwrap())
        .await
        .unwrap();
    let Strip::MirrorStrip(mirror) = chunk.strips[0].strip.as_ref().unwrap() else {
        unreachable!()
    };
    assert!(!mirror.segments.contains(&test.failed));
    assert!(!mirror.segments.contains(&test.target));
    let replacement = mirror
        .segments
        .iter()
        .find(|segment| **segment != test.survivor)
        .unwrap();
    let unit = u64::from(chunk.strips[0].unit_kb) * 1024;
    assert_eq!(
        test.io.read_segment(replacement, unit).await.unwrap().as_ref(),
        vec![42; usize::try_from(unit).unwrap()]
    );
    let owner = SegmentOwnerResolver::new(Arc::clone(&test.harness.handler), Arc::clone(&test.tasks));
    assert_eq!(
        owner
            .resolve(&test.chunk.id.unwrap(), &test.target)
            .await
            .unwrap(),
        SegmentOwnerDisposition::Absent
    );
    let key = BusyBlockKey {
        disk_id: test.target.disk_id.unwrap(),
        zone_index: test.target.zone_index,
        unit_offset: test.target.unit_offset,
    }
    .to_bytes();
    assert!(
        matches!(
            test.cluster
                .make_crowdb_client()
                .get(0, 1, &key, ReadMode::Linearizable, None)
                .await
                .unwrap(),
            GetOutcome::Found { .. }
        ),
        "unpublished allocation must remain for the DiskDB owner scanner grace period"
    );
}

#[tokio::test]
async fn live_unpublished_repair_target_is_retained_after_a_transport_failure() {
    let mut test = TestRepairTargets::start().await;
    test.stop_target(false).await;
    let task = test.execute().await;
    assert_eq!(task.state, ChunkTaskState::RetryWait);
    let payload = decode_payload(&task.payload).unwrap();
    assert_eq!(payload.targets.len(), 1);
    assert_eq!(payload.targets[0].destination, test.target);
    assert_eq!(payload.targets[0].phase, RepairTargetPhase::Allocated);
    let owner = SegmentOwnerResolver::new(Arc::clone(&test.harness.handler), Arc::clone(&test.tasks));
    assert_eq!(
        owner
            .resolve(&test.chunk.id.unwrap(), &test.target)
            .await
            .unwrap(),
        SegmentOwnerDisposition::TaskPending
    );
}

#[tokio::test]
async fn published_repair_target_survives_expiry_before_its_task_checkpoint() {
    let mut test = TestRepairTargets::start().await;
    let published = test.publish_before_checkpoint().await;
    test.stop_target(true).await;
    let task = test.execute().await;
    assert_eq!(task.state, ChunkTaskState::Completed, "{}", task.last_error);
    let payload = decode_payload(&task.payload).unwrap();
    assert_eq!(payload.targets.len(), 1);
    assert_eq!(payload.targets[0].destination, test.target);
    assert_eq!(payload.targets[0].phase, RepairTargetPhase::Confirmed);
    assert_eq!(
        test.harness
            .handler
            .query_chunk(&test.chunk.id.unwrap())
            .await
            .unwrap(),
        published
    );
}
