// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage,
    OwnerDescriptor, TargetReadinessProof, TransferPhase, TransferReadinessLimits,
};
use crowdb_protocol::common::InstanceValue;
use crowdb_protocol::key::{ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, InstanceKey};

#[tokio::test]
async fn completed_recovery_does_not_starve_an_idle_owner_of_placement() {
    let fixture = TestBalancePlanning::new().await;
    seed_committed_recoveries(&fixture.control).await;
    fixture.tick().await;
    let rows = fixture
        .control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap();
    let planned: Vec<TransferTransition> = rows
        .iter()
        .map(|row| serde_json::from_slice(&row.value).unwrap())
        .filter(|transfer: &TransferTransition| transfer.phase == TransferPhase::Planned)
        .collect();
    assert_eq!(
        planned.len(),
        1,
        "completed recovery must permit another placement"
    );
    assert_eq!(
        (planned[0].source.instance_id, planned[0].target.instance_id),
        (1, 2)
    );
    assert_eq!(planned[0].partition_id, Id128 { high: 41, low: 1 });
}

async fn seed_committed_recoveries(control: &Group0ControlPlane) {
    let mut head: ChunkKvRangeCatalogHead = read(control, ChunkKvRangeCatalogHeadKey.to_path()).await;
    let mut page: ChunkKvRangeCatalogPage = read(
        control,
        ChunkKvRangeCatalogPageKey {
            generation: head.generation,
            page_index: 0,
        }
        .to_path(),
    )
    .await;
    let mut records = Vec::new();
    for entry in &mut page.entries {
        let transfer = committed_recovery(entry);
        transfer.validate().unwrap();
        entry.owner_epoch = transfer.target_epoch;
        entry.transition_id = Some(transfer.transition_id);
        records.push(record(
            ChunkKvTransferKey {
                transition_id: transfer.transition_id,
            }
            .to_path(),
            &transfer,
        ));
    }
    page.generation += 1;
    page.seal().unwrap();
    head.previous_generation = Some(head.generation);
    head.generation = page.generation;
    head.pages[0].page_generation = page.generation;
    head.pages[0].page_checksum = page.checksum;
    head.seal().unwrap();
    head.validate_pages(std::slice::from_ref(&page)).unwrap();
    records.push(record(
        ChunkKvRangeCatalogPageKey {
            generation: page.generation,
            page_index: 0,
        }
        .to_path(),
        &page,
    ));
    records.push(record(ChunkKvRangeCatalogHeadKey.to_path(), &head));
    let path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: 1,
    }
    .to_path();
    let mut instance: InstanceValue = read(control, path.clone()).await;
    for hosted in &mut instance.extra.as_mut().unwrap().chunk_kv.as_mut().unwrap().hosted {
        hosted.owner_epoch = 2;
    }
    records.push(record(path, &instance));
    control.put_batch(records).await.unwrap();
}

async fn read<T: serde::de::DeserializeOwned>(control: &Group0ControlPlane, path: String) -> T {
    let row = control.get(path.as_bytes()).await.unwrap();
    serde_json::from_slice(row.value.as_deref().unwrap()).unwrap()
}

fn record<T: serde::Serialize>(path: String, value: &T) -> (Bytes, Bytes) {
    (Bytes::from(path), Bytes::from(serde_json::to_vec(value).unwrap()))
}

fn committed_recovery(entry: &ChunkKvRangeCatalogEntry) -> TransferTransition {
    let proof = TargetReadinessProof {
        target_instance_id: entry.owner.instance_id,
        target_epoch: entry.owner_epoch + 1,
        artifact: entry.artifact.clone(),
        durable_tail: 0,
    };
    TransferTransition {
        transition_id: Id128 {
            high: 99,
            low: entry.partition_id.low,
        },
        partition_id: entry.partition_id,
        range: entry.range.clone(),
        source: OwnerDescriptor {
            instance_id: 3,
            rpc_endpoint: "127.0.0.1:17003".into(),
        },
        source_epoch: entry.owner_epoch,
        target: entry.owner.clone(),
        target_epoch: proof.target_epoch,
        artifact: entry.artifact.clone(),
        target_artifact: entry.artifact.clone(),
        readiness_limits: TransferReadinessLimits {
            max_tail_records: 100,
            max_tail_bytes: 1_000_000,
            max_estimated_catchup_ms: 1_000,
            prepare_deadline_ms: 1_000,
            forwarding_grace_ms: 1_000,
        },
        planned_at_ms: 0,
        old_grant_expires_at_ms: 1,
        phase: TransferPhase::CatalogCommitted,
        release_proof: Some(AuthorityReleaseProof::LeaseExpired {
            activation_not_before_ms: 2,
        }),
        readiness_proof: Some(proof.clone()),
        catchup_proof: Some(proof),
        failure: None,
    }
}
