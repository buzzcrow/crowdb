// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_server::group0_control_plane::Group0ControlPlane;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage, ChunkKvRangeCatalogPageRef,
    ChunkKvRangeCatalogPartitionState, HostedPartition, Id128, KeyRange, OwnerDescriptor, PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;
use crowdb_protocol::common::{ChunkKvExtra, ChunkKvPartitionLoad, InstanceValue, ServiceExtra};
use crowdb_protocol::key::{ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, InstanceKey, TextKey};

pub async fn seed(control: &Group0ControlPlane) {
    let partition_ids = [Id128 { high: 41, low: 1 }, Id128 { high: 41, low: 2 }];
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        entries: partition_ids
            .iter()
            .enumerate()
            .map(|(index, partition_id)| ChunkKvRangeCatalogEntry {
                partition_id: *partition_id,
                range: KeyRange {
                    start: if index == 0 { Vec::new() } else { b"m".to_vec() },
                    end: (index == 0).then(|| b"m".to_vec()),
                },
                owner: OwnerDescriptor {
                    instance_id: 1,
                    rpc_endpoint: "127.0.0.1:17001".into(),
                },
                owner_epoch: 1,
                state: ChunkKvRangeCatalogPartitionState::Serving,
                artifact: PartitionArtifact {
                    tree_id: 50 + index as u64,
                    stream_name: StreamName {
                        high: 51,
                        low: index as u64 + 1,
                    },
                    tail_overlay: None,
                },
                transition_id: None,
            })
            .collect(),
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    put_json(
        control,
        ChunkKvRangeCatalogPageKey {
            generation: 1,
            page_index: 0,
        }
        .to_path(),
        &page,
    )
    .await;
    put_json(control, ChunkKvRangeCatalogHeadKey.to_path(), &head).await;

    seed_instances(control, &partition_ids).await;
}

async fn seed_instances(control: &Group0ControlPlane, partition_ids: &[Id128]) {
    let source_loads = partition_ids
        .iter()
        .enumerate()
        .map(|(index, partition_id)| ChunkKvPartitionLoad {
            partition_id: *partition_id,
            durable_bytes: 100,
            live_byte_samples: if index == 0 {
                vec![(b"a".to_vec(), 50), (b"f".to_vec(), 50)]
            } else {
                vec![(b"n".to_vec(), 50), (b"z".to_vec(), 50)]
            },
            independently_recoverable: true,
        })
        .collect();
    for (instance_id, chunk_kv) in [
        (
            1,
            ChunkKvExtra {
                capacity_bytes: 1_000,
                durable_bytes: 200,
                request_rate: 0,
                hosted: partition_ids
                    .iter()
                    .map(|partition_id| HostedPartition {
                        partition_id: *partition_id,
                        owner_epoch: 1,
                        recovering: false,
                    })
                    .collect(),
                partition_loads: source_loads,
                ..ChunkKvExtra::default()
            },
        ),
        (
            2,
            ChunkKvExtra {
                capacity_bytes: 1_000,
                ..ChunkKvExtra::default()
            },
        ),
    ] {
        put_chunk_kv_instance(control, instance_id, chunk_kv).await;
    }
}

async fn put_chunk_kv_instance(control: &Group0ControlPlane, instance_id: u64, chunk_kv: ChunkKvExtra) {
    put_json(
        control,
        InstanceKey {
            service: "chunk-kv".into(),
            instance_id,
        }
        .to_path(),
        &InstanceValue {
            instance_id,
            rpc_endpoint: format!("127.0.0.1:{}", 17_000 + instance_id),
            last_heartbeat_ms: u64::MAX,
            extra: Some(ServiceExtra {
                chunk_kv: Some(chunk_kv),
                ..ServiceExtra::default()
            }),
        },
    )
    .await;
}

async fn put_json<T: serde::Serialize>(control: &Group0ControlPlane, path: String, value: &T) {
    control
        .compare_and_put(
            Bytes::from(path),
            Bytes::from(serde_json::to_vec(value).unwrap()),
            0,
        )
        .await
        .unwrap();
}

pub async fn recent_split(control: &Group0ControlPlane) {
    use crowdb_protocol::chunk_kv::{SplitChildAssignment, SplitPhase, SplitTransition};
    use crowdb_protocol::key::ChunkKvSplitKey;
    let owner = OwnerDescriptor {
        instance_id: 1,
        rpc_endpoint: "127.0.0.1:17001".into(),
    };
    let artifact = |id| PartitionArtifact {
        tree_id: id,
        stream_name: StreamName { high: 51, low: id },
        tail_overlay: None,
    };
    let transition = SplitTransition {
        transition_id: Id128 { high: 91, low: 1 },
        parent_id: Id128 { high: 41, low: 1 },
        parent_range: KeyRange {
            start: vec![],
            end: Some(b"m".to_vec()),
        },
        parent_owner: owner.clone(),
        parent_epoch: 1,
        parent_next_epoch: 2,
        parent_artifact: artifact(50),
        retained_parent_artifact: artifact(60),
        split_key: b"f".to_vec(),
        child: SplitChildAssignment {
            partition_id: Id128 { high: 91, low: 2 },
            range: KeyRange {
                start: b"f".to_vec(),
                end: Some(b"m".to_vec()),
            },
            owner,
            owner_epoch: 2,
            artifact: artifact(61),
        },
        planned_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap(),
        phase: SplitPhase::Aborted,
        readiness_proof: None,
        failure: Some("split preparation aborted while the original source remains serving".into()),
    };
    transition.validate().unwrap();
    put_json(
        control,
        ChunkKvSplitKey {
            transition_id: transition.transition_id,
        }
        .to_path(),
        &transition,
    )
    .await;
}
