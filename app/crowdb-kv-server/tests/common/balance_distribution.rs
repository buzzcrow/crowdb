// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv_server::group0_control_plane::Group0ControlPlane;
use crowdb_protocol::{
    chunk_kv::{
        ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage,
        ChunkKvRangeCatalogPageRef, ChunkKvRangeCatalogPartitionState, HostedPartition, Id128, KeyRange,
        OwnerDescriptor, PartitionArtifact,
    },
    chunk_stream::StreamName,
    common::{ChunkKvExtra, ChunkKvPartitionLoad, InstanceValue, ServiceExtra},
    key::{ChunkKvRangeCatalogHeadKey, ChunkKvRangeCatalogPageKey, InstanceKey, TextKey},
};

pub async fn seed(control: &Group0ControlPlane, distribution: &[(u64, u64)], generation: u64) {
    let mut page = ChunkKvRangeCatalogPage {
        generation,
        page_index: 0,
        checksum: [0; 32],
        entries: distribution
            .iter()
            .enumerate()
            .map(|(index, (owner, _))| entry(index, *owner, distribution.len(), generation))
            .collect(),
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation,
        previous_generation: (generation > 1).then_some(generation - 1),
        checksum: [0; 32],
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: generation,
            page_index: 0,
            first_key: vec![],
            page_checksum: page.checksum,
        }],
    };
    head.seal().unwrap();
    let mut records = vec![
        (
            Bytes::from(ChunkKvRangeCatalogHeadKey.to_path()),
            Bytes::from(serde_json::to_vec(&head).unwrap()),
        ),
        (
            Bytes::from(
                ChunkKvRangeCatalogPageKey {
                    generation,
                    page_index: 0,
                }
                .to_path(),
            ),
            Bytes::from(serde_json::to_vec(&page).unwrap()),
        ),
    ];
    for owner in [1, 2] {
        let mut extra = ChunkKvExtra {
            capacity_bytes: 10_000,
            ..Default::default()
        };
        for (index, (_, bytes)) in distribution
            .iter()
            .enumerate()
            .filter(|(_, (id, _))| *id == owner)
        {
            let partition_id = Id128 {
                high: 41,
                low: u64::try_from(index).unwrap() + 1,
            };
            extra.durable_bytes += bytes;
            extra.hosted.push(HostedPartition {
                partition_id,
                owner_epoch: generation,
                recovering: false,
            });
            extra.partition_loads.push(ChunkKvPartitionLoad {
                partition_id,
                durable_bytes: *bytes,
                logical_bytes: 0,
                logical_metrics_exact: false,
                live_byte_samples: vec![],
                independently_recoverable: true,
            });
        }
        let instance = InstanceValue {
            instance_id: owner,
            rpc_endpoint: format!("127.0.0.1:{}", 17000 + owner),
            last_heartbeat_ms: u64::MAX,
            extra: Some(ServiceExtra {
                chunk_kv: Some(extra),
                ..Default::default()
            }),
        };
        records.push((
            Bytes::from(
                InstanceKey {
                    service: "chunk-kv".into(),
                    instance_id: owner,
                }
                .to_path(),
            ),
            Bytes::from(serde_json::to_vec(&instance).unwrap()),
        ));
    }
    control.put_batch(records).await.unwrap();
}

fn entry(index: usize, owner: u64, length: usize, generation: u64) -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id: Id128 {
            high: 41,
            low: u64::try_from(index).unwrap() + 1,
        },
        range: KeyRange {
            start: if index == 0 {
                vec![]
            } else {
                vec![u8::try_from(index).unwrap()]
            },
            end: (index + 1 < length).then(|| vec![u8::try_from(index + 1).unwrap()]),
        },
        owner: OwnerDescriptor {
            instance_id: owner,
            rpc_endpoint: format!("127.0.0.1:{}", 17000 + owner),
        },
        owner_epoch: generation,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        transition_id: None,
        artifact: PartitionArtifact {
            tree_id: 50 + u64::try_from(index).unwrap(),
            stream_name: StreamName {
                high: 51,
                low: u64::try_from(index).unwrap() + 1,
            },
            tail_overlay: None,
        },
    }
}
