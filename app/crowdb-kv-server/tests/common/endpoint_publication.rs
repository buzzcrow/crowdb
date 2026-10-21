// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_kv_server::background::domain_monitor::ChunkKvRangeMonitorDriver;

#[tokio::test]
async fn recovered_owner_address_advances_catalog_without_transferring_data() {
    let control = endpoint_fixture().await;
    let head: ChunkKvRangeCatalogHead = get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
    let page_path = ChunkKvRangeCatalogPageKey {
        generation: head.pages[0].page_generation,
        page_index: head.pages[0].page_index,
    }
    .to_path();
    let page: ChunkKvRangeCatalogPage = get_json(&control, &page_path).await;
    let prior = page.entries[0].clone();
    let path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: prior.owner.instance_id,
    }
    .to_path();
    let value = control.get(path.as_bytes()).await.unwrap();
    let mut instance: InstanceValue = serde_json::from_slice(value.value.as_ref().unwrap()).unwrap();
    instance.rpc_endpoint = "192.0.2.91:7401".into();
    instance.extra.as_mut().unwrap().chunk_kv.as_mut().unwrap().hosted = vec![HostedPartition {
        partition_id: prior.partition_id,
        owner_epoch: prior.owner_epoch,
        recovering: true,
    }];
    control
        .compare_and_put(
            Bytes::from(path),
            Bytes::from(serde_json::to_vec(&instance).unwrap()),
            value.revision,
        )
        .await
        .unwrap();
    let mut descriptor = descriptor();
    descriptor.domain = "chunk-kv".into();
    descriptor.service_registry_name = "chunk-kv".into();
    descriptor.failure_policy = DomainFailurePolicy::OperatorOnly;
    ChunkKvRangeMonitorDriver::new()
        .tick(&control, &descriptor)
        .await
        .unwrap();
    let current: ChunkKvRangeCatalogHead = get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
    assert_eq!(current.generation, head.generation + 1);
    let updated: ChunkKvRangeCatalogPage = get_json(
        &control,
        &ChunkKvRangeCatalogPageKey {
            generation: current.pages[0].page_generation,
            page_index: current.pages[0].page_index,
        }
        .to_path(),
    )
    .await;
    let mut expected = prior;
    expected.owner.rpc_endpoint = instance.rpc_endpoint;
    assert_eq!(updated.entries, vec![expected]);
    let retained: ChunkKvRangeCatalogPage = get_json(&control, &page_path).await;
    assert_eq!(retained, page, "old immutable pages remain intact");
    ChunkKvRangeMonitorDriver::new()
        .tick(&control, &descriptor)
        .await
        .unwrap();
    let repeated: ChunkKvRangeCatalogHead = get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
    assert_eq!(
        repeated, current,
        "same address does not churn catalog generations"
    );
}

#[tokio::test]
async fn unproven_owner_address_cannot_rewrite_the_catalog() {
    for case in ["expired", "wrong-epoch", "unhosted"] {
        let control = endpoint_fixture().await;
        let head: ChunkKvRangeCatalogHead = get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
        let path = InstanceKey {
            service: "chunk-kv".into(),
            instance_id: 1,
        }
        .to_path();
        let record = control.get(path.as_bytes()).await.unwrap();
        let mut instance: InstanceValue = serde_json::from_slice(record.value.as_ref().unwrap()).unwrap();
        instance.rpc_endpoint = "192.0.2.92:7401".into();
        let hosted = &mut instance.extra.as_mut().unwrap().chunk_kv.as_mut().unwrap().hosted;
        match case {
            "expired" => instance.last_heartbeat_ms = 0,
            "wrong-epoch" => hosted[0].owner_epoch += 1,
            "unhosted" => hosted.clear(),
            _ => unreachable!(),
        }
        control
            .compare_and_put(
                Bytes::from(path),
                Bytes::from(serde_json::to_vec(&instance).unwrap()),
                record.revision,
            )
            .await
            .unwrap();
        let mut descriptor = descriptor();
        descriptor.domain = "chunk-kv".into();
        descriptor.service_registry_name = "chunk-kv".into();
        descriptor.failure_policy = DomainFailurePolicy::OperatorOnly;
        ChunkKvRangeMonitorDriver::new()
            .tick(&control, &descriptor)
            .await
            .unwrap();
        let observed: ChunkKvRangeCatalogHead =
            get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
        assert_eq!(observed, head, "{case} observation is not authority to rebind");
    }
}

async fn endpoint_fixture() -> Group0ControlPlane {
    let (_, store) = registry_with_group_zero(PxLocalReplicaRole::Leader);
    let control = Group0ControlPlane::acquire(&store).await.unwrap();
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        checksum: [0; 32],
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id: Id128 { high: 1, low: 2 },
            range: KeyRange {
                start: vec![],
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 1,
                rpc_endpoint: "127.0.0.1:17001".into(),
            },
            owner_epoch: 1,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 3,
                stream_name: StreamName { high: 4, low: 5 },
                tail_overlay: None,
            },
            transition_id: None,
        }],
    };
    page.seal().unwrap();
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        checksum: [0; 32],
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: vec![],
            page_checksum: page.checksum,
        }],
    };
    head.seal().unwrap();
    put_json(
        &control,
        ChunkKvRangeCatalogPageKey {
            generation: 1,
            page_index: 0,
        }
        .to_path(),
        &page,
    )
    .await;
    put_json(&control, ChunkKvRangeCatalogHeadKey.to_path(), &head).await;
    put_chunk_kv_instance(
        &control,
        1,
        ChunkKvExtra {
            hosted: vec![HostedPartition {
                partition_id: page.entries[0].partition_id,
                owner_epoch: 1,
                recovering: false,
            }],
            ..ChunkKvExtra::default()
        },
    )
    .await;
    control
}
