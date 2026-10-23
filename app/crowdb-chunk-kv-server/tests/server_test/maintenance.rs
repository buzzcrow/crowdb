// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_chunk_kv::{MutationOperation, RequestId};

fn install_grant(service: &ChunkKvService, generation: u64) {
    let mut grant = ServingGrant {
        instance_id: INSTANCE_ID,
        lease_sequence: generation,
        catalog_generation: generation,
        issued_at_ms: 1_000,
        expires_at_ms: 13_000,
        assignments: vec![ServingAssignment {
            partition_id: Id128 { high: 1, low: 2 },
            owner_epoch: EPOCH,
        }],
        assignment_digest: [0; 32],
    };
    grant.seal();
    service
        .authority()
        .install(grant, &policy(), 1_000, service.monotonic_ms())
        .unwrap();
}

async fn write(partition: &Partition, sequence: u64) {
    partition
        .mutate(
            EPOCH,
            RequestId {
                client_high: 1,
                client_low: 1,
                client_sequence: sequence,
            },
            MutationOperation::Put {
                key: b"key".to_vec(),
                value: vec![1],
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn metadata_maintenance_requires_a_live_independent_assignment() {
    let (service, partition) = fixture().await;
    write(&partition, 1).await;
    write(&partition, 2).await;
    service.authority().clear();
    assert!(!service.maintain_serving_metadata().await.unwrap());
    assert_eq!(partition.metrics().snapshot().reclaimed_stream_metadata_pages, 0);
    install_grant(&service, 1);
    assert!(!service.maintain_serving_metadata().await.unwrap());
    let reclaimed = partition.metrics().snapshot().reclaimed_stream_metadata_pages;
    assert!(reclaimed > 0);

    let (mut head, mut page) = catalog(
        Id128 { high: 1, low: 2 },
        partition.snapshot().stream_name,
        EPOCH,
        2,
        Some(1),
    );
    page.entries[0].transition_id = Some(Id128 { high: 20, low: 21 });
    page.seal().unwrap();
    head.pages[0].page_checksum = page.checksum;
    head.seal().unwrap();
    service
        .install_catalog_and_reconcile(&head, &[page], &[])
        .unwrap();
    install_grant(&service, 2);
    write(&partition, 3).await;
    assert!(!service.maintain_serving_metadata().await.unwrap());
    assert_eq!(
        partition.metrics().snapshot().reclaimed_stream_metadata_pages,
        reclaimed
    );

    let (head, page) = catalog(
        Id128 { high: 1, low: 2 },
        partition.snapshot().stream_name,
        EPOCH,
        3,
        Some(2),
    );
    service
        .install_catalog_and_reconcile(&head, &[page], &[])
        .unwrap();
    install_grant(&service, 3);
    assert!(!service.maintain_serving_metadata().await.unwrap());
    assert!(partition.metrics().snapshot().reclaimed_stream_metadata_pages > reclaimed);
}
