// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn completed_replay_survives_remote_assignment_changes_after_original_deadline() {
    let publisher = ChunkKvRangeCatalogPublisher::new(Arc::new(MemoryChunkKvRangeCatalogStore::default()));
    let mut first = page(1, 1);
    first.entries[0].range.end = Some(b"m".to_vec());
    let mut remote = first.entries[0].clone();
    remote.partition_id.low = 2;
    remote.owner.instance_id = 9;
    remote.owner.rpc_endpoint = "127.0.0.1:9901".into();
    remote.range.start = b"m".to_vec();
    remote.range.end = None;
    remote.artifact.tree_id = 2;
    remote.artifact.stream_name.low = 4;
    first.entries.push(remote);
    first.seal().unwrap();
    publisher
        .publish(head(1, None, &first), vec![first.clone()])
        .await
        .unwrap();
    let mut calls = 0;
    let recovered = publisher
        .recover_current_for_instance(8, std::time::Instant::now(), |pages| {
            calls += 1;
            let publisher = &publisher;
            async move {
                let mut next = pages[0].clone();
                next.generation = 2;
                next.entries[1].owner_epoch = 2;
                next.seal().unwrap();
                publisher
                    .publish(head(2, Some(1), &next), vec![next])
                    .await
                    .unwrap();
                Ok::<_, &'static str>(pages[0].entries[0].owner_epoch)
            }
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls, 1, "unchanged local artifacts must not replay twice");
    assert_eq!(recovered.0.generation, 2);
    assert_eq!(recovered.2, 1);
}

#[tokio::test]
async fn changed_local_epoch_never_reuses_completed_replay_after_deadline() {
    let publisher = ChunkKvRangeCatalogPublisher::new(Arc::new(MemoryChunkKvRangeCatalogStore::default()));
    let first = page(1, 1);
    publisher
        .publish(head(1, None, &first), vec![first])
        .await
        .unwrap();
    let result = publisher
        .recover_current_for_instance(8, std::time::Instant::now(), |_| {
            let publisher = &publisher;
            async move {
                let next = page(2, 2);
                publisher
                    .publish(head(2, Some(1), &next), vec![next])
                    .await
                    .unwrap();
                Ok::<_, &'static str>(1)
            }
        })
        .await;
    assert_eq!(result.unwrap_err(), ChunkKvRangeCatalogError::GenerationConflict);
}
