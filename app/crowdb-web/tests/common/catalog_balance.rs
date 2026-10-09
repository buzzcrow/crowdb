// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::CrowdbKvClient;
use crowdb_protocol::chunk_kv::{
    balance::{
        BalanceObservation, BalanceOwnerObservation, BalancePartitionObservation, BalanceWeights,
        OBSERVATION_KEY,
    },
    ChunkKvRangeBalancePolicy, ChunkKvRangeCatalogPage,
};

pub async fn seed(kv: &CrowdbKvClient, page: &ChunkKvRangeCatalogPage) {
    let weights = BalanceWeights::new(&[(105, 1_050)], 80).unwrap();
    let first = &page.entries[0];
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let observation = BalanceObservation {
        policy_version: crowdb_protocol::chunk_kv::balance::POLICY_VERSION,
        catalog_generation: 9,
        observed_at_ms: now,
        valid_for_ms: 6_000,
        policy: ChunkKvRangeBalancePolicy::default(),
        reason: "within tolerance".into(),
        deviation_percent_millionths: 0,
        loss_millionths: Some(0),
        owners: vec![BalanceOwnerObservation {
            instance_id: first.owner.instance_id,
            rpc_endpoint: first.owner.rpc_endpoint.clone(),
            partition_count: 105,
            estimated_bytes: 1_050,
            weight: weights.weight(105, 1_050),
        }],
        partitions: page
            .entries
            .iter()
            .map(|entry| BalancePartitionObservation {
                partition_id: entry.partition_id,
                instance_id: entry.owner.instance_id,
                owner_epoch: entry.owner_epoch,
                estimated_bytes: 10,
                weight: weights.weight(1, 10),
                reason: "within tolerance".into(),
            })
            .collect(),
        candidate: None,
    };
    kv.put(
        0,
        0,
        OBSERVATION_KEY.as_bytes(),
        &serde_json::to_vec(&observation).unwrap(),
        None,
    )
    .await
    .unwrap();
}
