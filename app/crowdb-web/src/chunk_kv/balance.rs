// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Read the monitor's bounded diagnostic snapshot without scanning catalog/data.

use crowdb_kv_client::{CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::chunk_kv::{
    balance::{BalanceObservation, MAX_OBSERVATION_BYTES, OBSERVATION_KEY},
    ChunkKvRangeCatalogEntry,
};
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) struct Observation {
    value: Option<BalanceObservation>,
    reason: String,
}

pub(super) async fn observe(kv: &CrowdbKvClient, generation: u64) -> Observation {
    match load(kv, generation).await {
        Ok(value) => Observation {
            reason: value.reason.clone(),
            value: Some(value),
        },
        Err(reason) => Observation { value: None, reason },
    }
}

async fn load(kv: &CrowdbKvClient, generation: u64) -> Result<BalanceObservation, String> {
    let outcome = kv
        .get(0, 0, OBSERVATION_KEY.as_bytes(), ReadMode::Linearizable, None)
        .await
        .map_err(|error| error.to_string())?;
    let GetOutcome::Found { value, .. } = outcome else {
        return Err("Balance observation unavailable".into());
    };
    if value.len() > MAX_OBSERVATION_BYTES {
        return Err("Balance observation exceeds byte budget".into());
    }
    let result: BalanceObservation =
        serde_json::from_slice(&value).map_err(|_| "Invalid balance observation".to_string())?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| u64::try_from(time.as_millis()).unwrap_or(u64::MAX));
    if result.catalog_generation != generation {
        return Err("Stale balance catalog generation".into());
    }
    if result.observed_at_ms > now_ms.saturating_add(1_000)
        || now_ms.saturating_sub(result.observed_at_ms) > result.valid_for_ms
    {
        return Err("Stale balance observation".into());
    }
    if result.policy_version != crowdb_protocol::chunk_kv::balance::POLICY_VERSION
        || result.policy.validate().is_err()
    {
        return Err("Invalid balance policy observation".into());
    }
    Ok(result)
}

pub(super) fn partition(observation: &Observation, entry: &ChunkKvRangeCatalogEntry) -> Value {
    let Some(value) = &observation.value else {
        return Value::Null;
    };
    let Some(partition) = value.partitions.iter().find(|partition| {
        partition.partition_id == entry.partition_id
            && partition.instance_id == entry.owner.instance_id
            && partition.owner_epoch == entry.owner_epoch
    }) else {
        return Value::Null;
    };
    let Some(owner) = value
        .owners
        .iter()
        .find(|owner| owner.instance_id == entry.owner.instance_id)
    else {
        return Value::Null;
    };
    let mut result = json!({"partition":partition,"owner":owner});
    super::exact_integers(&mut result);
    result
}

pub(super) fn summary(observation: &Observation) -> Value {
    let Some(value) = &observation.value else {
        return json!({"reason":observation.reason});
    };
    let mut result = json!({"reason":value.reason,"policy_version":value.policy_version,"generation":value.catalog_generation,
        "observed_at_ms":value.observed_at_ms,"valid_for_ms":value.valid_for_ms,"policy":value.policy,
        "deviation_percent_millionths":value.deviation_percent_millionths,"loss_millionths":value.loss_millionths,"candidate":value.candidate,
        "owners":value.owners});
    super::exact_integers(&mut result);
    result
}
