// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Diagnostic publication is separate from topology and cannot prevent progress.

use crate::group0_control_plane::Group0ControlPlane;
use bytes::Bytes;
use crowdb_protocol::chunk_kv::balance::{BalanceObservation, MAX_OBSERVATION_BYTES, OBSERVATION_KEY};

pub(super) async fn publish(control: &Group0ControlPlane, observation: &BalanceObservation) {
    if let Err(error) = persist(control, observation).await {
        tracing::warn!(%error, "balance diagnostic observation unavailable");
    }
}

async fn persist(control: &Group0ControlPlane, observation: &BalanceObservation) -> Result<(), String> {
    let encoded = serde_json::to_vec(observation).map_err(|error| error.to_string())?;
    if encoded.len() > MAX_OBSERVATION_BYTES {
        return Err("balance diagnostic byte budget exceeded".into());
    }
    let current = control
        .get(OBSERVATION_KEY.as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    if let Some(prior) = current
        .value
        .as_deref()
        .and_then(|value| serde_json::from_slice::<BalanceObservation>(value).ok())
    {
        let mut comparable = observation.clone();
        comparable.observed_at_ms = prior.observed_at_ms;
        if comparable == prior
            && observation.observed_at_ms.saturating_sub(prior.observed_at_ms) < observation.valid_for_ms / 2
        {
            return Ok(());
        }
    }
    control
        .compare_and_put(
            Bytes::from_static(OBSERVATION_KEY.as_bytes()),
            Bytes::from(encoded),
            current.revision,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}
