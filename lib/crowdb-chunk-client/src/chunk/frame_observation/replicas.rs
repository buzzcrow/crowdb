// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::frame::FrameScanConfig;
use std::sync::Arc;
use tokio::{task::JoinSet, time::Instant};

use super::{
    observe_frame_prefix, FrameObservation, FrameObservationError, FrameObservationResult, FrameWindowReader,
    MAX_FRAME_OBSERVATION_REPLICAS,
};

#[derive(Debug)]
pub struct FrameReplicaSelection {
    selected_index: usize,
    selected: FrameObservation,
    healthy_replicas: usize,
    failures: Vec<String>,
}

impl FrameReplicaSelection {
    #[must_use]
    pub const fn selected_index(&self) -> usize {
        self.selected_index
    }
    #[must_use]
    pub const fn observation(&self) -> &FrameObservation {
        &self.selected
    }
    #[must_use]
    pub const fn healthy_replicas(&self) -> usize {
        self.healthy_replicas
    }
    #[must_use]
    pub fn failures(&self) -> &[String] {
        &self.failures
    }
}

/// Observes available replicas concurrently, discards incomplete/failed
/// observations, and compares their shortest complete prefix without arbitration.
/// The first half of the remaining budget bounds observation; comparison ends
/// after three quarters, reserving the rest for replay under the same deadline.
pub async fn select_frame_prefix(
    readers: &[Arc<dyn FrameWindowReader>],
    config: FrameScanConfig,
    window_bytes: u32,
    deadline: Instant,
) -> FrameObservationResult<FrameReplicaSelection> {
    if readers.is_empty() || readers.len() > MAX_FRAME_OBSERVATION_REPLICAS {
        return Err(FrameObservationError::InvalidRequest);
    }
    let start = Instant::now();
    if start >= deadline {
        return Err(FrameObservationError::DeadlineExpired);
    }
    let remaining = deadline - start;
    let observation_deadline = start + remaining / 2;
    let comparison_deadline = deadline - remaining / 4;
    let mut tasks = JoinSet::new();
    for (index, reader) in readers.iter().enumerate() {
        let reader = Arc::clone(reader);
        tasks.spawn(async move {
            (
                index,
                observe_frame_prefix(reader.as_ref(), config, window_bytes, observation_deadline).await,
            )
        });
    }
    let mut healthy = Vec::new();
    let mut failures = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (index, observation) = result.map_err(|error| FrameObservationError::Task(error.to_string()))?;
        match observation {
            Ok(observation) => healthy.push((index, observation)),
            Err(error) => failures.push(format!("replica {index}: {error}")),
        }
    }
    let Some((selected_index, selected)) = healthy
        .iter()
        .min_by_key(|(index, observation)| (observation.summary.physical_end, *index))
        .copied()
    else {
        return Err(FrameObservationError::NoHealthyReplica { failures });
    };
    let comparison_failures =
        compare_prefixes(readers, &healthy, selected_index, selected, comparison_deadline).await?;
    let healthy_replicas = healthy.len() - comparison_failures.len();
    failures.extend(comparison_failures);
    Ok(FrameReplicaSelection {
        selected_index,
        selected,
        healthy_replicas,
        failures,
    })
}

async fn compare_prefixes(
    readers: &[Arc<dyn FrameWindowReader>],
    healthy: &[(usize, FrameObservation)],
    selected_index: usize,
    selected: FrameObservation,
    deadline: Instant,
) -> FrameObservationResult<Vec<String>> {
    let mut tasks = JoinSet::new();
    for &(index, observation) in healthy {
        if observation.summary.physical_end == selected.summary.physical_end {
            check_match(selected_index, selected, index, observation)?;
            continue;
        }
        let reader = Arc::clone(&readers[index]);
        let mut config = observation.config;
        config.physical_limit = selected.summary.physical_end;
        config.required_end = config.physical_limit;
        tasks.spawn(async move {
            (
                index,
                observe_frame_prefix(reader.as_ref(), config, observation.window_bytes, deadline).await,
            )
        });
    }
    let mut failures = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (index, observation) = result.map_err(|error| FrameObservationError::Task(error.to_string()))?;
        match observation {
            Ok(observation) => check_match(selected_index, selected, index, observation)?,
            Err(error) => failures.push(format!("replica {index} comparison: {error}")),
        }
    }
    Ok(failures)
}

fn check_match(
    first: usize,
    expected: FrameObservation,
    second: usize,
    actual: FrameObservation,
) -> FrameObservationResult<()> {
    if expected.summary.digest != actual.summary.digest
        || expected.summary.logical_end != actual.summary.logical_end
    {
        return Err(FrameObservationError::ConflictingPrefixes {
            first,
            second,
            physical_end: expected.summary.physical_end,
        });
    }
    Ok(())
}
