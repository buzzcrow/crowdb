// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One backend-owned sampler publishes immutable, expiring health evidence.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwapOption;
use crowdb_access_s3::metrics::{DependencyHealth, S3HealthSnapshot};
use tokio::time::{Instant, MissedTickBehavior};

use super::super::{dependencies, S3Dispatcher, StorageReadiness};

pub(super) const MAX_AGE: Duration = Duration::from_secs(3);

#[derive(Default)]
pub(in crate::s3) struct HealthObservations {
    pub(super) latest: ArcSwapOption<HealthObservation>,
    running: AtomicBool,
}

pub(super) struct HealthObservation {
    pub health: S3HealthSnapshot,
    pub details: serde_json::Map<String, serde_json::Value>,
    pub started: Instant,
    pub checked_at_unix_ms: u64,
    pub generation: u64,
}

/// Owns the background sampler; dropping it cancels in-flight observation work.
pub struct HealthObserver(tokio::task::JoinHandle<()>);

impl Drop for HealthObserver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct SamplingLease(Arc<HealthObservations>);

impl Drop for SamplingLease {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::Release);
    }
}

impl S3Dispatcher {
    /// Starts one sampler independently of health and business requests.
    ///
    /// # Errors
    /// Rejects another sampler while an existing worker or its cancellation is active.
    pub fn start_health_observer(self: &Arc<Self>) -> Result<HealthObserver, &'static str> {
        self.observations
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| "Access health sampler is already running")?;
        let dispatcher = Arc::clone(self);
        let lease = SamplingLease(Arc::clone(&self.observations));
        Ok(HealthObserver(tokio::spawn(async move {
            let _lease = lease;
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let observation = dispatcher.sample_health().await;
                dispatcher.observations.latest.store(Some(Arc::new(observation)));
            }
        })))
    }

    async fn sample_health(&self) -> HealthObservation {
        let started = Instant::now();
        let checked_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            });
        let observed = tokio::time::timeout(
            Duration::from_millis(800),
            Arc::clone(&self.operations).observe_dependencies(),
        )
        .await
        .unwrap_or_else(|_| {
            Some(StorageReadiness {
                metadata: Err("Dependency observation exceeded its 800 ms deadline".into()),
                chunks: Err("Dependency observation exceeded its 800 ms deadline".into()),
            })
        });
        let mut health = self
            .health
            .snapshot(&self.metrics, self.body_allocator.as_deref());
        let mut details = serde_json::Map::new();
        if let Some(observed) = observed {
            health.readiness.metadata = dependencies::health(&observed.metadata);
            health.readiness.chunks = dependencies::health(&observed.chunks);
            for (name, result) in [("metadata", observed.metadata), ("chunks", observed.chunks)] {
                let detail = match result {
                    Ok(detail) | Err(detail) => detail,
                };
                details.insert(name.into(), serde_json::Value::String(detail));
            }
        }
        if let Some(ready) = self.authenticator.is_ready() {
            health.readiness.authentication = if ready {
                DependencyHealth::Ready
            } else {
                DependencyHealth::Unavailable
            };
        }
        let generation = self
            .observations
            .latest
            .load()
            .as_ref()
            .map_or(1, |last| last.generation + 1);
        HealthObservation {
            health,
            details,
            started,
            checked_at_unix_ms,
            generation,
        }
    }

    /// Latest completed backend sampling generation, without causing any work.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn health_generation_for_tests(&self) -> u64 {
        self.observations
            .latest
            .load()
            .as_ref()
            .map_or(0, |last| last.generation)
    }
}
