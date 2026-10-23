// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One boot's independent Group-0 process-lease renewal.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crowdb_kv_client::{
    CrowdbKvClient, StreamProcessAuthority, StreamProcessLeaseClient, StreamProcessLeaseError,
    StreamProcessLeaseTiming,
};
use crowdb_protocol::{
    chunk_kv::DomainMonitorDescriptor,
    chunk_stream::{StreamProcessLease, StreamPurpose},
};
use tokio::task::JoinHandle;
use tracing::warn;

pub struct ProcessLeaseSession {
    authority: Arc<StreamProcessAuthority>,
    client: Arc<StreamProcessLeaseClient>,
    renewal: JoinHandle<()>,
}

impl ProcessLeaseSession {
    /// Claim once from a fixed predecessor and renew independently of recovery/data IO.
    /// # Errors
    /// Returns invalid policy, a changed predecessor or an unavailable lease.
    pub async fn start(
        kv: Arc<CrowdbKvClient>,
        instance_id: u64,
        policy: &DomainMonitorDescriptor,
    ) -> Result<Self, StreamProcessLeaseError> {
        policy
            .validate()
            .map_err(|error| StreamProcessLeaseError::Invalid(error.to_string()))?;
        let client = Arc::new(StreamProcessLeaseClient::new(kv));
        let nonce = StreamProcessLease::generate_boot_nonce()
            .map_err(|error| StreamProcessLeaseError::Invalid(error.to_string()))?;
        let predecessor = client
            .load(StreamPurpose::Wal, instance_id)
            .await?
            .map(|record| record.identity);
        let record = client
            .claim(
                StreamPurpose::Wal,
                instance_id,
                nonce,
                predecessor,
                wall_ms(),
                policy.lease_duration_ms,
            )
            .await?;
        let authority = Arc::new(StreamProcessAuthority::new(
            record,
            StreamProcessLeaseTiming {
                max_clock_skew_ms: policy.max_clock_skew_ms,
                self_fence_margin_ms: policy.self_fence_margin_ms,
            },
            wall_ms(),
            Instant::now(),
        )?);
        let renewal = tokio::spawn(renew(
            Arc::clone(&client),
            Arc::clone(&authority),
            policy.lease_duration_ms,
            policy.heartbeat_interval_ms,
        ));
        Ok(Self {
            authority,
            client,
            renewal,
        })
    }

    #[must_use]
    pub fn authority(&self) -> Arc<StreamProcessAuthority> {
        Arc::clone(&self.authority)
    }

    /// Publish a heartbeat from this exact live boot.
    /// # Errors
    /// Propagates process fencing and conditional metadata errors.
    pub async fn publish_observation(
        &self,
        endpoint: &str,
        observation: &crowdb_protocol::common::ChunkKvExtra,
    ) -> Result<(), StreamProcessLeaseError> {
        let context = self.authority.capture(Instant::now())?;
        let result = self
            .client
            .publish_chunk_kv_observation(context.identity(), endpoint, observation, wall_ms())
            .await;
        if matches!(
            &result,
            Err(StreamProcessLeaseError::Replaced | StreamProcessLeaseError::Expired)
        ) {
            self.authority.fence();
        }
        result?;
        self.authority.validate_context(&context, Instant::now())
    }

    /// Remove this boot's discovery entry while retaining its exclusion boundary.
    /// # Errors
    /// Fails when the lease is expired, replaced or the conditional write fails.
    pub async fn withdraw_observation(&self) -> Result<(), StreamProcessLeaseError> {
        let context = self.authority.capture(Instant::now())?;
        self.client
            .withdraw_chunk_kv_observation(context.identity(), wall_ms())
            .await
    }

    /// Wake process shutdown after permanent expiry or confirmed replacement.
    pub async fn wait_for_fence(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        while self.authority.capture(Instant::now()).is_ok() {
            interval.tick().await;
        }
    }
}

impl Drop for ProcessLeaseSession {
    fn drop(&mut self) {
        self.authority.fence();
        self.renewal.abort();
    }
}

async fn renew(
    client: Arc<StreamProcessLeaseClient>,
    authority: Arc<StreamProcessAuthority>,
    lease_ms: u64,
    interval_ms: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    while let Ok(context) = authority.capture(Instant::now()) {
        let renewal = async {
            interval.tick().await;
            client.renew(context.identity(), wall_ms(), lease_ms).await
        };
        let result = tokio::time::timeout_at(context.deadline().into(), renewal).await;
        match result {
            Ok(Ok(record)) => {
                if let Err(error) = authority.observe(record, wall_ms(), Instant::now()) {
                    warn!(%error, "process lease renewal lost local authority");
                    break;
                }
            }
            Ok(Err(
                error @ (StreamProcessLeaseError::Replaced
                | StreamProcessLeaseError::Expired
                | StreamProcessLeaseError::Invalid(_)),
            )) => {
                warn!(%error, "process lease is fenced; restart required");
                break;
            }
            Ok(Err(error)) => warn!(%error, "process lease renewal unavailable; retaining original deadline"),
            Err(_) => {
                warn!("process lease renewal reached its original safety deadline");
                break;
            }
        }
    }
    authority.fence();
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
