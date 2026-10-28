// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One boot's independent Group-0 process-lease renewal.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crowdb_paxos_kv_client::{
    PaxosKvClient, StreamProcessAuthority, StreamProcessContext, StreamProcessLeaseClient,
    StreamProcessLeaseError, StreamProcessLeaseTiming,
};
use crowdb_protocol::{
    chunk_kv::DomainMonitorDescriptor,
    chunk_stream::{StreamProcessLease, StreamPurpose},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::warn;

enum ObservationUpdate {
    Publish {
        endpoint: String,
        observation: crowdb_protocol::common::ChunkKvExtra,
    },
    Withdraw,
    #[cfg(feature = "test-util")]
    Barrier,
}

struct ObservationRequest {
    update: ObservationUpdate,
    context: StreamProcessContext,
    reply: oneshot::Sender<Result<(), StreamProcessLeaseError>>,
}

enum LeaseUpdate {
    Renewal(Result<StreamProcessLease, StreamProcessLeaseError>),
    Observation { fenced: bool },
}

pub struct ProcessLeaseSession {
    authority: Arc<StreamProcessAuthority>,
    observations: mpsc::Sender<ObservationRequest>,
    renewal: JoinHandle<()>,
}

impl ProcessLeaseSession {
    /// Claim once from a fixed predecessor and renew independently of recovery/data IO.
    /// # Errors
    /// Returns invalid policy, a changed predecessor or an unavailable lease.
    pub async fn start(
        kv: Arc<PaxosKvClient>,
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
        // Renewal and discovery mutate the same lease record. One metadata
        // writer prevents their periodic ticks spending each other's CAS
        // retry budget while a healthy, slower commit is still in flight.
        let (observations, requests) = mpsc::channel(1);
        let renewal = tokio::spawn(renew(
            Arc::clone(&client),
            Arc::clone(&authority),
            policy.lease_duration_ms,
            policy.heartbeat_interval_ms,
            requests,
        ));
        Ok(Self {
            authority,
            observations,
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
        self.submit_observation(ObservationUpdate::Publish {
            endpoint: endpoint.into(),
            observation: observation.clone(),
        })
        .await
    }

    /// Remove this boot's discovery entry while retaining its exclusion boundary.
    /// # Errors
    /// Fails when the lease is expired, replaced or the conditional write fails.
    pub async fn withdraw_observation(&self) -> Result<(), StreamProcessLeaseError> {
        self.submit_observation(ObservationUpdate::Withdraw).await
    }

    async fn submit_observation(&self, update: ObservationUpdate) -> Result<(), StreamProcessLeaseError> {
        let context = self.authority.capture(Instant::now())?;
        let (reply, response) = oneshot::channel();
        let publication = async {
            self.observations
                .send(ObservationRequest {
                    update,
                    context,
                    reply,
                })
                .await
                .map_err(|_| StreamProcessLeaseError::OutcomeUnknown)?;
            response
                .await
                .map_err(|_| StreamProcessLeaseError::OutcomeUnknown)?
        };
        let result = tokio::time::timeout_at(context.deadline().into(), publication)
            .await
            .map_err(|_| StreamProcessLeaseError::Expired)?;
        self.authority.validate_context(&context, Instant::now())?;
        result
    }

    #[cfg(feature = "test-util")]
    /// Wait until earlier queued observation requests have been handled.
    /// # Errors
    /// Returns process fencing or an unavailable coordinator.
    pub async fn flush_observations_for_tests(&self) -> Result<(), StreamProcessLeaseError> {
        self.submit_observation(ObservationUpdate::Barrier).await
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
    mut observations: mpsc::Receiver<ObservationRequest>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    let mut observation_turn = false;
    while let Ok(context) = authority.capture(Instant::now()) {
        let update = async {
            // A slow renewal can leave every next tick overdue. Give one
            // already queued observation its turn before checking that tick.
            if observation_turn {
                if let Ok(request) = observations.try_recv() {
                    return LeaseUpdate::Observation {
                        fenced: apply_observation(&client, &authority, request).await,
                    };
                }
            }
            tokio::select! {
                biased;
                _ = interval.tick() => LeaseUpdate::Renewal(
                    client.renew(context.identity(), wall_ms(), lease_ms).await
                ),
                Some(request) = observations.recv() => LeaseUpdate::Observation {
                    fenced: apply_observation(&client, &authority, request).await,
                },
            }
        };
        let result = tokio::time::timeout_at(context.deadline().into(), update).await;
        observation_turn = matches!(&result, Ok(LeaseUpdate::Renewal(_)));
        match result {
            Ok(LeaseUpdate::Observation { fenced: false }) => {}
            Ok(LeaseUpdate::Observation { fenced: true }) => break,
            Ok(LeaseUpdate::Renewal(Ok(record))) => {
                if let Err(error) = authority.observe(record, wall_ms(), Instant::now()) {
                    warn!(%error, "process lease renewal lost local authority");
                    break;
                }
            }
            Ok(LeaseUpdate::Renewal(Err(
                error @ (StreamProcessLeaseError::Replaced
                | StreamProcessLeaseError::Expired
                | StreamProcessLeaseError::Invalid(_)),
            ))) => {
                warn!(%error, "process lease is fenced; restart required");
                break;
            }
            Ok(LeaseUpdate::Renewal(Err(error))) => {
                warn!(%error, "process lease renewal unavailable; retaining original deadline");
            }
            Err(_) => {
                warn!("process lease renewal reached its original safety deadline");
                break;
            }
        }
    }
    authority.fence();
}

async fn apply_observation(
    client: &StreamProcessLeaseClient,
    authority: &StreamProcessAuthority,
    request: ObservationRequest,
) -> bool {
    if request.reply.is_closed() {
        return false;
    }
    let context = request.context;
    if Instant::now() >= context.deadline() {
        let _ = request.reply.send(Err(StreamProcessLeaseError::Expired));
        return false;
    }
    let publication = async {
        match request.update {
            ObservationUpdate::Publish {
                endpoint,
                observation,
            } => {
                client
                    .publish_chunk_kv_observation(context.identity(), &endpoint, &observation, wall_ms())
                    .await
            }
            ObservationUpdate::Withdraw => {
                client
                    .withdraw_chunk_kv_observation(context.identity(), wall_ms())
                    .await
            }
            #[cfg(feature = "test-util")]
            ObservationUpdate::Barrier => Ok(()),
        }
    };
    let Ok(result) = tokio::time::timeout_at(context.deadline().into(), publication).await else {
        let _ = request.reply.send(Err(StreamProcessLeaseError::OutcomeUnknown));
        return false;
    };
    let fenced = matches!(
        &result,
        Err(StreamProcessLeaseError::Replaced | StreamProcessLeaseError::Expired)
    );
    if fenced {
        authority.fence();
    }
    let _ = request.reply.send(result);
    fenced
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
