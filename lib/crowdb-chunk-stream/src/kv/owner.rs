// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 owner assignment and independent process-lease renewal.

use super::{kv_error, KvStreamRegistry};
use crate::{Result, StreamError, StreamName, StreamOwnerAuthority, StreamWriterSession};
use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use crowdb_kv_client::{
    CrowdbKvClient, GetOutcome, ReadMode, StreamProcessAuthority, StreamProcessLeaseClient,
    StreamProcessLeaseError, StreamProcessLeaseTiming,
};
use crowdb_protocol::{
    chunk_stream::{StreamProcessLease, StreamPurpose, StreamWriterOwner},
    key::{StreamWriterOwnerKey, TextKey},
    timing::{MAX_WRITE_CLOCK_SKEW_MS, MAX_WRITE_REQUEST_AGE_MS},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const LEASE_MS: u64 = 15_000;

pub(super) struct ProcessOwner {
    authority: Arc<StreamProcessAuthority>,
    renewal: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ProcessOwner {
    fn drop(&mut self) {
        if let Some(task) = &self.renewal {
            self.authority.fence();
            task.abort();
        }
    }
}

pub(super) struct OwnerProcesses {
    slots: [ArcSwapOption<ProcessOwner>; 2],
    starting: AtomicBool,
}

impl Default for OwnerProcesses {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| ArcSwapOption::empty()),
            starting: AtomicBool::new(false),
        }
    }
}

struct Starting<'a>(&'a AtomicBool);
impl Drop for Starting<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl KvStreamRegistry {
    /// Compose WAL ownership with the server's already-renewed process scope.
    /// # Errors
    /// Rejects dead authority or a runtime already bound to a process.
    pub fn bind_owner_process(&self, authority: Arc<StreamProcessAuthority>) -> Result<()> {
        let context = authority
            .capture(Instant::now())
            .map_err(|_| StreamError::StaleWriter)?;
        let slot = &self.processes.slots[usize::from(context.identity().purpose == StreamPurpose::Wal)];
        let owner = Arc::new(ProcessOwner {
            authority,
            renewal: None,
        });
        if slot
            .compare_and_swap(&None::<Arc<ProcessOwner>>, Some(owner))
            .is_some()
        {
            return Err(StreamError::InvalidRequest(
                "stream runtime already has a process owner".into(),
            ));
        }
        Ok(())
    }

    async fn owner_process(&self, purpose: StreamPurpose) -> Result<Arc<ProcessOwner>> {
        let slot = &self.processes.slots[usize::from(purpose == StreamPurpose::Wal)];
        if let Some(owner) = slot.load_full() {
            return Ok(owner);
        }
        if self
            .processes
            .starting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(StreamError::Backpressure);
        }
        let _starting = Starting(&self.processes.starting);
        if let Some(owner) = slot.load_full() {
            return Ok(owner);
        }
        let nonce =
            StreamProcessLease::generate_boot_nonce().map_err(|e| StreamError::Internal(e.to_string()))?;
        let instance = u64::from_le_bytes(nonce[..8].try_into().expect("nonce half")).max(1);
        let client = Arc::new(StreamProcessLeaseClient::new(self.kv.clone()));
        let lease = client
            .claim(purpose, instance, nonce, None, wall_ms(), LEASE_MS)
            .await
            .map_err(|e| StreamError::ReadUnavailable(e.to_string()))?;
        let authority = Arc::new(
            StreamProcessAuthority::new(
                lease,
                StreamProcessLeaseTiming::default(),
                wall_ms(),
                Instant::now(),
            )
            .map_err(|e| StreamError::ReadUnavailable(e.to_string()))?,
        );
        let renewal = tokio::spawn(renew_process(client, authority.clone()));
        let owner = Arc::new(ProcessOwner {
            authority,
            renewal: Some(renewal),
        });
        slot.store(Some(owner.clone()));
        Ok(owner)
    }

    pub(super) async fn acquire_owner(
        &self,
        name: StreamName,
        epoch: u64,
        purpose: StreamPurpose,
    ) -> Result<Arc<StreamWriterSession>> {
        let process = self.owner_process(purpose).await?;
        let context = process
            .authority
            .capture(Instant::now())
            .map_err(|_| StreamError::StaleWriter)?;
        let previous = read_owner(&self.kv, name).await?;
        if let Some((owner, _)) = &previous {
            if owner.process.purpose != purpose {
                return Err(StreamError::Corruption(
                    "owner purpose differs from stream record".into(),
                ));
            }
            if epoch < owner.writer_epoch || (!owner.released && owner.process == context.identity()) {
                return Err(StreamError::StaleWriter);
            }
            if !owner.released {
                exclude_previous(&self.kv, &process.authority, owner).await?;
            }
        }
        process
            .authority
            .capture(Instant::now())
            .map_err(|_| StreamError::StaleWriter)?;
        let owner = StreamWriterOwner {
            stream_name: name,
            writer_epoch: epoch,
            process: context.identity(),
            assignment_nonce: StreamProcessLease::generate_boot_nonce()
                .map_err(|error| StreamError::Internal(error.to_string()))?,
            released: false,
        };
        let revision = previous.as_ref().map_or(0, |(_, revision)| *revision);
        write_owner(&self.kv, &owner, revision).await?;
        Ok(StreamWriterSession::new(Arc::new(AssignedOwner {
            kv: self.kv.clone(),
            process,
            owner,
        })))
    }
}

struct AssignedOwner {
    kv: Arc<CrowdbKvClient>,
    process: Arc<ProcessOwner>,
    owner: StreamWriterOwner,
}

#[async_trait]
impl StreamOwnerAuthority for AssignedOwner {
    fn deadline(&self) -> Result<Instant> {
        self.process
            .authority
            .capture(Instant::now())
            .map(|context| context.deadline())
            .map_err(|_| StreamError::StaleWriter)
    }
    async fn release(&self) -> Result<()> {
        let Some((mut current, revision)) = read_owner(&self.kv, self.owner.stream_name).await? else {
            return Err(StreamError::StaleWriter);
        };
        if current.process != self.owner.process
            || current.writer_epoch != self.owner.writer_epoch
            || current.assignment_nonce != self.owner.assignment_nonce
        {
            return Err(StreamError::StaleWriter);
        }
        if current.released {
            return Ok(());
        }
        current.released = true;
        write_owner(&self.kv, &current, revision).await
    }
}

async fn exclude_previous(
    kv: &Arc<CrowdbKvClient>,
    current: &StreamProcessAuthority,
    owner: &StreamWriterOwner,
) -> Result<()> {
    let client = StreamProcessLeaseClient::new(kv.clone());
    loop {
        current
            .capture(Instant::now())
            .map_err(|_| StreamError::StaleWriter)?;
        let previous = client
            .load(owner.process.purpose, owner.process.instance_id)
            .await
            .map_err(|e| StreamError::ReadUnavailable(e.to_string()))?
            .ok_or_else(|| StreamError::Corruption("stream owner process record is missing".into()))?;
        let expiry = if previous.identity == owner.process {
            previous.expires_at_ms.max(previous.grant_expires_at_ms)
        } else if previous.identity.process_epoch > owner.process.process_epoch {
            previous.prior_exclusion_deadline_ms
        } else {
            return Err(StreamError::StaleWriter);
        };
        let safe = expiry
            .saturating_add(MAX_WRITE_CLOCK_SKEW_MS)
            .saturating_add(MAX_WRITE_REQUEST_AGE_MS);
        if wall_ms() >= safe {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis((safe - wall_ms()).min(500))).await;
    }
}

async fn read_owner(kv: &CrowdbKvClient, name: StreamName) -> Result<Option<(StreamWriterOwner, u64)>> {
    let key = StreamWriterOwnerKey { stream_name: name }.to_path();
    match kv
        .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
        .await
        .map_err(|error| kv_error(&error))?
    {
        GetOutcome::NotFound => Ok(None),
        GetOutcome::Found { value, revision } => {
            let owner: StreamWriterOwner =
                serde_json::from_slice(&value).map_err(|e| StreamError::Corruption(e.to_string()))?;
            if owner.stream_name != name || owner.validate().is_err() {
                return Err(StreamError::Corruption("invalid stream owner assignment".into()));
            }
            Ok(Some((owner, revision)))
        }
    }
}

async fn write_owner(kv: &CrowdbKvClient, owner: &StreamWriterOwner, revision: u64) -> Result<()> {
    let key = StreamWriterOwnerKey {
        stream_name: owner.stream_name,
    }
    .to_path();
    let value = serde_json::to_vec(owner).map_err(|e| StreamError::Internal(e.to_string()))?;
    match kv.put_cas(0, 0, key.as_bytes(), &value, revision).await {
        Ok(_) => Ok(()),
        Err(error) => {
            if read_owner(kv, owner.stream_name)
                .await?
                .as_ref()
                .is_some_and(|(current, _)| current == owner)
            {
                Ok(())
            } else {
                Err(kv_error(&error))
            }
        }
    }
}

async fn renew_process(client: Arc<StreamProcessLeaseClient>, authority: Arc<StreamProcessAuthority>) {
    while let Ok(context) = authority.capture(Instant::now()) {
        tokio::time::sleep(Duration::from_millis(LEASE_MS / 3)).await;
        let renewal = client.renew(context.identity(), wall_ms(), LEASE_MS);
        match tokio::time::timeout_at(context.deadline().into(), renewal).await {
            Ok(Ok(lease)) => {
                if authority.observe(lease, wall_ms(), Instant::now()).is_err() {
                    break;
                }
            }
            Ok(Err(StreamProcessLeaseError::Replaced | StreamProcessLeaseError::Expired)) => break,
            Ok(Err(error)) => {
                tracing::warn!(%error, "stream process renewal unavailable; retaining deadline");
            }
            _ => break,
        }
    }
    authority.fence();
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| u64::try_from(time.as_millis()).unwrap_or(u64::MAX))
}
