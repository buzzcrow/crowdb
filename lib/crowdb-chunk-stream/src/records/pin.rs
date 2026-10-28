// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Finite-reader interval leases for ordered Stream chunk records.

use super::{corrupt, storage_error, StreamChunkRecordSnapshot, StreamChunkRecordStore};
use crate::{ChunkId, Result, StreamError};
use arc_swap::ArcSwap;
use bytes::Bytes;
use crowdb_paxos_kv_client::{BatchOp, GetOutcome};
use crowdb_protocol::chunk_stream::{StreamName, StreamProcessLease};
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;

const LEASE_MS: u64 = 60_000;
const RENEW_MS: u64 = 15_000;
pub(super) const PIN_GRACE_MS: u64 = 31_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PinRecord {
    pub(super) id: [u8; 16],
    pub(super) stream: StreamName,
    pub(super) begin: u64,
    pub(super) end: u64,
    pub(super) active: Option<ChunkId>,
    pub(super) expires: u64,
}

pub struct StreamReadPin {
    deadline: Arc<ArcSwap<Instant>>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for StreamReadPin {
    fn drop(&mut self) {
        self.stop.take();
    }
}

impl StreamReadPin {
    pub(crate) fn remaining(&self) -> Duration {
        self.deadline.load().saturating_duration_since(Instant::now())
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn expire_for_tests(&self) {
        self.deadline.store(Arc::new(Instant::now()));
    }
    pub(crate) async fn release(mut self) {
        self.stop.take();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
    /// # Errors
    /// Rejects an expired conservative local lease even if an old task resumes.
    pub fn check(&self) -> Result<()> {
        if Instant::now() >= **self.deadline.load() {
            Err(StreamError::ReadUnavailable("Stream read lease expired".into()))
        } else {
            Ok(())
        }
    }
}

impl StreamChunkRecordStore {
    /// # Errors
    /// Rejects trimmed/stale captures, exhausted pin budgets and unavailable KV.
    pub async fn pin_read(&self, captured: &StreamChunkRecordSnapshot) -> Result<StreamReadPin> {
        let id = StreamProcessLease::generate_boot_nonce()
            .map_err(|error| StreamError::Internal(error.to_string()))?;
        let started = Instant::now();
        let record = PinRecord {
            id,
            stream: captured.record.stream_id,
            begin: captured.record.trim_offset,
            end: captured
                .record
                .logical_end
                .ok_or_else(|| corrupt("pin tail missing"))?,
            active: Some(captured.record.chunk_id),
            expires: now_ms().saturating_add(LEASE_MS),
        };
        self.register_pin(&record).await?;
        let deadline = Arc::new(ArcSwap::from_pointee(
            started + Duration::from_millis(LEASE_MS - 1000),
        ));
        let (stop, receive) = oneshot::channel();
        let task = tokio::spawn(maintain(self.clone(), record, deadline.clone(), receive));
        let pin = StreamReadPin {
            deadline,
            stop: Some(stop),
            task: Some(task),
        };
        pin.check()?;
        Ok(pin)
    }

    async fn register_pin(&self, record: &PinRecord) -> Result<()> {
        for _ in 0..16 {
            let current = self
                .load_tail(record.stream)
                .await?
                .ok_or(StreamError::WriteStalled)?;
            if current.record.trim_offset > record.begin || current.record.logical_end < Some(record.end) {
                return Err(StreamError::ReadUnavailable(
                    "captured Stream interval was retired".into(),
                ));
            }
            let pins = self.read_pins(record.stream).await?;
            let expired: Vec<_> = pins
                .iter()
                .filter(|pin| pin.expires.saturating_add(PIN_GRACE_MS) <= now_ms())
                .take(32)
                .collect();
            if pins.len().saturating_sub(expired.len()) >= 512 {
                return Err(StreamError::Backpressure);
            }
            let mut ops: Vec<_> = expired
                .into_iter()
                .map(|pin| BatchOp::Delete {
                    key: Bytes::from(pin_key(pin)),
                })
                .collect();
            ops.push(pin_put(record)?);
            match self.fenced_batch(&current, ops).await {
                Ok(()) => return Ok(()),
                Err(StreamError::Backpressure) => tokio::task::yield_now().await,
                Err(StreamError::WriteStalled) => {
                    if let GetOutcome::Found { value, .. } =
                        self.backend.get(&pin_key(record)).await.map_err(storage_error)?
                    {
                        let observed: PinRecord =
                            serde_json::from_slice(&value).map_err(|_| corrupt("read pin encoding"))?;
                        if &observed == record {
                            return Ok(());
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(StreamError::Backpressure)
    }

    pub(super) async fn prune_pins(
        &self,
        tail: &StreamChunkRecordSnapshot,
        pins: &[PinRecord],
    ) -> Result<bool> {
        let ops: Vec<_> = pins
            .iter()
            .filter(|pin| pin.expires.saturating_add(PIN_GRACE_MS) <= now_ms())
            .take(32)
            .map(|pin| BatchOp::Delete {
                key: Bytes::from(pin_key(pin)),
            })
            .collect();
        if ops.is_empty() {
            return Ok(false);
        }
        self.fenced_batch(tail, ops).await?;
        Ok(true)
    }

    pub(super) async fn read_pins(&self, stream: StreamName) -> Result<Vec<PinRecord>> {
        let records = self
            .backend
            .scan(&pin_prefix(stream), &[], 512, false)
            .await
            .map_err(storage_error)?;
        if records.truncated {
            return Err(corrupt("read pin budget"));
        }
        records
            .items
            .into_iter()
            .map(|(key, value)| {
                if value.len() > 1024 {
                    return Err(corrupt("read pin length"));
                }
                let pin: PinRecord =
                    serde_json::from_slice(&value).map_err(|_| corrupt("read pin encoding"))?;
                if pin.stream != stream || pin.begin > pin.end || key.as_ref() != pin_key(&pin) {
                    return Err(corrupt("read pin identity"));
                }
                Ok(pin)
            })
            .collect()
    }
}

async fn maintain(
    store: StreamChunkRecordStore,
    mut pin: PinRecord,
    deadline: Arc<ArcSwap<Instant>>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut interval = Duration::from_millis(RENEW_MS);
    let released = loop {
        tokio::select! {
            _ = &mut stop => break true,
            () = tokio::time::sleep(interval) => {}
        }
        let started = Instant::now();
        if started >= **deadline.load() {
            break false;
        }
        interval = Duration::from_millis(100);
        let Some(tail) = store.load_tail(pin.stream).await.ok().flatten() else {
            continue;
        };
        match matches_pin(&store, &pin).await {
            Ok(true) if pin.expires > now_ms() => {}
            Err(_) => continue,
            _ => break false,
        }
        let mut next = pin.clone();
        next.expires = now_ms().saturating_add(LEASE_MS);
        let Ok(op) = pin_put(&next) else { break false };
        let committed = match store.fenced_batch(&tail, vec![op]).await {
            Ok(()) => true,
            Err(StreamError::WriteStalled) => matches_pin(&store, &next).await.unwrap_or(false),
            Err(_) => false,
        };
        if committed {
            if Instant::now() >= **deadline.load() {
                break false;
            }
            pin = next;
            deadline.store(Arc::new(started + Duration::from_millis(LEASE_MS - 1000)));
            interval = Duration::from_millis(RENEW_MS);
        }
    };
    if released {
        release_pin(&store, &pin).await;
    } else {
        deadline.store(Arc::new(Instant::now()));
    }
}

async fn matches_pin(store: &StreamChunkRecordStore, pin: &PinRecord) -> Result<bool> {
    match store.backend.get(&pin_key(pin)).await.map_err(storage_error)? {
        GetOutcome::NotFound => Ok(false),
        GetOutcome::Found { value, .. } => {
            if value.len() > 1024 {
                return Err(corrupt("read pin length"));
            }
            let observed: PinRecord =
                serde_json::from_slice(&value).map_err(|_| corrupt("read pin encoding"))?;
            Ok(&observed == pin)
        }
    }
}

async fn release_pin(store: &StreamChunkRecordStore, pin: &PinRecord) {
    for _ in 0..8 {
        let Ok(Some(tail)) = store.load_tail(pin.stream).await else {
            break;
        };
        match store
            .fenced_batch(
                &tail,
                vec![BatchOp::Delete {
                    key: Bytes::from(pin_key(pin)),
                }],
            )
            .await
        {
            Err(StreamError::Backpressure) => tokio::task::yield_now().await,
            _ => break,
        }
    }
}

pub(super) fn pin_prefix(stream: StreamName) -> Vec<u8> {
    let mut key = vec![crowdb_protocol::key::CROWDB_KEY_MAGIC, 0, 0x1e];
    key.extend_from_slice(&stream.high.to_be_bytes());
    key.extend_from_slice(&stream.low.to_be_bytes());
    key
}
fn pin_key(pin: &PinRecord) -> Vec<u8> {
    let mut key = pin_prefix(pin.stream);
    key.extend_from_slice(&pin.id);
    key
}
fn pin_put(pin: &PinRecord) -> Result<BatchOp> {
    Ok(BatchOp::Put {
        key: Bytes::from(pin_key(pin)),
        value: Bytes::from(serde_json::to_vec(pin).map_err(|_| corrupt("read pin encoding"))?),
    })
}
pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
