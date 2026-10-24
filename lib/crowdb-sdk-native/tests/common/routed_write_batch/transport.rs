// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use super::TestFailure;
use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    ChunkKvRangeCatalogSource, ChunkKvRpcTransport, ChunkKvTransport, Group0ChunkKvRangeCatalogSource, Result,
};
use crowdb_protocol::chunk_kv::{
    BatchMutationRequest, BatchMutationResponse, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage,
    ChunkKvResponse, PointOperation, PointRequest,
};
use serde_json::{json, Value};

pub struct TestProcessSample {
    user_ticks: u64,
    system_ticks: u64,
    ticks_per_second: usize,
    rss_kib: u64,
    cumulative_hwm_kib: u64,
}

impl TestProcessSample {
    pub fn capture() -> std::result::Result<Self, TestFailure> {
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let fields: Vec<_> = stat
            .get(stat.rfind(')').ok_or("missing process comm terminator")? + 1..)
            .ok_or("missing process stat fields")?
            .split_whitespace()
            .collect();
        // After comm, index zero is field 3; fields 14 and 15 are CPU ticks.
        let user_ticks = fields.get(11).ok_or("missing process user ticks")?.parse()?;
        let system_ticks = fields.get(12).ok_or("missing process system ticks")?.parse()?;
        let status = std::fs::read_to_string("/proc/self/status")?;
        let auxv = std::fs::read("/proc/self/auxv")?;
        let width = std::mem::size_of::<usize>();
        let mut ticks_per_second = None;
        for pair in auxv.chunks_exact(2 * width) {
            // Linux AT_CLKTCK is 17; the auxiliary vector stores native words.
            if usize::from_ne_bytes(pair[..width].try_into()?) == 17 {
                ticks_per_second = Some(usize::from_ne_bytes(pair[width..].try_into()?));
                break;
            }
        }
        let ticks_per_second = ticks_per_second
            .filter(|ticks| *ticks > 0)
            .ok_or("process clock tick frequency unavailable")?;
        Ok(Self {
            user_ticks,
            system_ticks,
            ticks_per_second,
            rss_kib: status_kib(&status, "VmRSS:")?,
            cumulative_hwm_kib: status_kib(&status, "VmHWM:")?,
        })
    }

    pub fn delta(&self, after: &Self) -> std::result::Result<Value, TestFailure> {
        if self.ticks_per_second != after.ticks_per_second {
            return Err("process clock frequency changed".into());
        }
        let user = after
            .user_ticks
            .checked_sub(self.user_ticks)
            .ok_or("process user ticks decreased")?;
        let system = after
            .system_ticks
            .checked_sub(self.system_ticks)
            .ok_or("process system ticks decreased")?;
        Ok(json!({"user_cpu_ticks":user,"system_cpu_ticks":system,
            "clock_ticks_per_second":self.ticks_per_second,
            "clock_resolution_ns":1_000_000_000_u64 / self.ticks_per_second as u64,
            "vm_rss_before_kib":self.rss_kib,"vm_rss_after_kib":after.rss_kib,
            "cumulative_vm_hwm_before_kib":self.cumulative_hwm_kib,
            "cumulative_vm_hwm_after_kib":after.cumulative_hwm_kib,
            "scope":"client process including native RPC workers; snapshots bracket timed work",
            "cpu_note":"tick-quantized; short trials can report zero, which does not imply zero CPU cost",
            "memory_note":"VmHWM is process lifetime cumulative, not an independent peak for this path",
            "allocation_count":Value::Null,"copied_bytes_measured":Value::Null}))
    }
}

fn status_kib(status: &str, name: &str) -> std::result::Result<u64, TestFailure> {
    let mut words = status
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .ok_or("process memory field unavailable")?
        .split_whitespace();
    let value = words.next().ok_or("missing process memory value")?.parse()?;
    if words.next() != Some("kB") || words.next().is_some() {
        return Err("unexpected process memory units".into());
    }
    Ok(value)
}

pub struct TestCountedCatalog {
    source: Group0ChunkKvRangeCatalogSource,
    pub loads: AtomicU64,
}

impl TestCountedCatalog {
    pub fn new(source: Group0ChunkKvRangeCatalogSource) -> Self {
        Self {
            source,
            loads: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl ChunkKvRangeCatalogSource for TestCountedCatalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        self.loads.fetch_add(1, Ordering::Relaxed);
        self.source.load().await
    }
}

#[derive(Default)]
pub struct TestRpcCounts {
    point: AtomicU64,
    batch: AtomicU64,
    batch_items: AtomicU64,
    client_failures: AtomicU64,
    server_failures: AtomicU64,
}

pub struct TestCountedTransport {
    pub native: Arc<ChunkKvRpcTransport>,
    counts: TestRpcCounts,
}

impl TestCountedTransport {
    pub fn new(native: Arc<ChunkKvRpcTransport>) -> Self {
        Self {
            native,
            counts: TestRpcCounts::default(),
        }
    }

    pub fn counts(&self) -> [u64; 5] {
        [
            &self.counts.point,
            &self.counts.batch,
            &self.counts.batch_items,
            &self.counts.client_failures,
            &self.counts.server_failures,
        ]
        .map(|counter| counter.load(Ordering::Relaxed))
    }

    pub fn delta(&self, before: [u64; 5]) -> Value {
        let after = self.counts();
        let delta: Vec<_> = after
            .iter()
            .zip(before)
            .map(|(after, before)| after - before)
            .collect();
        json!({"point_rpc_attempts": delta[0], "batch_rpc_attempts": delta[1],
            "batch_items_including_retries": delta[2], "client_attempt_failures": delta[3],
            "typed_attempt_failures": delta[4]})
    }
}

#[async_trait]
impl ChunkKvTransport for TestCountedTransport {
    async fn point(&self, endpoint: &str, request: &PointRequest) -> Result<ChunkKvResponse> {
        assert!(
            matches!(request.operation, PointOperation::PutIfAbsent { .. }),
            "comparison must not issue a GET or an unconditional mutation"
        );
        self.counts.point.fetch_add(1, Ordering::Relaxed);
        let result = self.native.point(endpoint, request).await;
        match &result {
            Err(_) => {
                self.counts.client_failures.fetch_add(1, Ordering::Relaxed);
            }
            Ok(response) if response.result.is_err() => {
                self.counts.server_failures.fetch_add(1, Ordering::Relaxed);
            }
            Ok(_) => {}
        }
        result
    }

    async fn batch_mutate(
        &self,
        endpoint: &str,
        request: &BatchMutationRequest,
    ) -> Result<BatchMutationResponse> {
        assert!(request
            .operations
            .iter()
            .all(|item| matches!(item.operation, PointOperation::PutIfAbsent { .. })));
        self.counts.batch.fetch_add(1, Ordering::Relaxed);
        self.counts
            .batch_items
            .fetch_add(request.operations.len() as u64, Ordering::Relaxed);
        let result = self.native.batch_mutate(endpoint, request).await;
        match &result {
            Err(_) => {
                self.counts.client_failures.fetch_add(1, Ordering::Relaxed);
            }
            Ok(response) => {
                let failures = match &response.result {
                    Err(_) => 1,
                    Ok(items) => items.iter().filter(|item| item.result.is_err()).count(),
                };
                self.counts
                    .server_failures
                    .fetch_add(failures as u64, Ordering::Relaxed);
            }
        }
        result
    }
}

pub fn tcp_delta(before: &Value, after: &Value) -> Value {
    let mut result = json!({});
    for class in ["metadata", "aggregate"] {
        assert!(before[class]["transport"]["rdma"].is_null());
        assert!(after[class]["transport"]["rdma"].is_null());
        for (native, report) in [
            ("wire_bytes_sent", "tcp_protocol_bytes_sent"),
            ("rpc_frames_sent", "completed_rpc_frames_sent"),
            ("writev_calls", "writev_calls"),
            ("send_queue_rejects", "send_queue_rejects"),
        ] {
            let before = before[class]["transport"][native].as_u64();
            let after = after[class]["transport"][native].as_u64();
            result[class][report] = match (before, after) {
                (Some(before), Some(after)) => json!(after - before),
                _ => Value::Null,
            };
        }
    }
    for name in ["submit_fail", "submit_retry", "resp_missed", "reaped"] {
        result["rpc_counters"][name] = json!(
            after["rpc_counters"][name].as_u64().unwrap() - before["rpc_counters"][name].as_u64().unwrap()
        );
    }
    result
}
