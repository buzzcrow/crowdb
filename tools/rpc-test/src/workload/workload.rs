// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded per-loader correlation and per-completion-owner measurement.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_common::metrics::histogram::HistogramData;
use crowdb_common::metrics::{HistogramSnapshot, LatencyHistogram, MetricsRegistry};
use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs, ConnectionPingResponse};
use crowdb_rpc_ffi::co_bench::CoBenchHandler;
use crowdb_rpc_ffi::{Buffer, RpcError};
use flatbuffers::FlatBufferBuilder;

#[derive(Clone, Copy)]
pub(super) enum Count {
    Attempts,
    Responses,
    Warmup,
    Window,
    Drain,
    ResponseErrors,
    Timeouts,
    ConnectionErrors,
    PayloadMismatches,
    Duplicates,
    SubmittedInWindow,
    LateLoaders,
}

const COUNT: usize = 12;
const EMPTY: u64 = u64::MAX;

struct Shard {
    counts: [AtomicU64; COUNT],
    latency: Arc<LatencyHistogram>,
}

struct Slot {
    expected: AtomicU64,
    started: AtomicBool,
    submitted_in_window: AtomicBool,
    shard: usize,
}

pub(super) struct EchoWorkload {
    slots: Vec<Slot>,
    shards: Vec<Shard>,
    mask: u64,
    payload_bytes: usize,
    pub measure_start: Instant,
    pub measure_end: Instant,
}

impl EchoWorkload {
    pub fn new(
        loaders: u32,
        owners: &[usize],
        payload_bytes: usize,
        warmup: Duration,
        measure: Duration,
        registry: &mut MetricsRegistry,
    ) -> Self {
        let shard_count = owners.iter().copied().max().unwrap_or(0) + 1;
        let measure_start = Instant::now() + warmup;
        Self {
            slots: (0..loaders)
                .map(|i| Slot {
                    expected: AtomicU64::new(EMPTY),
                    started: AtomicBool::new(false),
                    submitted_in_window: AtomicBool::new(false),
                    shard: owners[i as usize % owners.len()],
                })
                .collect(),
            shards: (0..shard_count)
                .map(|i| Shard {
                    counts: std::array::from_fn(|_| AtomicU64::new(0)),
                    latency: registry.register_histogram(format!("bench.rpc.owner{i}.latency.lh")),
                })
                .collect(),
            mask: u64::from(loaders.next_power_of_two() - 1),
            payload_bytes,
            measure_start,
            measure_end: measure_start + measure,
        }
    }

    fn slot(&self, id: u64) -> &Slot {
        &self.slots[usize::try_from(id & self.mask).expect("bounded loader index")]
    }

    fn inc(&self, slot: &Slot, count: Count) {
        self.shards[slot.shard].counts[count as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn totals(&self) -> [u64; COUNT] {
        std::array::from_fn(|i| {
            self.shards
                .iter()
                .map(|s| s.counts[i].load(Ordering::Relaxed))
                .sum()
        })
    }

    pub fn started_loaders(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.started.load(Ordering::Relaxed))
            .count()
    }

    pub fn histogram(&self) -> HistogramSnapshot {
        let mut data = HistogramData::default();
        for shard in &self.shards {
            data.merge(&shard.latency.capture_total());
        }
        data.snapshot(0)
    }

    fn complete(
        &self,
        id: u64,
        status: Result<(), RpcError>,
        latency_ns: u64,
        control: &[u8],
        data: &[u8],
    ) -> bool {
        let now = Instant::now();
        let slot = self.slot(id);
        self.inc(slot, Count::Responses);
        self.inc(
            slot,
            if now < self.measure_start {
                Count::Warmup
            } else if now < self.measure_end {
                Count::Window
            } else {
                Count::Drain
            },
        );
        if slot.submitted_in_window.load(Ordering::Relaxed) {
            self.inc(slot, Count::SubmittedInWindow);
        }
        let correlated = slot.expected.swap(EMPTY, Ordering::AcqRel) == id;
        if !correlated {
            self.inc(slot, Count::Duplicates);
        }
        if let Err(error) = status {
            self.inc(slot, Count::ResponseErrors);
            match error {
                RpcError::Timeout => self.inc(slot, Count::Timeouts),
                RpcError::ConnectionClosed | RpcError::ConnectionError => {
                    self.inc(slot, Count::ConnectionErrors);
                }
                _ => {}
            }
        } else {
            let valid = correlated && valid_response(id, self.payload_bytes, control, data);
            if !valid {
                self.inc(slot, Count::PayloadMismatches);
            }
            if valid && now >= self.measure_start && now < self.measure_end {
                self.shards[slot.shard].latency.observe(latency_ns);
            }
        }
        now < self.measure_end
    }
}

impl CoBenchHandler for EchoWorkload {
    fn build_request(&self, id: u64) -> Option<(Buffer, Buffer)> {
        let now = Instant::now();
        if now >= self.measure_end {
            return None;
        }
        let slot = self.slot(id);
        if !slot.started.swap(true, Ordering::Relaxed) && now >= self.measure_start {
            self.inc(slot, Count::LateLoaders);
        }
        slot.expected.store(id, Ordering::Release);
        slot.submitted_in_window
            .store(now >= self.measure_start, Ordering::Relaxed);
        self.inc(slot, Count::Attempts);
        let mut bytes = vec![0xAB; self.payload_bytes];
        let prefix = bytes.len().min(8);
        bytes[..prefix].copy_from_slice(&id.to_le_bytes()[..prefix]);
        Some((Buffer::from_vec(build_control(id)), Buffer::from_vec(bytes)))
    }

    fn on_response(&self, id: u64, status: Result<(), RpcError>, latency_ns: u64) -> bool {
        self.complete(id, status, latency_ns, &[], &[])
    }

    fn on_response_data(
        &self,
        id: u64,
        status: Result<(), RpcError>,
        latency_ns: u64,
        control: &[u8],
        data: &[u8],
    ) -> bool {
        self.complete(id, status, latency_ns, control, data)
    }
}

pub(super) fn build_control(id: u64) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let request = ConnectionPingRequest::create(
        &mut builder,
        &ConnectionPingRequestArgs {
            id,
            rpc_create_nano: 0,
        },
    );
    builder.finish(request, None);
    builder.finished_data().to_vec()
}

fn valid_response(id: u64, size: usize, control: &[u8], data: &[u8]) -> bool {
    let prefix = size.min(8);
    data.len() == size
        && data[..prefix] == id.to_le_bytes()[..prefix]
        && data[prefix..].iter().all(|byte| *byte == 0xAB)
        && flatbuffers::root::<ConnectionPingResponse<'_>>(control)
            .is_ok_and(|response| response.id() == id && response.ret().0 == 0)
}
