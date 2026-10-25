// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded per-loader correlation and per-completion-owner measurement.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_common::metrics::histogram::HistogramData;
use crowdb_common::metrics::{HistogramSnapshot, LatencyHistogram, MetricsRegistry};
use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs, ConnectionPingResponse};
use crowdb_rpc_ffi::co_bench::CoBenchHandler;
use crowdb_rpc_ffi::{Buffer, ProducerBufferAllocator, RpcError};
use flatbuffers::FlatBufferBuilder;
use serde_json::{json, Value};

use super::payload::PayloadProfile;

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
    BuildErrors,
}

const COUNT: usize = 13;
const EMPTY: u64 = u64::MAX;

struct Shard {
    counts: [AtomicU64; COUNT],
    latency: Arc<LatencyHistogram>,
    sizes: Vec<Arc<LatencyHistogram>>,
}

struct Slot {
    expected: AtomicU64,
    started: AtomicBool,
    submitted_in_window: AtomicBool,
    shard: usize,
    expected_class: AtomicUsize,
}

pub(super) struct EchoWorkload {
    slots: Vec<Slot>,
    shards: Vec<Shard>,
    mask: u64,
    profile: PayloadProfile,
    allocator: Option<ProducerBufferAllocator>,
    pub measure_start: Instant,
    pub measure_end: Instant,
}

impl EchoWorkload {
    pub fn new(
        loaders: u32,
        owners: &[usize],
        profile: PayloadProfile,
        allocator: Option<ProducerBufferAllocator>,
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
                    expected_class: AtomicUsize::new(0),
                })
                .collect(),
            shards: (0..shard_count)
                .map(|i| Shard {
                    counts: std::array::from_fn(|_| AtomicU64::new(0)),
                    latency: registry.register_histogram(format!("bench.rpc.owner{i}.latency.lh")),
                    sizes: profile
                        .sizes()
                        .iter()
                        .map(|size| {
                            registry.register_histogram(format!("bench.rpc.owner{i}.size{size}.latency.lh"))
                        })
                        .collect(),
                })
                .collect(),
            mask: u64::from(loaders.next_power_of_two() - 1),
            profile,
            allocator,
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

    pub fn size_results(&self) -> Vec<Value> {
        self.profile
            .sizes()
            .iter()
            .enumerate()
            .map(|(index, size)| {
                let mut data = HistogramData::default();
                for shard in &self.shards {
                    data.merge(&shard.sizes[index].capture_total());
                }
                let snapshot = data.snapshot(0);
                json!({"payload_bytes": size, "completed": snapshot.count,
                   "payload_bytes_completed": snapshot.count * *size as u64,
                   "p50_ns": snapshot.p50, "p99_ns": snapshot.p99,
                   "p999_ns": snapshot.p999, "max_ns": snapshot.max})
            })
            .collect()
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
            let class = slot.expected_class.load(Ordering::Relaxed);
            let valid = correlated && valid_response(id, self.profile.sizes()[class], control, data);
            if !valid {
                self.inc(slot, Count::PayloadMismatches);
            }
            if valid && now >= self.measure_start && now < self.measure_end {
                self.shards[slot.shard].latency.observe(latency_ns);
                self.shards[slot.shard].sizes[class].observe(latency_ns);
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
        let class = self.profile.select(id);
        slot.expected_class.store(class, Ordering::Relaxed);
        if !slot.started.swap(true, Ordering::Relaxed) && now >= self.measure_start {
            self.inc(slot, Count::LateLoaders);
        }
        slot.expected.store(id, Ordering::Release);
        slot.submitted_in_window
            .store(now >= self.measure_start, Ordering::Relaxed);
        let length = self.profile.sizes()[class];
        let data = if let Some(allocator) = &self.allocator {
            if length == 0 {
                Some(Buffer::from_vec(Vec::new()))
            } else {
                allocator
                    .allocate(u32::try_from(length).ok()?)
                    .and_then(|buffer| {
                        buffer
                            .initialize_with(length, 0xAB, |bytes| {
                                let prefix = bytes.len().min(8);
                                bytes[..prefix].copy_from_slice(&id.to_le_bytes()[..prefix]);
                            })
                            .ok()
                    })
            }
        } else {
            let mut bytes = vec![0xAB; length];
            let prefix = bytes.len().min(8);
            bytes[..prefix].copy_from_slice(&id.to_le_bytes()[..prefix]);
            Some(Buffer::from_vec(bytes))
        };
        let Some(data) = data else {
            self.inc(slot, Count::BuildErrors);
            return None;
        };
        self.inc(slot, Count::Attempts);
        Some((Buffer::from_vec(build_control(id)), data))
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
