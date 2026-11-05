// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use crowdb_tree_ffi::runtime::{ResourceUsage, ResourceUsageLog, ResourceUsageSnapshot, TREE_RESOURCE_NAMES};
use crowdb_tree_ffi::{Config, Crowdbtree, TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;

fn snapshot(id: u64, used: u64) -> ResourceUsageSnapshot {
    let mut resources = [ResourceUsage::default(); TREE_RESOURCE_NAMES.len()];
    resources[0] = ResourceUsage {
        used,
        reserved: used / 2,
        allocated: used / 2,
        limit: 100,
        ..ResourceUsage::default()
    };
    ResourceUsageSnapshot {
        scope_id: id,
        resource_mask: 1,
        resources,
    }
}

fn flush(log: &mut ResourceUsageLog, at: Instant) -> String {
    let mut bytes = Vec::new();
    log.flush(&mut bytes, "timestamp", at).unwrap();
    String::from_utf8(bytes).unwrap()
}

#[test]
fn averages_weight_observed_duration_and_keep_unchanged_usage_in_later_windows() {
    let mut log = ResourceUsageLog::default();
    let start = Instant::now();
    log.begin_sample();
    log.observe("tree", snapshot(1, 10), start);
    log.end_sample(start);
    log.begin_sample();
    let at = start + Duration::from_millis(100);
    log.observe("tree", snapshot(1, 80), at);
    log.end_sample(at);
    let first = flush(&mut log, start + Duration::from_millis(400));
    assert!(first.contains(" avg=62.500 sample_max=80 "), "{first}");
    assert!(
        first.contains("samples=2 coverage_ns=400000000 max_sample_gap_ns=300000000"),
        "{first}"
    );
    let second = flush(&mut log, start + Duration::from_millis(700));
    assert!(second.contains(" avg=80.000 sample_max=80 "), "{second}");
    assert!(
        second.contains("samples=0 coverage_ns=300000000 max_sample_gap_ns=600000000"),
        "{second}"
    );
}

#[test]
fn removed_and_reopened_scopes_do_not_mix_or_grow_forever() {
    let mut log = ResourceUsageLog::default();
    let start = Instant::now();
    log.begin_sample();
    log.observe("tree", snapshot(1, 40), start);
    log.end_sample(start);
    log.begin_sample();
    let at = start + Duration::from_millis(100);
    log.observe("tree", snapshot(2, 20), at);
    log.end_sample(at);
    let first = flush(&mut log, start + Duration::from_millis(200));
    assert_eq!(first.lines().count(), 2);
    assert!(first.contains("scope_id=1 unit=bytes active=0"), "{first}");
    assert!(first.contains("scope_id=2 unit=bytes active=1"), "{first}");
    let second = flush(&mut log, start + Duration::from_millis(300));
    assert_eq!(second.lines().count(), 1);
    assert!(!second.contains("scope_id=1"), "{second}");
}

#[test]
fn failed_samples_keep_last_usage_and_expose_missing_coverage() {
    let mut log = ResourceUsageLog::default();
    let start = Instant::now();
    log.begin_sample();
    log.observe("tree", snapshot(1, 90), start);
    log.end_sample(start);
    log.begin_sample();
    log.sample_failed("tree");
    log.end_sample(start + Duration::from_millis(100));
    let text = flush(&mut log, start + Duration::from_millis(200));
    assert!(text.contains(" avg=90.000 sample_max=90 "), "{text}");
    assert!(text.contains("sample_age_ns=200000000 sample_errors=1"), "{text}");
}

#[test]
fn contention_deltas_are_per_window_and_raw_queries_do_not_drain() {
    let mut log = ResourceUsageLog::default();
    let start = Instant::now();
    log.begin_sample();
    log.observe("tree", snapshot(1, 10), start);
    let mut next = snapshot(1, 10);
    next.resources[0].denied_tree = 3;
    next.resources[0].denied_host = 5;
    next.resources[0].waits = 7;
    log.observe("tree", next, start + Duration::from_millis(100));
    log.end_sample(start + Duration::from_millis(100));
    let first = flush(&mut log, start + Duration::from_millis(200));
    assert!(first.contains("denied_tree=3 denied_host=5 waits=7"), "{first}");
    let second = flush(&mut log, start + Duration::from_millis(300));
    assert!(second.contains("denied_tree=0 denied_host=0 waits=0"), "{second}");
}

#[test]
fn native_host_and_tree_resource_samples_keep_distinct_limits_and_scopes() {
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let tree = Crowdbtree::open(&Config {
        runtime: Some(runtime.clone()),
        ..Config::default()
    })
    .unwrap();
    let host = runtime.resource_usage().unwrap();
    let first = tree.resource_usage().unwrap();
    let second = tree.resource_usage().unwrap();
    assert_eq!(host.resource_mask, 0x3ff);
    assert_eq!(first.resource_mask, 0xffcff);
    assert_eq!(first.resources[13].limit, first.resources[6].limit);
    assert_eq!(first.resources[14].limit, first.resources[6].limit);
    assert_eq!(first.resources[15].limit, first.resources[6].limit + 2);
    assert_eq!(first.resources[16].limit, 4096);
    assert_eq!(first.resources[18].limit, 64);
    assert_eq!(first.scope_id, second.scope_id);
    assert_ne!(first.scope_id, 0);
    assert!(first.resources[1].used > 0);
    assert!(host.resources[1].used >= first.resources[1].used);
    assert!(first.resources[6].used > 0);
    assert_eq!(host.resources[8].limit, 4);
    assert_eq!(first.resources[0].denied_host, second.resources[0].denied_host);
}

#[test]
fn operation_and_source_windows_keep_units_peaks_and_capacity_denials() {
    let mut log = ResourceUsageLog::default();
    let start = Instant::now();
    let mut held = snapshot(7, 0);
    held.resource_mask = (1 << 13) | (1 << 17) | (1 << 18);
    held.resources[13].limit = 256;
    held.resources[13].used = 256;
    held.resources[17].limit = 1024;
    held.resources[17].used = 512;
    held.resources[18].limit = 64;
    held.resources[18].used = 64;
    log.begin_sample();
    log.observe("tree", held, start);
    log.end_sample(start);
    let at = start + Duration::from_millis(100);
    held.resources[13].used = 0;
    held.resources[13].denied_tree = 3;
    held.resources[17].used = 0;
    held.resources[18].used = 0;
    log.begin_sample();
    log.observe("tree", held, at);
    log.end_sample(at);
    let text = flush(&mut log, start + Duration::from_millis(200));
    let operation = text
        .lines()
        .find(|line| line.contains(".backend_operation_slots "))
        .unwrap();
    assert!(operation.contains("unit=count"), "{operation}");
    assert!(operation.contains("avg=128.000 sample_max=256"), "{operation}");
    assert!(operation.contains("denied_tree=3"), "{operation}");
    let keys = text
        .lines()
        .find(|line| line.contains(".source_key_bytes "))
        .unwrap();
    assert!(keys.contains("unit=bytes"), "{keys}");
    assert!(keys.contains("avg=256.000 sample_max=512"), "{keys}");
    let gaps = text
        .lines()
        .find(|line| line.contains(".gap_source_entry_slots "))
        .unwrap();
    assert!(gaps.contains("unit=count"), "{gaps}");
    assert!(gaps.contains("avg=32.000 sample_max=64"), "{gaps}");
    assert!(gaps.contains("contention_measured=0"), "{gaps}");
}

#[test]
fn transient_waiter_usage_remains_visible_after_the_queue_drains() {
    let mut log = ResourceUsageLog::default();
    let start = Instant::now();
    log.begin_sample();
    log.observe("tree", snapshot(1, 0), start);
    let mut waiting = snapshot(1, 0);
    waiting.resources[0].waiters = 10;
    log.observe("tree", waiting, start + Duration::from_millis(100));
    log.observe("tree", snapshot(1, 0), start + Duration::from_millis(300));
    log.end_sample(start + Duration::from_millis(300));
    let text = flush(&mut log, start + Duration::from_millis(400));
    assert!(
        text.contains("waiters=0 waiters_avg=5.000 waiters_sample_max=10"),
        "{text}"
    );
}

#[test]
fn fractional_worker_occupancy_does_not_round_down_to_idle() {
    let mut log = ResourceUsageLog::default();
    let start = Instant::now();
    let mut busy = ResourceUsageSnapshot {
        resource_mask: 1 << 8,
        ..Default::default()
    };
    busy.resources[8] = ResourceUsage {
        limit: 1,
        used: 1,
        ..Default::default()
    };
    log.begin_sample();
    log.observe("host.tree", busy, start);
    busy.resources[8].used = 0;
    log.observe("host.tree", busy, start + Duration::from_millis(300));
    log.end_sample(start + Duration::from_millis(300));
    let text = flush(&mut log, start + Duration::from_millis(400));
    assert!(
        text.contains("used=0 reserved=0 allocated=0 avg=0.750 sample_max=1"),
        "{text}"
    );
}

#[test]
fn pin_release_windows_report_registered_pending_and_capacity_denials() {
    let mut log = ResourceUsageLog::default();
    let start = Instant::now();
    let mut held = ResourceUsageSnapshot {
        resource_mask: 1 << 20,
        ..Default::default()
    };
    held.resources[20] = ResourceUsage {
        limit: 10,
        used: 8,
        allocated: 8,
        waiters: 2,
        ..Default::default()
    };
    log.begin_sample();
    log.observe("host.tree", held, start);
    held.resources[20] = ResourceUsage {
        limit: 10,
        used: 4,
        allocated: 4,
        denied_host: 3,
        waiters: 0,
        ..Default::default()
    };
    log.observe("host.tree", held, start + Duration::from_millis(100));
    log.end_sample(start + Duration::from_millis(100));
    let text = flush(&mut log, start + Duration::from_millis(200));
    assert!(
        text.contains("host.tree.resource.chunk_pin_release_slots"),
        "{text}"
    );
    assert!(text.contains("unit=count"), "{text}");
    assert!(
        text.contains("limit=10 used=4 reserved=0 allocated=4 avg=6.000 sample_max=8"),
        "{text}"
    );
    assert!(text.contains("denied_host=3"), "{text}");
    assert!(
        text.contains("waiters=0 waiters_avg=1.000 waiters_sample_max=2"),
        "{text}"
    );
    let next = flush(&mut log, start + Duration::from_millis(300));
    assert!(next.contains("denied_host=0"), "{next}");
    assert!(next.contains("avg=4.000 sample_max=4"), "{next}");
}
