// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_access_dataset::{
    CasOutcome, DatasetIdentity, DatasetStore, DatasetUploads, NamespacePath, OperationId, StoreError,
    StoredValue, UploadRecord,
};
use serde::Serialize;
use serde_json::json;
use std::{
    future::Future,
    hint::black_box,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use super::upload::{TestPayload, TestStore};

pub struct TestCpuCase {
    pub key: Vec<u8>,
    pub before: StoredValue,
    pub json: Vec<u8>,
    pub after: Vec<u8>,
    pub decoded: UploadRecord,
    pub victim: OperationId,
    pub uploads: DatasetUploads,
    pub canned: Arc<TestCannedStore>,
    pub fixture: Arc<TestStore>,
}

impl TestCpuCase {
    pub async fn new(retired: u32, compact: bool) -> Self {
        let key = DatasetUploads::location(
            &DatasetIdentity::new(NamespacePath::root(), "owner-cpu").unwrap(),
            owner(0xe0, 0),
        )
        .unwrap();
        let live = owner(0xb0, 0);
        let victim = owner(0xb0, 1);
        let mut retired_owners: Vec<_> = (0..retired).map(|index| owner(0xa0, index)).collect();
        let mut value = json!({
            "version": 4,
            "length": 4,
            "part_size": 4,
            "md5": ([0_u8; 16]),
            "expires_at": 1000,
            "state": "claimed",
            "owners": [live, victim],
            "retired_owners": retired_owners,
            "cleanup_complete": false,
        });
        let decoded: UploadRecord = serde_json::from_value(value.clone()).unwrap();
        let json = serde_json::to_vec(&decoded).unwrap();
        let before = StoredValue {
            bytes: if compact {
                decoded.encode_stored_for_tests().unwrap()
            } else {
                json.clone()
            },
            revision: 1,
        };
        retired_owners.push(victim);
        retired_owners.sort();
        value["owners"] = json!([live]);
        value["retired_owners"] = json!(retired_owners);
        let after = serde_json::from_value::<UploadRecord>(value)
            .unwrap()
            .encode_stored_for_tests()
            .unwrap();
        let canned = Arc::new(TestCannedStore {
            key: key.clone(),
            before: before.clone(),
            after: after.clone(),
            gets: AtomicU64::new(0),
            exchanges: AtomicU64::new(0),
        });
        let uploads = DatasetUploads::new(canned.clone(), Arc::new(TestPayload::default()));
        assert_eq!(uploads.metadata(&key).await.unwrap(), decoded);
        uploads.retire_owner(&key, victim).await.unwrap();
        let fixture = Arc::new(TestStore::default());
        assert!(matches!(
            fixture.compare_exchange(&key, None, &before.bytes).await.unwrap(),
            CasOutcome::Applied(_)
        ));
        Self {
            key,
            before,
            json,
            after,
            decoded,
            victim,
            uploads,
            canned,
            fixture,
        }
    }
}

fn owner(domain: u8, index: u32) -> OperationId {
    let mut bytes = [domain; 16];
    bytes[12..].copy_from_slice(&index.to_be_bytes());
    OperationId::from_bytes(&bytes).unwrap()
}

/// Each invocation replays an independent fixed input; this adapter does not persist a CAS.
pub struct TestCannedStore {
    key: Vec<u8>,
    before: StoredValue,
    after: Vec<u8>,
    gets: AtomicU64,
    exchanges: AtomicU64,
}

impl TestCannedStore {
    pub fn counts(&self) -> (u64, u64) {
        (
            self.gets.load(Ordering::Relaxed),
            self.exchanges.load(Ordering::Relaxed),
        )
    }
}

#[async_trait]
impl DatasetStore for TestCannedStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        assert_eq!(key, self.key);
        Ok(Some(self.before.clone()))
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.exchanges.fetch_add(1, Ordering::Relaxed);
        // These exact comparisons are included in the canned-adapter CPU sample.
        assert_eq!(key, self.key);
        assert_eq!(expected, Some(self.before.bytes.as_slice()));
        assert_eq!(value, self.after);
        Ok(CasOutcome::Applied(2))
    }
}

#[derive(Clone, Copy, Serialize)]
pub struct TestTiming {
    thread_cpu_ns: u64,
    wall_ns: u64,
}

pub async fn measured<F: Future>(work: F) -> (F::Output, TestTiming) {
    let before = thread_cpu_ns();
    let started = Instant::now();
    let output = black_box(work).await;
    let wall_ns = u64::try_from(started.elapsed().as_nanos()).unwrap();
    let thread_cpu_ns = thread_cpu_ns().checked_sub(before).unwrap();
    (
        output,
        TestTiming {
            thread_cpu_ns,
            wall_ns,
        },
    )
}

fn thread_cpu_ns() -> u64 {
    // Scheduler runtime can otherwise stay unchanged until the next tick.
    // Yield at both sample boundaries to refresh accounting without unsafe
    // clock bindings; the separately reported empty sample includes this cost.
    std::thread::yield_now();
    std::fs::read_to_string("/proc/thread-self/schedstat")
        .expect("Linux per-thread scheduler runtime must be available")
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

pub fn report(retired: u32, mode: &str, iterations: u64, record_bytes: usize, samples: &[TestTiming]) {
    let mut cpu: Vec<_> = samples.iter().map(|sample| sample.thread_cpu_ns).collect();
    let mut wall: Vec<_> = samples.iter().map(|sample| sample.wall_ns).collect();
    cpu.sort_unstable();
    wall.sort_unstable();
    println!(
        "owner_cpu {}",
        json!({
            "retired_before": retired,
            "mode": mode,
            "iterations_per_sample": iterations,
            "record_bytes": record_bytes,
            "samples": samples,
            "median_thread_cpu_ns_per_iteration": cpu[cpu.len() / 2] / iterations,
            "median_wall_ns_per_iteration": wall[wall.len() / 2] / iterations,
        })
    );
}
