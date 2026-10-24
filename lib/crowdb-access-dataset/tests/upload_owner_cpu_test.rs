// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

#[path = "common/upload_owner_cpu.rs"]
mod cpu;
#[path = "common/upload.rs"]
mod upload;

use cpu::{measured, report, TestCpuCase};
use crowdb_access_dataset::{CasOutcome, DatasetStore, UploadRecord};
use std::{hint::black_box, sync::atomic::Ordering};

const ITERATIONS: u64 = 128;
const SAMPLES: usize = 7;
const WARMUP: u64 = 8;

#[derive(Clone, Copy)]
enum TestMode {
    Decode,
    Encode,
    Roundtrip,
    StoredValueClone,
    RetireCanned,
    FixtureCasPair,
}

impl TestMode {
    fn name(self) -> &'static str {
        match self {
            Self::Decode => "serde_record_decode_without_session_validation",
            Self::Encode => "serde_predecoded_record_encode",
            Self::Roundtrip => "serde_record_roundtrip_without_session_validation",
            Self::StoredValueClone => "stored_value_vec_clone",
            Self::RetireCanned => "retire_owner_with_canned_clone_and_exact_comparisons",
            Self::FixtureCasPair => "fixture_store_cas_and_reset_pair",
        }
    }
}

async fn exercise(case: &TestCpuCase, mode: TestMode, iterations: u64) {
    for _ in 0..iterations {
        match mode {
            TestMode::Decode => {
                black_box(serde_json::from_slice::<UploadRecord>(black_box(&case.before.bytes)).unwrap());
            }
            TestMode::Encode => {
                black_box(serde_json::to_vec(black_box(&case.decoded)).unwrap());
            }
            TestMode::Roundtrip => {
                let record: UploadRecord = serde_json::from_slice(black_box(&case.before.bytes)).unwrap();
                black_box(serde_json::to_vec(black_box(&record)).unwrap());
            }
            TestMode::StoredValueClone => {
                black_box(black_box(&case.before).clone());
            }
            TestMode::RetireCanned => {
                case.uploads
                    .retire_owner(black_box(&case.key), black_box(case.victim))
                    .await
                    .unwrap();
            }
            TestMode::FixtureCasPair => {
                assert!(matches!(
                    case.fixture
                        .compare_exchange(&case.key, Some(&case.before.bytes), &case.after)
                        .await
                        .unwrap(),
                    CasOutcome::Applied(_)
                ));
                assert!(matches!(
                    case.fixture
                        .compare_exchange(&case.key, Some(&case.after), &case.before.bytes)
                        .await
                        .unwrap(),
                    CasOutcome::Applied(_)
                ));
            }
        }
    }
}

// Run through pixi with `cargo test --release -p crowdb-access-dataset
// --test upload_owner_cpu_test -- --ignored --nocapture --test-threads=1` in an exclusive window.
#[tokio::test(flavor = "current_thread")]
#[ignore = "bounded release CPU benchmark; run alone, never during integration or workload measurements"]
async fn upload_owner_cpu_decomposition() {
    assert!(
        !black_box(cfg!(debug_assertions)),
        "this benchmark requires --release"
    );
    println!(
        "owner_cpu scope: current-thread CPU only; no KV/WAL/network; CPU and wall samples are distinct; \
         no quantities are subtracted. Seven raw samples per mode, 128 fixed iterations, eight warmups. \
         Setup is measured separately. Canned GET clones and exact before/after comparisons are timed; \
         its CAS does not persist. Counters are checked outside timing. Standalone serde excludes \
         decode_session validation; retire_owner includes it. Fixture CAS pairs include two mutations \
         and the existing test store's ArcSwap/BTreeMap/Vec cloning. CPU boundaries include scheduler \
         yields and proc reads; the separately measured empty boundary is not subtracted."
    );
    let mut overhead = Vec::new();
    for _ in 0..SAMPLES {
        overhead.push(measured(async {}).await.1);
    }
    report(0, "empty_timer_boundary", 1, 0, &overhead);
    for retired in [1, 64, 256, 4095] {
        let (case, setup) = measured(TestCpuCase::new(retired)).await;
        report(retired, "fixture_setup", 1, case.before.bytes.len(), &[setup]);
        for mode in [
            TestMode::Decode,
            TestMode::Encode,
            TestMode::Roundtrip,
            TestMode::StoredValueClone,
            TestMode::RetireCanned,
            TestMode::FixtureCasPair,
        ] {
            exercise(&case, mode, WARMUP).await;
            let mut samples = Vec::new();
            for _ in 0..SAMPLES {
                let before = case.canned.counts();
                let mutations = case.fixture.mutations.load(Ordering::Relaxed);
                samples.push(measured(exercise(&case, mode, ITERATIONS)).await.1);
                let calls = u64::from(matches!(mode, TestMode::RetireCanned)) * ITERATIONS;
                assert_eq!(case.canned.counts(), (before.0 + calls, before.1 + calls));
                let exchanges = u64::from(matches!(mode, TestMode::FixtureCasPair)) * 2 * ITERATIONS;
                assert_eq!(
                    case.fixture.mutations.load(Ordering::Relaxed),
                    mutations + exchanges
                );
            }
            report(
                retired,
                mode.name(),
                ITERATIONS,
                case.before.bytes.len(),
                &samples,
            );
        }
    }
}
