// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crowdb_console_shared::ops::s3_bench::{self, MixWeights, S3BenchConfig, S3BenchWorkload};
use crowdb_test_harness::test_dirs::TestDir;

#[test]
fn mixed_weights_are_case_insensitive_and_deterministic() {
    let lower = MixWeights::parse("w20r70rr5l5").expect("lowercase mix");
    let mixed = MixWeights::parse("W20R70RR5L5").expect("mixed-case mix");
    let first = (0..256).map(|value| lower.select_name(value)).collect::<Vec<_>>();
    let second = (0..256).map(|value| mixed.select_name(value)).collect::<Vec<_>>();
    assert_eq!(first, second);
    for expected in ["write", "read", "range-read", "list"] {
        assert!(first.contains(&expected), "missing {expected}");
    }
}

#[test]
fn mixed_weights_reject_ambiguous_or_invalid_terms() {
    for value in [
        "",
        "w0",
        "w1W2",
        "x1",
        "rr",
        "w18446744073709551616",
        "w18446744073709551615r1",
    ] {
        assert!(MixWeights::parse(value).is_err(), "accepted {value}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "starts the complete memory-backed storage and S3 process stack"]
async fn memory_mix_exercises_every_s3_operation() {
    let runtime = TestDir::new("s3-memory-bench").expect("create benchmark runtime");
    let work_dir = runtime.path().to_path_buf();
    let result = s3_bench::run(S3BenchConfig {
        work_dir,
        workload: S3BenchWorkload::Mix,
        object_size: 1024,
        dataset_objects: 8,
        concurrency: 4,
        operations: 200,
        duration: Duration::from_secs(10),
        warmup_operations: 4,
        seed: 7,
        memory_budget_bytes: 2 * 1024 * 1024 * 1024,
        list_limit: 4,
        mix: MixWeights::default(),
    })
    .await
    .expect("memory S3 benchmark");
    assert_eq!(result.total_operations, 200);
    assert_eq!(result.total_errors, 0);
    assert!(result.by_operation.write.is_some());
    assert!(result.by_operation.read.is_some());
    assert!(result.by_operation.range_read.is_some());
    assert!(result.by_operation.list.is_some());
    assert_eq!(result.backing.diskio, "mem");
    assert!(!result.object_bytes_verified);
}
