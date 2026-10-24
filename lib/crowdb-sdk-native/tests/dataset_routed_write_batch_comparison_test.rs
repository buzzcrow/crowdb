// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/routed_write_batch.rs"]
mod fixture;

use crowdb_access_dataset::SnapshotId;
use fixture::{TestFailure, TestMode, TestProfile, TestRoutedBatch, TestRows};
use serde_json::{json, Value};

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires an owned real Chunk-KV fixture and exclusive measurement window"]
async fn compare_bounded_immutable_point_and_routed_batch_writes() -> Result<(), TestFailure> {
    let client = TestRoutedBatch::connect().await?;
    let run = SnapshotId::random().to_string();
    let mut reports = Vec::new();
    let mut summaries = Vec::new();
    let mut valid = true;
    let mut trial = 0;
    for profile in [TestProfile::Metadata, TestProfile::LargeValue] {
        let mut point_times = Vec::new();
        let mut batch_times = Vec::new();
        for round in 0..6 {
            let measured = round >= 3;
            let order = if round % 2 == 0 {
                [TestMode::Point, TestMode::Batch]
            } else {
                [TestMode::Batch, TestMode::Point]
            };
            for mode in order {
                let rows = TestRows::new(profile, &run, trial, mode.key());
                let mut result = client.trial(rows, mode).await?;
                result.report["warmup"] = json!(!measured);
                result.report["round"] = json!(round);
                println!("{}", result.report);
                valid &= result.valid;
                if measured && result.valid {
                    match mode {
                        TestMode::Point => point_times.push(result.elapsed_ns),
                        TestMode::Batch => batch_times.push(result.elapsed_ns),
                    }
                }
                reports.push(result.report);
            }
            trial += 1;
            if !valid {
                break;
            }
        }
        summaries.push(json!({"profile":profile.name(),"point":spread(&mut point_times),"batch":spread(&mut batch_times)}));
        if !valid {
            break;
        }
    }
    let report = json!({"schema_version":1,"run":run,"transport":"tcp",
        "scope":"real routed Chunk-KV typed write acknowledgements; no Dataset publication",
        "warmup_rounds_per_path":3,"measurement_rounds_per_path":3,"valid":valid,
        "byte_semantics":"TCP protocol bytes accepted by writev, including RPC framing; not NIC bytes, RDMA WRs or doorbells",
        "rpc_scope":"owner RPC attempts and completed TCP frames; catalog source loads are separate and not called RPC counts",
        "budget_semantics":"logical pending key/value owners plus one prepared row; transport copies and encoding are additional",
        "implicit_confirmation_gets":false,"cleanup":"isolated keys remain in the externally owned disposable fixture",
        "trials":reports,"summaries":summaries});
    println!("{report}");
    if let Ok(path) = std::env::var("CROWDB_WRITE_BATCH_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    client.shutdown();
    if !valid {
        return Err(
            "write comparison failed; original results retained in report without confirmation reads".into(),
        );
    }
    Ok(())
}

fn spread(samples: &mut [u64]) -> Value {
    samples.sort_unstable();
    if samples.len() != 3 {
        return json!({"valid_rounds":samples.len(),"elapsed_ns":samples});
    }
    json!({"valid_rounds":3,"min_ns":samples[0],"median_ns":samples[1],"max_ns":samples[2],"elapsed_ns":samples})
}
