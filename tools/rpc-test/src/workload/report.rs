// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Canonical machine result; unavailable measurements stay null.

use std::collections::BTreeSet;

use crowdb_rpc_ffi::co_bench::CoStats;
use crowdb_rpc_ffi::sys::CrowdbRpcClientCounters;
use serde_json::{json, Value};

use super::workload::{Count, EchoWorkload};
use super::RpcArgs;

pub(super) fn base(args: &RpcArgs, rdma: bool) -> Value {
    let mut row = json!({
        "schema_version": 1, "case_id": args.case_id, "repeat_index": args.repeat_index,
        "run_id": chrono::Utc::now().to_rfc3339(), "requested_path": if rdma {"rdma"} else {"tcp"},
        "control_path": "tcp", "client_mode": args.mode, "parameters": args,
        "loader_count": args.loader_count, "connection_count": args.connection_count,
        "producer_threads": 0, "coroutine_starter_threads": 1, "in_flight_per_loader": 1, "logical_ops_per_rpc": 1,
        "payload_bytes": args.payload_bytes, "warmup_s": args.warmup_secs, "measure_s": args.duration_secs,
        "allow_tcp_fallback": false, "valid": false, "client_connections": [],
        "latency_scope": "native post-build/pre-submit to response callback; Rust validation excluded",
        "transport_counter_scope": "all traffic including warmup/drain and capability frames",
        "resource_counter_scope": "all traffic including warmup/drain",
    });
    for name in [
        "actual_path",
        "qpair_count",
        "fallback_count",
        "submitted",
        "submitted_in_window",
        "wire_bytes",
        "avg_ns",
        "p50_ns",
        "p90_ns",
        "p95_ns",
        "p99_ns",
        "p999_ns",
        "min_ns",
        "max_ns",
        "submit_rejects",
        "timeouts",
        "connection_errors",
        "cq_errors",
        "payload_mismatches",
        "duplicate_completions",
        "retries",
        "response_errors",
        "client_exit",
        "server_exit",
        "failure",
        "client_before",
        "client_after",
        "server_before",
        "server_after",
        "frames_per_send",
        "server_frames_per_send",
        "epoll_wakeups",
        "rnr_provider_counters",
        "topology",
        "git_sha",
        "binary_hashes",
        "cpu_hca_identity",
    ] {
        row[name] = Value::Null;
    }
    for name in [
        "completed_in_window",
        "warmup_completed",
        "drain_completed",
        "rpc_tps",
        "logical_ops_s",
        "payload_Bps",
        "latency_count",
        "histogram_zero_count",
        "histogram_overflow_count",
        "errors_total",
        "setup_errors",
        "report_errors",
    ] {
        row[name] = json!(0);
    }
    row
}

pub(super) fn paths(report: &mut Value, connections: &[Value], rdma: bool) -> Result<Vec<usize>, String> {
    let expected = if rdma { "rdma" } else { "tcp" };
    let mut qpairs = BTreeSet::new();
    let mut owners = Vec::new();
    for (index, connection) in connections.iter().enumerate() {
        if connection["path"] != expected || connection["open"] != true {
            return Err(format!("workload path mismatch: {connection}"));
        }
        if rdma {
            let qp = connection["info"]["qpair_number"]
                .as_u64()
                .filter(|v| *v > 0)
                .ok_or("missing actual QP number")?;
            if !qpairs.insert(qp) {
                return Err("RDMA connections share a QP".into());
            }
            owners.push(
                connection["info"]["cq_index"]
                    .as_u64()
                    .ok_or("missing CQ owner")?
                    .try_into()
                    .map_err(|_| "invalid CQ owner")?,
            );
        } else {
            owners.push(index);
        }
    }
    report["actual_path"] = json!(expected);
    report["qpair_count"] = json!(qpairs.len());
    report["client_connections"] = json!(connections);
    Ok(owners)
}

pub(super) fn finish(
    report: &mut Value,
    args: &RpcArgs,
    workload: &EchoWorkload,
    native: &CoStats,
    counters: &CrowdbRpcClientCounters,
) {
    let counts = workload.totals();
    let get = |c: Count| counts[c as usize];
    let histogram = workload.histogram();
    let rejects = native.total_errors.saturating_sub(get(Count::ResponseErrors));
    let accepted = get(Count::Attempts).saturating_sub(rejects);
    let reconcile = native.total_ops != get(Count::Responses)
        || native.total_errors < get(Count::ResponseErrors)
        || accepted != native.total_ops
        || workload.started_loaders() != args.loader_count as usize
        || get(Count::LateLoaders) != 0
        || histogram.count == 0
        || histogram.overflow_count != 0;
    let cq_errors = sum_rdma_delta(report, "cq_errors");
    let fallback = sum_rdma_delta(report, "fallback_count");
    let transport_errors: u64 = [
        "protocol_errors",
        "send_errors",
        "receive_post_errors",
        "registration_failures",
    ]
    .into_iter()
    .map(|field| sum_rdma_delta(report, field))
    .sum();
    let errors = native.total_errors
        + get(Count::PayloadMismatches)
        + get(Count::Duplicates)
        + counters.resp_missed
        + cq_errors
        + fallback
        + transport_errors
        + u64::from(reconcile);
    let tps = ratio(histogram.count, args.duration_secs);
    let fields = json!({
        "submitted": accepted, "build_attempts": get(Count::Attempts),
        "submitted_in_window": get(Count::SubmittedInWindow),
        "completed_in_window": histogram.count, "window_callbacks": get(Count::Window),
        "warmup_completed": get(Count::Warmup), "drain_completed": get(Count::Drain),
        "callbacks_total": get(Count::Responses), "native_total_ops": native.total_ops,
        "native_total_errors": native.total_errors, "started_loaders": workload.started_loaders(),
        "late_loaders": get(Count::LateLoaders), "rpc_tps": tps, "logical_ops_s": tps,
        "payload_Bps": tps * f64::from(u32::try_from(args.payload_bytes).unwrap_or(u32::MAX)), "latency_count": histogram.count,
        "avg_ns": histogram.avg, "p50_ns": histogram.p50, "p90_ns": histogram.p90,
        "p95_ns": histogram.p95, "p99_ns": histogram.p99, "p999_ns": histogram.p999,
        "min_ns": histogram.min, "max_ns": histogram.max, "histogram_zero_count": histogram.zero_count,
        "histogram_overflow_count": histogram.overflow_count,
    });
    extend(report, &fields);
    let fields = json!({
        "errors_total": errors, "submit_rejects": rejects, "response_errors": get(Count::ResponseErrors),
        "timeouts": get(Count::Timeouts), "reaped": counters.reaped,
        "connection_errors": get(Count::ConnectionErrors), "cq_errors": cq_errors,
        "payload_mismatches": get(Count::PayloadMismatches), "duplicate_completions": get(Count::Duplicates),
        "retries": counters.submit_retry, "native_submit_fail": counters.submit_fail,
        "unmatched_responses": counters.resp_missed, "fallback_count": fallback,
        "report_errors": u64::from(reconcile), "valid": errors == 0,
    });
    extend(report, &fields);
    for (side, output) in [
        ("client", "frames_per_send"),
        ("server", "server_frames_per_send"),
    ] {
        let rdma = report["actual_path"] == "rdma";
        let frames = delta(report, side, "rpc_frames_sent", rdma);
        let sends = delta(
            report,
            side,
            if rdma { "sends" } else { "writev_progress_calls" },
            rdma,
        );
        report[output] = if sends > 0 {
            json!(ratio(frames, sends))
        } else {
            Value::Null
        };
    }
    report["wire_bytes"] = json!(delta(
        report,
        "client",
        "wire_bytes_sent",
        report["actual_path"] == "rdma"
    ));
}

fn sum_rdma_delta(report: &Value, name: &str) -> u64 {
    delta(report, "client", name, true) + delta(report, "server", name, true)
}

fn delta(report: &Value, side: &str, name: &str, rdma: bool) -> u64 {
    let read = |phase| {
        let status = &report[format!("{side}_{phase}")];
        (if rdma { &status["rdma"] } else { status })[name]
            .as_u64()
            .unwrap_or(0)
    };
    read("after").saturating_sub(read("before"))
}

#[allow(clippy::cast_precision_loss)]
fn ratio(numerator: u64, denominator: u64) -> f64 {
    numerator as f64 / denominator as f64
}

pub(super) fn fail(report: &mut Value, error: &str) {
    report["failure"] = json!(error);
    let category = if report["native_total_ops"].is_number() {
        "report_errors"
    } else {
        "setup_errors"
    };
    report[category] = json!(report[category].as_u64().unwrap_or(0) + 1);
    report["errors_total"] = json!(report["errors_total"].as_u64().unwrap_or(0) + 1);
    report["valid"] = json!(false);
    report["client_exit"] = json!(1);
}

fn extend(report: &mut Value, fields: &Value) {
    report
        .as_object_mut()
        .expect("report object")
        .extend(fields.as_object().expect("fields object").clone());
}
