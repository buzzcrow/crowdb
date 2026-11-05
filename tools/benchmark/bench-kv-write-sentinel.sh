#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Snapshot sentinel: zero workload/correctness errors, zero elections,
# at least one completed snapshot, zero failed snapshots during full load.
# Three repetitions of 100k x 16KiB; reuse the already running containers.
# History 2026-10-27: FAILED run1; run2/run3 not run. AMD 5950X, host network,
# 3 containers x 12GiB/4 CPU, mem-block Tree/WAL, e2e election profile,
# 100k x 16KiB, 1T/1C, planned warmup/measurement/tail 5/20/5s.
# Memory guard stopped load at 7.64s (80%); 1 incomplete, 0 completed load
# snapshots, 2 elections. All 3 containers subsequently OOMKilled (exit 137).
# Partial throughput is NOT a valid perf result. Full-read verification still
# ran after load cancellation; this was not an idle reclamation observation.
# Node 2 flush output entries/time: 6081/1.102s, 11532/2.538s,
# 19481/8.325s, 42188/34.295s. Frozen queue reached 10; active could keep growing.
# Evidence: .crowdb-runtime/artifacts/bench/kv-review-sentinel/
# Image: sha256:eb0dd67818e3f3ec2f81ba6969db8320f2dcd8a9c92c1bf727d1d1f7cce9d09d
# 2026-10-27 follow-up requested larger memory, same image/dataset/CPU/profile:
# 16GiB/node, old 80% guard: run1 stopped at 23.521s, 1 incomplete,
# 0 elections / 0 completed snapshots. Final inspect: all running, no OOM.
# Sampled node memory peaks GiB: 12.488 / 13.829 / 13.517.
# 20GiB/node, old 80% guard: run1 completed 5/20/5s and verification,
# 3725.7 ops/s, p99=358399ns, zero request errors/incomplete/elections.
# Sentinel still FAILED: 0 completed snapshots in its window. No OOM.
# Sampled node memory peaks GiB: 15.518 / 15.248 / 14.496.
# run2/run3 not dispatched after each failed sentinel; no three-run pass.
# Evidence: .crowdb-runtime/artifacts/bench/kv-review-sentinel-{16g,20g}/
# Both follow-ups used CLI SHA256 1537fe8ad93a31974956b5c5019d6df900c870f923ca0957f5a0b5a2cd7dbe55.
# Guard subsequently changed to 90% at user request; above results remain 80%.
# 2026-10-27: 20GiB/node, 90% guard, same server image and 4-CPU quota.
# CLI SHA256: 43a7ad7904bde40d11c1408b44b0bf2b246133867bdde4af600facc1a4aa0129.
# Three INDEPENDENT fresh-cluster repetitions passed: 3794.15 / 3864.7 /
# 3720.3 ops/s, p99 430079 / 352255 / 434175ns. Each completed 5/20/5s,
# verified bytes, 0 errors/incomplete/elections, 3 completed snapshots total.
# This is NOT a pass for three consecutive repetitions on one deployment:
# the original reused run2 was stopped during preparation by the external
# host observer (MemAvailable=2003020KiB, below its 4GiB floor); run3 unrun.
# Docker OOMKilled=false; exit137 there resulted from external docker stop.
# Independent run2/run3 used newly deployed containers after preserving failure.
# Evidence: kv-review-sentinel-20g-90pct{,-fresh2,-fresh3} under bench artifacts.
set -euo pipefail
cd "$(dirname "$0")/../.."
export KV_BENCH_CASES="${KV_BENCH_CASES:-largeval_16k_run1 largeval_16k_run2 largeval_16k_run3}"
exec bash tools/benchmark/bench-kv-write-regression.sh

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 100000 keys, value_size=16384, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/sentinel/; full verification passed.
# Single fresh deployment, e2e election profile; not a continuous three-run sentinel pass.
# 2026-10-27 largeval_16k_run1 host-container ops/s=3678.0 latency_ns={"avg":271747.8872349103,"count":73560,"max":58775148,"min":186252,"p50":266239,"p99":364543,"p999":602111} passed=true
