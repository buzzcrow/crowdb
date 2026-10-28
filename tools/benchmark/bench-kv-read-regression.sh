#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Paxos-KV read: externally started 3-node host-network containers, mem-block.
# Run with pixi run bash tools/benchmark/bench-kv-read-regression.sh.
# Set KV_BENCH_DEPLOYMENT; optional KV_BENCH_CASES, DURATION, WARMUP, TAIL,
# KEYSPACE, CLI and LOG_ROOT use the KV_BENCH_ prefix.
# Defaults: 5s warmup / 20s measurement / 5s tail. <20s is a smoke run.
# Keep every dated result, including failures. Old native process data below
# remains a useful reference; compare code, resource and dataset conditions.
set -euo pipefail
cd "$(dirname "$0")/../.."
source tools/benchmark/bench-kv-common.sh

run_case() {
    local label="$1" threads="$2" mode="$3" policy=leader
    selected "$label" || return 0
    [[ "$mode" != minslot ]] || policy=any-replica
    prepare_dataset --value-size 64
    invoke "$label" read --key-space "$KEYSPACE" --value-size 64 --loader-num "$threads" \
        --connections "$threads" --read-mode "$mode" --read-endpoint-policy "$policy"
}
for threads in 1 6 16 32; do
    run_case "lin_${threads}t" "$threads" linearizable
    run_case "minslot_${threads}t" "$threads" minslot
done
# All cases now verify complete dataset bytes outside timing; keep these names.
run_case lin_16t_verify 16 linearizable
run_case minslot_16t_verify 16 minslot
finish_cases

# ---- Native process history (no containers) ----
# Reference results (2026-08-19, Apple M5 Pro, 18c, arm64, macOS 26.5):
#   10s mem mode, 3-node cluster, 100k pre-populated keys, 64B values.
#
#   label              mode        T:C   ops/s    avg_us  p99_us  err  notes
#   lin_1t             linearizable 1:1   21112    46      67      0    per-read cost
#   minslot_1t         minslot      1:1   21691    45      66      0    same as lin (lease ~0)
#   lin_6t             linearizable 6:6   70668    84      163     0    mid-concurrency
#   minslot_6t         minslot      6:6   77622    76      142     0    +9.8% vs lin (distributed)
#   lin_16t            linearizable 16:16 106399   148     251     0    high concurrency
#   minslot_16t        minslot      16:16 107455   147     235     0    +1.0% vs lin (converging)
#   lin_32t            linearizable 32:32 119473   265     418     0    saturation
#   minslot_32t        minslot      32:32 113270   280     432     0    -5.2% vs lin (saturated)
#   lin_16t_verify     linearizable 16:16 105613   150     252     0    corr=0
#   minslot_16t_verify minslot      16:16 106662   148     237     0    corr=0
#
# 2026-09-02 (same hw, +mem-block WAL wipe fix + bench on group 1):
#   Bench runs on group 1 (group 0 sysdata preserved). Numbers are within
#   run-to-run noise of the prior 2026-09-02 run. lin_1t still has the
#   startup lease-expiry burst (1188 errors, corr=0) — pre-existing, not
#   related to the group-1 change.
#
#   label              mode        T:C   ops/s    avg_us  p50_us  p99_us  err   notes
#   lin_1t             linearizable 1:1   14493    66      100     500     1188  startup lease burst
#   minslot_1t         minslot      1:1   12368    78      100     500     0
#   lin_6t             linearizable 6:6   68923    83      100     500     0     +2.6% vs prior
#   minslot_6t         minslot      6:6   97380    59      100     100     0     +1.1% vs prior
#   lin_16t            linearizable 16:16 232388   65      100     500     0     +0.5% vs prior
#   minslot_16t        minslot      16:16 228956   66      100     500     0     +2.1% vs prior
#   lin_32t            linearizable 32:32 268557   114     500     500     0     +1.0% vs prior
#   minslot_32t        minslot      32:32 260685   116     500     500     0     +1.6% vs prior
#   lin_16t_verify     linearizable 16:16 234234   64      100     500     0     corr=0
#   minslot_16t_verify minslot      16:16 228800   66      100     500     0     corr=0
#
# Analysis: doc/design/kv/kv-read-flow-analysis.md § Latest Benchmark Results.
#
# Intel i9-7960X (2026-09-10, 16c/32t, Linux 6.11, x86_64):
#   Same build/config as AMD 2026-09-02. mem-block backend, 10s, 100k
#   pre-populated keys (group 1), 64B values, 3-node cluster. Zero errors
#   across all configs (lin_1t startup lease burst is gone on Intel).
#   Low-concurrency (1T, 6T) 50-83% slower than AMD — per-op overhead is
#   much higher on Intel. 16T-32T within 6-28% (within 30% gap). minslot
#   scales better on Intel: minslot_16t is 24% faster than lin_16t (vs
#   ~equal on AMD). Gaps > 30% documented in
#   doc/working/regression-perf-review.md.
#
#   label              mode        T:C   ops/s    avg_us  p50_us  p99_us  err   notes
#   lin_1t             linearizable 1:1   6380     151     135     540     0
#   minslot_1t         minslot      1:1   2157     454     471     712     0
#   lin_6t             linearizable 6:6   34334    167     161     321     0
#   minslot_6t         minslot      6:6   33634    170     164     325     0
#   lin_16t            linearizable 16:16 168911   90      85      175     0
#   minslot_16t        minslot      16:16 209008   72      70      126     0     +24% vs lin
#   lin_32t            linearizable 32:32 205674   151     144     321     0
#   minslot_32t        minslot      32:32 244649   126     124     216     0     +19% vs lin
#   lin_16t_verify     linearizable 16:16 168670   90      85      175     0     corr=0
#   minslot_16t_verify minslot      16:16 207835   73      70      129     0     corr=0

# ---- Host-network container history ----
# 2026-10-27: AMD Ryzen 9 5950X, 32 logical CPUs, ~62GiB host RAM.
# 3 containers, 8GiB/4 CPU quota each, no cpuset; mem-block Tree + WAL;
# server workers=2, inflight=32/coalesce=16, peer pool=4, event-write.
# Release host CLI, 5/60/5s warmup/measurement/tail, 100k keys x 64B.
# Image: crowdb-node:ci253-shared, sha256:89532eddadf085c527116a1986bb67149017eaeca381abbcae89b501d1c9abb1
# Revision: ed0447797d137512f0bdf2fd3db1c46e22804bdc; source tree: ea1c3256e20ae90140bd95e727ab87c748b3a033.
# Before port consolidation: mgmt/RPC 10000/10100, offsets 0/1000/2000.
# Raw reports: .crowdb-runtime/artifacts/bench/kv-host-read/ (includes CLI SHA256).
# label          ops/s       avg_us   p50_us   p99_us   p999_us  errors/incomplete
# lin_1t         15673.817   63.700   60.159   109.567  135.167  0/0
# minslot_1t     10799.117   92.483   89.599   150.527  187.391  0/0
# lin_16t        108878.550  146.824  144.383  251.903  299.007  0/0
# minslot_16t    180776.800  88.296   86.527   139.263  185.343  0/0
# lin_16t is ~53% below the Sep-02 native reference. Conditions/code changed;
# no cgroup CPU throttling observed. Cause remains open; do not blame host networking.

# 2026-10-27: 1M x 64B read baseline, 5/20/5s (new standard).
# Same host, three 8GiB/4-CPU containers, no cpuset; workers=2, inflight/coalesce=32/16,
# peer pool=4, event-write, mem-block Tree/WAL. Default ports + offsets 0/1/2.
# Image: sha256:eb0dd67818e3f3ec2f81ba6969db8320f2dcd8a9c92c1bf727d1d1f7cce9d09d
# Source tree: b984ab3e0838bee528d880e6bb83fc34c3c54663.
# Raw reports + CLI checksum: .crowdb-runtime/artifacts/bench/kv-review-read/
# All zero errors/incomplete. Dataset differs from preceding 100k-key read history.
# 2026-10-27 lin_1t host-container ops/s=15305.9 latency_ns={"avg":65233.4343096453,"count":306118,"max":678089,"min":48201,"p50":64255,"p99":87039,"p999":105471} passed=true
# 2026-10-27 minslot_1t host-container ops/s=12233.05 latency_ns={"avg":81621.3496348008,"count":244661,"max":1754642,"min":55010,"p50":80895,"p99":113663,"p999":151551} passed=true
# 2026-10-27 lin_16t host-container ops/s=76390.9 latency_ns={"avg":209298.92435879144,"count":1527818,"max":18931996,"min":41271,"p50":167935,"p99":428031,"p999":15925247} passed=true
# 2026-10-27 minslot_16t host-container ops/s=137165.5 latency_ns={"avg":116380.78411116498,"count":2743310,"max":14660221,"min":41411,"p50":99839,"p99":352255,"p999":1318911} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/read/; full verification passed.
# 2026-10-27 lin_16t host-container ops/s=103176.05 latency_ns={"avg":154955.89959830794,"count":2063521,"max":2919434,"min":45220,"p50":153599,"p99":247807,"p999":319487} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/read/; full verification passed.
# 2026-10-27 lin_1t host-container ops/s=13340.1 latency_ns={"avg":74842.10373235583,"count":266802,"max":2234769,"min":48370,"p50":74751,"p99":106495,"p999":144383} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/read/; full verification passed.
# 2026-10-27 minslot_16t host-container ops/s=134152.2 latency_ns={"avg":118881.20723253141,"count":2683044,"max":2860484,"min":46610,"p50":114175,"p99":223231,"p999":397311} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/read/; full verification passed.
# 2026-10-27 minslot_1t host-container ops/s=11314.6 latency_ns={"avg":88249.34428525975,"count":226292,"max":1078029,"min":51920,"p50":84479,"p99":154623,"p999":199679} passed=true
