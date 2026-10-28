#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Paxos-KV write: externally started 3-node host-network containers, mem-block.
# Run with pixi run bash tools/benchmark/bench-kv-write-regression.sh.
# Set KV_BENCH_DEPLOYMENT; optional KV_BENCH_CASES, DURATION, WARMUP, TAIL,
# KEYSPACE, CLI and LOG_ROOT use the KV_BENCH_ prefix.
# Defaults: 5s warmup / 20s measurement / 5s tail. <20s is a smoke run.
# Keep every dated result, including failures. Old native process data below
# remains a useful reference; compare code, resource and dataset conditions.
set -euo pipefail
cd "$(dirname "$0")/../.."
source tools/benchmark/bench-kv-common.sh

KEYSPACE="${KV_BENCH_KEYSPACE:-100000}"
run_case() {
    local label="$1" threads="$2" connections="$3" window="$4" coalesce="$5" workers="$6"; shift 6
    selected "$label" || return 0
    if [[ "$window" == 64 ]]; then
        : "${KV_BENCH_DEPLOYMENT_B:?profile B requires a separately prestarted win64/coalesce64 deployment}"
        load_deployment "$KV_BENCH_DEPLOYMENT_B"
    else
        load_deployment "$KV_BENCH_DEPLOYMENT"
    fi
    invoke "$label" write --key-space "$KEYSPACE" --value-size 512 --loader-num "$threads" \
        --connections "$connections" --expect-window "$window" --expect-coalesce "$coalesce" \
        --rpc-workers "$workers" --event-write "$@"
}
run_case write_1t_1c_win32_coales16 1 1 32 16 2
run_case write_16t_2c_win32_coales16 16 2 32 16 2
run_case write_64t_4c_win32_coales16 64 4 32 16 2
run_case write_128t_4c_win32_coales16 128 4 32 16 4
run_case write_256t_8c_win32_coales16 256 8 32 16 4
for repetition in 1 2 3; do
    selected "largeval_16k_run${repetition}" || continue
    load_deployment "$KV_BENCH_DEPLOYMENT"
    invoke "largeval_16k_run${repetition}" write --key-space "${KV_BENCH_LARGE_KEYSPACE:-100000}" \
        --value-size 16384 --loader-num 1 --connections 1 --expect-window 32 --expect-coalesce 16 \
        --event-write --require-snapshot
done
run_case write_512t_16c_win64_coales64 512 16 64 64 4
run_case write_1000t_16c_win64_coales64 1000 16 64 64 4
finish_cases

# ---- Native process history (no containers) ----
# 2026-09-02 (same hw, +page-count metrics +flush re-check loop):
#   sagg/ragg columns are 0 (moved to crowdb-common histograms). p50/p99
#   use coarser histogram buckets (500us increments) — not directly
#   comparable to the 2026-08-27 exact values. 128T and 256T show
#   r2=0us/r3=0us (replicas not responding) after the fixture data reset —
#   consensus instability from the clean→run transition, not storage-
#   related. A standalone compare-128t run (fresh deploy, no prior
#   sub-tests) gets 198K ops/s with co=15.75/16, confirming the storage
#   changes are fine. See doc/working/todo_tree_count.md for the open
#   issue.
#
#   T    C    W    win  co        ops/s     WAL/node  p50    p99     err   sagg  ragg  r2    r2tps    r3    r3tps    enq   wait
#   1    1    2    32   1.0/16    3,943     78,876    500    500     0     0     0     120   79,079   123   79,079   0     0
#   16   2    2    32   4.9/16    57,867    238,370   500    1000    0     0     0     166   159,700  427   159,699  0     0
#   64   4    2    32   6.9/16    151,431   439,470   500    1000    0     0     0     150   280,382  152   280,382  0     0
#   128  4    4    32   4.4/16    168,756   775,238   1000   5000    0     0     0     340   495,894  360   495,893  0     0
#   256  8    4    32   3.9/16    22,503    282,738   5000   5000    256   0     0     0     498,403  0     498,402  0     0
#   512  16   4    64   58.0/64   264,130   91,061    5000   5000    0     0     0     543   91,257   560   91,256   0     0
#   1000 16   4    64   26.8/64   225,760   168,712   5000   50000   0     0     0     0     91,262   0     91,262   0     0
#
# 2026-09-02 (same hw, +mem-block WAL wipe fix + bench on group 1):
#   wipe_user_data now calls IoBackend::remove_dir_all (clears mem-block
#   in-memory segments — tokio::fs::remove_dir_all was a no-op on them)
#   and calls remove_group BEFORE create_group_with_wal. Bench runs on
#   group 1 (group 0 sysdata preserved). The 256T consensus instability
#   is fixed: 22K→181K ops/s, 256→0 errors. r2/r3 now respond at all
#   thread counts. fixture data reset frees RSS between sub-tests (128MB–9.5GB
#   freed). 512T slightly lower (264K→240K) — within run-to-run noise.
#
#   T    C    W    win  co        ops/s     WAL/node  p50    p99     err   sagg  ragg  r2    r2tps    r3    r3tps    enq   wait
#   1    1    2    32   1.0/16    6,258     125,165   500    500     0     0     0     75    125,352  76    125,267  0     0
#   16   2    2    32   4.3/16    65,897    303,919   500    500     0     0     0     250   178,925  152   178,894  0     0
#   64   4    2    32   6.1/16    155,883   511,013   500    1000    0     0     0     169   207,256  159   207,215  0     0
#   128  4    4    32   4.9/16    184,812   749,977   1000   5000    0     0     0     233   418,032  220   417,987  0     0
#   256  8    4    32   3.7/16    181,204   983,928   5000   5000    0     0     0     502   652,144  414   652,048  0     0
#   512  16   4    64   57.4/64   239,669   83,509    5000   5000    0     0     0     868   83,660   645   83,614   0     0
#   1000 16   4    64   27.7/64   220,607   159,609   5000   50000   0     0     0     0     83,662   0     83,617   0     0
#
# Zero-copy crowdb-rpc + event-write beats legacy at every thread count.
# Peak ~234K at 512T (2026-08-27); ~264K at 512T (2026-09-02, +page-count
# metrics +flush re-check loop, +13.1%). Was ~124K with legacy, ~191K
# without event-write. 1000T now uses co=64 (was co=32) — 1000 threads
# fill 64-key batches as well as 512T. Coalesce fill: 48% at co=16
# (256T), 42% at co=64 (512T/1000T) — batches not full, bottleneck is
# accept-round latency. Inflight window NEVER full (enq=0 at all configs).
# Inter-replica: r2≈r3 (symmetric). Zero errors across all configs
# (except 256T:8C on 2026-09-02 — pre-existing consensus instability).
# See doc/design/kv/kv-write-flow-analysis.md for full analysis.
#
# Intel i9-7960X (2026-09-10, 16c/32t, Linux 6.11, x86_64):
#   Same build/config as AMD 2026-09-02. mem-block backend, 20s, 512B
#   values, 1M keyspace, 3-node cluster, event-write + peer-pool=4.
#   High-concurrency (64T+) within 3-12% of AMD. 512T/1000T slightly
#   faster (Intel has higher memory bandwidth at saturation). 1T and 16T
#   are 88% and 74% slower — low-concurrency per-op overhead is much
#   higher on Intel; documented in doc/working/regression-perf-review.md.
#
#   T    C    W    win  co        ops/s     avg     WAL/node  p50    p99     err   sagg  ragg  r2    r2tps    r3    r3tps    enq   wait
#   1    1    2    32   1.0/16    755       1313    15,099    1343   1769    0     0     0     643   15,304   647   15,304   0     0
#   16   2    2    32   5.3/16    17,095    926     64,200    880    1597    0     0     0     334   49,307   349   49,306   0     0
#   64   4    2    32   10.9/16   123,376   511     226,178   481    925     0     0     0     194   177,494  197   177,494  0     0
#   128  4    4    32   7.5/16    163,024   779     434,636   720    1548    0     0     0     269   257,979  286   257,978  0     0
#   256  8    4    32   5.3/16    174,940   1457    657,040   1343   3129    0     0     0     408   400,109  353   400,107  0     0
#   512  16   4    64   58.4/64   247,570   2060    84,819    1933   4325    0     0     0     4189  85,028   773   85,026   0     0
#   1000 16   4    64   28.5/64   234,664   4250    164,771   3948   10551   0     0     0     1303  165,191  1142  165,189  0     0
#
# macOS M5 Pro (2026-08-19, legacy transport, pre-zero-copy):
#   coalesce=32, max_inflight=128, same workload.
#
#   T    C    ops/s     WAL      p50    p99    p999   err
#   1    1    10,144    304,358  95     153    211    0
#   4    2    21,879    449,508  178    307    380    0
#   16   4    47,260    276,795  330    523    619    0
#   32   16   57,889    170,600  537    894    1,046  0
#   64   32   69,908    104,777  888    1,440  1,745  0
#   128  32   78,155    86,840   1,590  2,654  3,794  0
#   256  32   87,448    86,619   2,870  4,704  7,004  0
#
# macOS peak ~87K at 256T (legacy). M5 Pro faster at 1T (10K vs 3.7K, 2.7x)
# due to lower per-op overhead, but saturates earlier (non-SMT 18-core vs
# 32-thread SMT AMD). Zero-copy comparison on M5 Pro pending.

# ---- Host-network container history ----
# 2026-10-27: AMD Ryzen 9 5950X, 32 logical CPUs; three 8GiB/4-CPU containers,
# no cpuset, mem-block Tree/WAL, server workers=2, window/coalesce=32/16,
# peer pool=4, event-write; management/RPC 10135/11135 + offsets 0/1/2.
# Release CLI, 5/20/5s; 100k x 512B (old native: 1M keys and clean-start writes).
# Each case prefills/verifies then overwrites; same containers retained between cases.
# Image: sha256:eb0dd67818e3f3ec2f81ba6969db8320f2dcd8a9c92c1bf727d1d1f7cce9d09d
# Source tree: b984ab3e0838bee528d880e6bb83fc34c3c54663; HEAD: 9380e341d634cca89d8ac678aa0340e56cd7a3de.
# Raw reports + CLI checksum: .crowdb-runtime/artifacts/bench/kv-review-write/
# All zero errors/incomplete. Measurement-end memory MiB per node: 1T=689/680/680,
# 16T=2533/2726/2725, 64T=3576/5214/5230; retained state grows between cases.
# 2026-10-27 write_1t_1c_win32_coales16 host-container ops/s=4835.45 latency_ns={"avg":206663.62079020566,"count":96709,"max":1445419,"min":139992,"p50":196607,"p99":321535,"p999":536575} passed=true
# 2026-10-27 write_16t_2c_win32_coales16 host-container ops/s=47325.25 latency_ns={"avg":337976.5844395962,"count":946505,"max":17705262,"min":159192,"p50":327679,"p99":540671,"p999":983039} passed=true
# 2026-10-27 write_64t_4c_win32_coales16 host-container ops/s=71047.35 latency_ns={"avg":900637.8576456405,"count":1420947,"max":45301604,"min":162352,"p50":892927,"p99":1327103,"p999":2637823} passed=true

# 2026-10-27: unrestricted CPU follow-up, fresh 3-node host-network cluster.
# No CPU quota/cpuset; Docker NanoCpus/CpuQuota=0, container and ancestor cpu.max=max.
# 20GiB/node, CLI memory guard=90%; same image/source/server profile above.
# 100k x 512B, 64T/4C, workers=2, window/coalesce=32/16, peer pool=4,
# default election profile; prefill + overwrite, 5/20/5s; zero errors/incomplete.
# CLI SHA256: 43a7ad7904bde40d11c1408b44b0bf2b246133867bdde4af600facc1a4aa0129.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-write-unlimited-cpu/
# 45.1% below historical 151431 TPS. Prior 71047.35 run reused containers after
# 1T/16T cases and used 8GiB/4-CPU; do not attribute all change to CPU quota.
# 2026-10-27 write_64t_4c_win32_coales16 host-container ops/s=83187.05 latency_ns={"avg":769169.0450635045,"count":1663741,"max":18326773,"min":214733,"p50":770047,"p99":1212415,"p999":1564671} passed=true

# 2026-10-27: same unrestricted CPU fresh-cluster case; Tokio server workers
# now match available logical CPUs (32), independently of RPC workers=2.
# CLI Tokio also explicitly set to available CPUs (32); prior CLI used default.
# Other conditions: 20GiB, 90% guard, 100k x 512B, 64T/4C, win32/coalesce16,
# peer pool4, event-write, default election profile, 5/20/5s, same image/CLI.
# Zero errors/incomplete/elections; sampled throttling=0.
# Mean coalescing estimate: 2314913 server puts / 151039 proposal rounds =
# 15.3266 keys/round (95.79% of 16), inflight_enqueued=0.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-write-unlimited-tokio32/
# 2026-10-27 write_64t_4c_win32_coales16 host-container ops/s=115746.6 latency_ns={"avg":552682.1046825566,"count":2314932,"max":3584362,"min":222993,"p50":536575,"p99":905215,"p999":1310719} passed=true

# 2026-10-27: Chosen/BatchChosen notification now use send_one_way (no reply completion).
# Same fresh 3-node host-network, release CLI, 20GiB/node, no CPU quota/cpuset,
# Tokio32/RPC2, 64T/4C, 100k x 512B, window32/coalesce16, peer pool4, 5/20/5s.
# Docker image ID: sha256:f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff
# Source tree: 2e5e29a36a45cecbba35ced31fae980d0e5cb509; same CLI SHA256 as prior run.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-write-oneway/
# WAL append/s per node=7585.27/7585.22/7585.32; no reaped counters in any node log.
# Zero errors/incomplete/elections; full verification passed. Leader node3 (prior node2).
# TPS +0.35% versus 115746.6: no demonstrated material throughput improvement.
# 2026-10-27 write_64t_4c_win32_coales16 host-container ops/s=116152.15 latency_ns={"avg":550749.4989313586,"count":2323043,"max":3217648,"min":226702,"p50":536575,"p99":892927,"p999":1253375} passed=true

# 2026-10-27: user-requested native diagnostic, three standalone release KV servers.
# Same staged server binary as notification-fix image, now directly launched via
# external systemd user services; no Docker. 20GiB/node, swap0, no CPU quota, CPUs0-31.
# Same workload/knobs: 100k x 512B, 64T/4C, Tokio32/RPC2, win32/coalesce16,
# peer pool4, event-write, mem-block Tree/WAL, 5/20/5s. Fresh data; leader=node2.
# CLI rebuilt only to add read-only native PID evidence; workload code unchanged.
# Zero errors/incomplete/elections, full verification passed, no reaped counters.
# +9.78% TPS versus latest host-container; one run, not an isolated Docker-overhead proof.
# Leader WAL=8269.5 append/s over complete internal 15s log windows; approx8553.6/s
# from HTTP deltas has 0.557s end-sampling skew, so do not mix those intervals.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-write-native/ (attempt-1: preflight failure).
# 2026-10-27 write_64t_4c_win32_coales16 native-process ops/s=127516.45 latency_ns={"avg":501723.8486289416,"count":2550329,"max":2439695,"min":225471,"p50":489471,"p99":790527,"p999":1011711} passed=true

# 2026-10-27: unrestricted memory + CPU container baseline, fresh deployment per case.
# AMD 5950X/32 logical CPUs; no --memory/--memory-swap/--cpus/--cpuset;
# Docker Memory/MemorySwap/NanoCpus/CpuQuota=0, cpuset empty; cgroup memory.max=max,
# cpu.max=max. Tokio32, server RPC workers2, peer pool4, event-write, mem-block.
# 100k x 512B, prefill + verify + overwrite + verify; 5/20/5s.
# CLI workers2 for 1/16/64T; workers4 for 128/256/512/1000T.
# Profile A: window32/coalesce16; profile B (512/1000T): window64/coalesce64.
# Same notification-fix image sha256:f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff.
# CLI now accepts memory.max=max; finite-limit runs retain the 90% guard.
# Raw reports, image inspect, cgroup timeline, logs and CLI hash:
# .crowdb-runtime/artifacts/bench/kv-review-unlimited/write-*/
# External observer stops owned nodes only if host MemAvailable falls below4GiB;
# no intervention occurred in these write cases. This is not a container memory quota.
# 2026-10-27 write_1t_1c_win32_coales16 host-container ops/s=4761.2 latency_ns={"avg":209877.27959337982,"count":95224,"max":1770434,"min":153041,"p50":207871,"p99":274431,"p999":350207} passed=true
# 2026-10-27 write_16t_2c_win32_coales16 host-container ops/s=45774.45 latency_ns={"avg":349418.2195482414,"count":915489,"max":1675024,"min":167951,"p50":339967,"p99":536575,"p999":618495} passed=true
# 2026-10-27 write_64t_4c_win32_coales16 host-container ops/s=129753.35 latency_ns={"avg":493052.27683639765,"count":2595067,"max":4463586,"min":199662,"p50":483327,"p99":761855,"p999":1007615} passed=true
# 2026-10-27 write_128t_4c_win32_coales16 host-container ops/s=158281.3 latency_ns={"avg":808343.0829668444,"count":3165626,"max":2721382,"min":264362,"p50":782335,"p99":1368063,"p999":1630207} passed=true
# 2026-10-27 write_256t_8c_win32_coales16 host-container ops/s=177685.1 latency_ns={"avg":1440245.5994630388,"count":3553702,"max":5921808,"min":281762,"p50":1392639,"p99":2605055,"p999":3096575} passed=true
# 2026-10-27 write_512t_16c_win64_coales64 host-container ops/s=194868.8 latency_ns={"avg":2626473.476040033,"count":3897376,"max":8206807,"min":491744,"p50":2523135,"p99":4849663,"p999":5734399} passed=true
# 2026-10-27 write_1000t_16c_win64_coales64 host-container ops/s=209807.5 latency_ns={"avg":4764418.7959129205,"count":4196150,"max":24693742,"min":625525,"p50":4521983,"p99":9764863,"p999":12582911} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 100000 keys, value_size=16384, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/sentinel/; full verification passed.
# Single fresh deployment, e2e election profile; not a continuous three-run sentinel pass.
# 2026-10-27 largeval_16k_run1 host-container ops/s=3678.0 latency_ns={"avg":271747.8872349103,"count":73560,"max":58775148,"min":186252,"p50":266239,"p99":364543,"p999":602111} passed=true

# 2026-10-27: Paxos-KV naming migration comparison, no CPU/memory quota/cpuset.
# 3-node host-network mem-block; Tokio32, server/CLI RPC2, peer pool4, event-write.
# 100k x 512B, 64T/4C, window32/coalesce16; warmup/measurement/tail=5/20/5s.
# Command is now bench paxos-kv. Image e1fc2fee3decd3e0b4693aa6d9518a9bcf7ac097b1e2663b62292161563282a4.
# Clean run: 128782.6 TPS, p99=765951ns, p999=970751ns, avg=496774ns;
# zero errors/incomplete, full verification passed. Before rename:129753.35 TPS (-0.748%).
# One comparison only, not a statistical equivalence claim.
# Earlier attempt:122869.55 TPS, p99=843775ns; clippy overlapped, excluded from perf comparison.
# Both raw runs and restore evidence: workshop/crowdb-ssw/paxos-kv/artifacts/naming-validation-20261027/.
