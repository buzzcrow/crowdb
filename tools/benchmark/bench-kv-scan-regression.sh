#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Paxos-KV scan: externally started 3-node host-network containers, mem-block.
# Run with pixi run bash tools/benchmark/bench-kv-scan-regression.sh.
# Set KV_BENCH_DEPLOYMENT; optional KV_BENCH_CASES, DURATION, WARMUP, TAIL,
# KEYSPACE, CLI and LOG_ROOT use the KV_BENCH_ prefix.
# Defaults: 5s warmup / 20s measurement / 5s tail. <20s is a smoke run.
# Keep every dated result, including failures. Old native process data below
# remains a useful reference; compare code, resource and dataset conditions.
set -euo pipefail
cd "$(dirname "$0")/../.."
source tools/benchmark/bench-kv-common.sh

run_case() {
    local label="$1" limit="$2" threads="$3" mode="$4" size="$5"; shift 5
    selected "$label" || return 0
    local policy=leader mix=() KEYSPACE="$KEYSPACE"
    if [[ "$label" == largeval_16k || "$label" == mixed_1k ]]; then
        KEYSPACE="${KV_BENCH_LARGE_KEYSPACE:-100000}"
    fi
    [[ "$mode" != minslot ]] || policy=any-replica
    [[ "$label" != mixed_1k ]] || mix=(--mixed-values)
    prepare_dataset --value-size "$size" "${mix[@]}"
    invoke "$label" scan --key-space "$KEYSPACE" --scan-limit "$limit" --value-size "$size" \
        --loader-num "$threads" --connections "$threads" --read-mode "$mode" \
        --read-endpoint-policy "$policy" "${mix[@]}" "$@"
}
run_case bounded_10 10 1 linearizable 64
run_case bounded_1k 1000 1 linearizable 64
run_case bounded_10k 10000 1 linearizable 64
run_case full_keyspace "$KEYSPACE" 1 linearizable 64
[[ "$KEYSPACE" -ge 11 ]] || { echo 'scan requires KEYSPACE >=11' >&2; exit 2; }
printf -v deep_key 'k%020d' "$((KEYSPACE - 11))"
printf -v reverse_key 'k%020d' 10
run_case deep_pag_10 10 1 linearizable 64 --scan-start-after "$deep_key"
run_case mixed_1k 1000 1 linearizable 64
run_case minslot_1k 1000 1 minslot 64
run_case reverse_1k 1000 1 linearizable 64 --reverse
run_case reverse_deep_10 10 1 linearizable 64 --reverse --scan-start-after "$reverse_key"
# Actually populate 16KiB values; historical runs below used 64B prepopulation.
run_case largeval_16k 1000 1 linearizable 16384
for threads in 4 16 32; do
    run_case "lin_${threads}t" 1000 "$threads" linearizable 64
    run_case "minslot_${threads}t" 1000 "$threads" minslot 64
done
run_case reverse_4t 1000 4 minslot 64 --reverse
finish_cases

# ---- Native process history (no containers) ----
# Reference results (2026-08-19, Apple M5 Pro, 18c, arm64, macOS 26.5):
#   10s mem mode, 3-node cluster, 100k pre-populated keys, 64B values
#   unless noted. Post-R67 (spawn_blocking maintenance).
#
#   label            limit   T:C   scans/s  avg_us  p99_us   err  notes
#   bounded_10       10      1:1   21320    46      73       0    O(limit) headline
#   bounded_1k       1000    1:1   4708     211     239      0    typical scan
#   bounded_10k      10000   1:1   562      1777    1911     0    large bounded
#   full_100k        100000  1:1   49       20411   22864    0    pagination
#   deep_pag_10      10      1:1   21003    46      66       0    O(limit) pushdown
#   mixed_1k         1000    1:1   1043     957     1175     0    64B:70%,1KiB:20%,16KiB:10%
#   minslot_1k       1000    1:1   4721     211     241      0    MinSlot routing
#   largeval_16k     1000    1:1   101      9893    13496    0    R67: 16KiB values
#   lin_4t           1000    4:4   15504    257     409      0    max leader throughput
#   minslot_4t       1000    4:4   16232    245     358      0    +4.7% vs lin
#   lin_16t          1000    16:16 32384    492     781      0
#   minslot_16t      1000    16:16 32217    495     816      0    -0.5% vs lin
#   lin_32t          1000    32:32 38859    820     2416     0
#   minslot_32t      1000    32:32 36684    869     1416     0    -5.6% throughput, -41% p99
#
# Reference results (2026-09-02, AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux):
#   20s mem mode, 3-node cluster, 100k pre-populated keys (group 1),
#   64B values unless noted. +mem-block WAL wipe fix + bench on group 1.
#
#   label            limit   T:C   scans/s  avg_us  p99_us   err  notes
#   bounded_10       10      1:1   11864    83      500      0    O(limit) headline
#   bounded_1k       1000    1:1   1316     724     5000     0    typical scan
#   bounded_10k      10000   1:1   134      7031    50000    0    large bounded
#   full_100k        100000  1:1   13       70091   100000   0    pagination
#   deep_pag_10      10      1:1   12577    78      500      0    O(limit) pushdown
#   mixed_1k         1000    1:1   1120     845     1000     0    64B:70%,1KiB:20%,16KiB:10%
#   minslot_1k       1000    1:1   1126     846     5000     0    MinSlot routing
#   largeval_16k     1000    1:1   1131     832     1000     0    16KiB values
#   lin_4t           1000    4:4   4270     893     5000     0    max leader throughput
#   minslot_4t       1000    4:4   4083     934     5000     0    -4.4% vs lin
#   lin_16t          1000    16:16 21897    700     5000     0
#   minslot_16t      1000    16:16 21954    698     5000     0    +0.3% vs lin
#   lin_32t          1000    32:32 24215    1289    5000     0
#   minslot_32t      1000    32:32 24253    1285    5000     0    +0.2% throughput
#
# Analysis: doc/design/kv/kv-scan-flow-analysis.md § Latest Benchmark Results.
#
# Intel i9-7960X (2026-09-10, 16c/32t, Linux 6.11, x86_64):
#   Same build/config as AMD 2026-09-02. mem-block backend, 20s, 100k
#   pre-populated keys (group 1), 64B values unless noted, 3-node cluster.
#   Zero errors across all configs. Low-concurrency (1T, 4T) 56-71% slower
#   than AMD — per-scan overhead is much higher on Intel. 16T-32T within
#   6-30%. p99 values are exact (not coarser 500us buckets like AMD).
#   Gaps > 30% documented in doc/working/regression-perf-review.md.
#
#   label            limit   T:C   scans/s  avg_us  p50_us  p99_us   err  notes
#   bounded_10       10      1:1   4283     231     166     692      0    O(limit) headline
#   bounded_1k       1000    1:1   417      2273    2277    2883     0    typical scan
#   bounded_10k      10000   1:1   59       15938   15073   21626    0    large bounded
#   full_100k        100000  1:1   8        115054  114294  163577   0    pagination
#   deep_pag_10      10      1:1   3664     270     178     716      0    O(limit) pushdown
#   mixed_1k         1000    1:1   427      2213    2228    2850     0    64B:70%,1KiB:20%,16KiB:10%
#   minslot_1k       1000    1:1   408      2325    2326    2932     0    MinSlot routing
#   largeval_16k     1000    1:1   421      2234    2244    2818     0    16KiB values
#   lin_4t           1000    4:4   1747     2173    2162    3014     0    max leader throughput
#   minslot_4t       1000    4:4   1593     2389    2375    3162     0    -8.9% vs lin
#   lin_16t          1000    16:16 16969    886     876     1351     0
#   minslot_16t      1000    16:16 15382    979     962     1515     0    -9.3% vs lin
#   lin_32t          1000    32:32 22820    1342    1327    2072     0
#   minslot_32t      1000    32:32 22823    1341    1318    2088     0    +0.0% vs lin

# ---- Host-network container history ----
# 2026-10-27: AMD 5950X 32 logical CPUs; 3 x 8GiB/4-CPU containers, no cpuset;
# workers=2, window/coalesce=32/16, peer pool=4, event-write, default election profile.
# mem-block Tree/WAL, host network, management/RPC 10135/11135 + offsets 0/1/2.
# 1M x 64B, 5/20/5s; same deployment after read matrix and a new dataset prefill.
# Image: sha256:eb0dd67818e3f3ec2f81ba6969db8320f2dcd8a9c92c1bf727d1d1f7cce9d09d
# Source tree: b984ab3e0838bee528d880e6bb83fc34c3c54663.
# Raw reports: .crowdb-runtime/artifacts/bench/kv-review-scan/; server logs in kv-review-read/.
# All zero errors/incomplete; full_keyspace now means 1M keys, unlike historical full_100k.
# 2026-10-27 bounded_10 host-container ops/s=11496.8 latency_ns={"avg":86853.30406286966,"count":229936,"max":5065726,"min":56421,"p50":83967,"p99":139263,"p999":241663} passed=true
# 2026-10-27 bounded_1k host-container ops/s=1361.15 latency_ns={"avg":734435.0102119531,"count":27223,"max":2934998,"min":601728,"p50":708607,"p99":983039,"p999":1327103} passed=true
# 2026-10-27 full_keyspace host-container ops/s=0.55 latency_ns={"avg":1659944006.909091,"count":11,"max":1877296928,"min":1406636784,"p50":1635778559,"p99":1877296928,"p999":1877296928} passed=true
# 2026-10-27 deep_pag_10 host-container ops/s=12500.65 latency_ns={"avg":79858.59522104851,"count":250013,"max":1251827,"min":54341,"p50":77823,"p99":127999,"p999":202751} passed=true
# 2026-10-27 minslot_1k host-container ops/s=778.65 latency_ns={"avg":1283793.9630771207,"count":15573,"max":3995902,"min":568707,"p50":1236991,"p99":2752511,"p999":3407871} passed=true
# 2026-10-27 reverse_1k host-container ops/s=370.05 latency_ns={"avg":2701686.825564113,"count":7401,"max":321203070,"min":2276460,"p50":2490367,"p99":4390911,"p999":4947967} passed=true
# 2026-10-27 reverse_deep_10 host-container ops/s=6156.7 latency_ns={"avg":162273.8791560414,"count":123134,"max":1255176,"min":123632,"p50":160767,"p99":216063,"p999":245759} passed=true

# 2026-10-27: mixed + large-value scan, fresh 3 x 12GiB/4-CPU containers;
# otherwise same image/server config above; 5/20/5s. 100k keys for these cases.
# mixed: actual 70% 64B / 20% 1KiB / 10% 16KiB; largeval: actual 16KiB throughout.
# Historical largeval rows with 64B prepopulation are not comparable as 16KiB workloads.
# Raw reports + logs + CLI checksum: .crowdb-runtime/artifacts/bench/kv-review-scan-large/
# 2026-10-27 mixed_1k host-container ops/s=224.3 latency_ns={"avg":4456280.788898796,"count":4486,"max":13092660,"min":1418798,"p50":4063231,"p99":11206655,"p999":12451839} passed=true
# 2026-10-27 largeval_16k host-container ops/s=31.55 latency_ns={"avg":31667539.259904914,"count":631,"max":40593476,"min":24946544,"p50":31850495,"p99":37486591,"p999":40593476} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/scan/; full verification passed.
# 2026-10-27 bounded_10 host-container ops/s=13687.45 latency_ns={"avg":72957.76122287205,"count":273749,"max":1142009,"min":52030,"p50":71679,"p99":102399,"p999":130559} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/scan/; full verification passed.
# 2026-10-27 bounded_1k host-container ops/s=1404.55 latency_ns={"avg":711771.9990744366,"count":28091,"max":1835865,"min":501384,"p50":708607,"p99":1081343,"p999":1261567} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/scan/; full verification passed.
# 2026-10-27 deep_pag_10 host-container ops/s=13140.55 latency_ns={"avg":75991.47226333753,"count":262811,"max":1056270,"min":51360,"p50":73215,"p99":134143,"p999":186367} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/scan/; full verification passed.
# 2026-10-27 full_keyspace host-container ops/s=1.0 latency_ns={"avg":994163950.65,"count":20,"max":1068703819,"min":900398625,"p50":994050047,"p99":1068703819,"p999":1068703819} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/scan/; full verification passed.
# 2026-10-27 minslot_1k host-container ops/s=1272.2 latency_ns={"avg":785748.5947178117,"count":25444,"max":2498383,"min":519425,"p50":770047,"p99":1245183,"p999":1646591} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/scan/; full verification passed.
# 2026-10-27 reverse_1k host-container ops/s=399.35 latency_ns={"avg":2503342.7186678355,"count":7987,"max":4551340,"min":1948547,"p50":2473983,"p99":3506175,"p999":4079615} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 1000000 keys, value_size=64, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/scan/; full verification passed.
# 2026-10-27 reverse_deep_10 host-container ops/s=11787.55 latency_ns={"avg":84681.64017119758,"count":235751,"max":1082029,"min":57210,"p50":82431,"p99":134143,"p999":200703} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 100000 keys, value_size=64, mixed=True.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/scan-mixed/; full verification passed.
# 2026-10-27 mixed_1k host-container ops/s=181.1 latency_ns={"avg":5518864.853948095,"count":3622,"max":10438412,"min":1641084,"p50":5439487,"p99":8781823,"p999":10223615} passed=true

# 2026-10-27: no memory/CPU quota/cpuset, Tokio32/RPC2, 3-node host-network mem-block.
# Image f576b3d9387c58c81df9c0e99bf1ca85659fac77ae05e285b539fb7840e7d9ff; 5/20/5s.
# Dataset: 100000 keys, value_size=16384, mixed=False.
# Raw: .crowdb-runtime/artifacts/bench/kv-review-unlimited/scan-large/; full verification passed.
# 2026-10-27 largeval_16k host-container ops/s=36.4 latency_ns={"avg":27400801.240384616,"count":728,"max":41199855,"min":18380413,"p50":26083327,"p99":39845887,"p999":41199855} passed=true
