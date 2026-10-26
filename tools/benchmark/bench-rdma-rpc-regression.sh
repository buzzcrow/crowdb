#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Shared coroutine workload, structured readiness and canonical result files.
#
# Measured 2026-10-23, clean source 944274af, matching release CLI/server, v4.
# Environment: AMD Ryzen 9 5950X, 16 cores / 32 threads, about 62 GiB RAM.
# HCA: rocep35s0f0 port 1, 100 Gb/s, PCIe 8 GT/s x4; one physical host.
# raw-server = two native host processes. OCI = two non-root UID 1000
# host-network containers; RDMA shares this HCA with independent QPs and
# unlimited memlock. TCP OCI has no RDMA device mappings. Runtime image:
# sha256:e3c9c5c6d5fc3424ac3f22cf4cdcc9b1c872f47336484f54962b2541c4ed6cef.
# CPU affinity/frequency were not pinned. Native builds used -j32.
# Workload: coroutine, 128-byte payload, one in-flight RPC per loader,
# 1 s warmup + 10 s validated completion window; drain counted separately.
# request_timeout_secs=5. Peak rows select median TPS of three repeats;
# avg/P90/P99/max are from that same trial. All 26 final trials have valid
# reports, errors_total=0, report_errors=0 and fallback_count=0.
#
# topology    loaders  QPs  CQ/pollers  socket_workers  aggregation  TPS          avg_us  p90_us  p99_us   max_ms  errors  fallback
# raw-server  2048     16   16          1               on           3,026,938.6  675.55  823.29  1097.73  5.788   0       0
# OCI         2048     16   16          1               on           3,236,760.8  631.64  704.51  892.93   3.559   0       0
# Raw TPS repeats: 3,036,947.8 / 3,026,938.6 / 3,026,730.4.
# OCI TPS repeats: 3,236,760.8 / 3,228,268.4 / 3,251,586.3.
#
# Progression, raw-server (one 10 s trial each):
# loaders  QPs  CQ/pollers  aggregation  TPS          avg_us  p90_us  p99_us  max_ms  errors
# 1        1    1           off          60,964.4     15.88   15.94   21.25   0.361   0
# 512      8    8           on           2,290,244.8  222.58  257.02  299.01  0.656   0
#
# Per peer: io_engines=1, io_workers=1; connection_count=16 counts RC QPs.
# qpair_number in connection status is the provider QP ID, not a count.
# Per QP: sq_depth=rq_depth=posted_receive_target=1024,
# send_queue_capacity=4096 logical RPCs. cq_depth=2048 per CQ (provider 4095),
# wire_slot_capacity=1024 B, send_arena_bytes=receive_arena_bytes=1 MiB;
# registered peak=34 MiB per peer including shared pools. poll_batch/
# verbs_poll_batch/poll_empty_budget=256/32/256. aggregation_max_bytes=1024,
# aggregation_max_frames=256; complete frames only, bounded by both slots.
# Strict RDMA: TCP fallback disabled. Contiguous RQ is primed before CM;
# single-loader warmup completed 57,184 RPCs, removing the previous 1.46 s
# cold-response tail without changing warmup/deadlines. High-load latency
# includes queueing; P90 is not maximum latency. This is the highest repeated
# tested configuration, not a measured hardware ceiling.
# Canonical JSON/CSV/TSV, full counters, errors and binary hashes:
# .crowdb-runtime/rdma-check/artifacts/bench/
# accepted-944274af-rdma-native-final/ and accepted-944274af-rdma-oci-final/.
#
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
exec pixi run -- python tools/benchmark/rpc_regression.py --transport rdma "$@"
