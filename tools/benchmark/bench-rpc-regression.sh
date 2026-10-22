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
# topology    Nagle  loaders  connections  socket_workers  TPS          avg_us  p90_us   p99_us   max_ms  errors  fallback
# raw-server  off    1000     32           16              1,203,469.3  829.48  1589.25  7143.42  22.432  0       0
# raw-server  on     1000     32           16              2,167,732.3  459.98  851.97   1548.29  5.356   0       0
# OCI         off    1000     32           16              1,094,684.9  912.03  1441.79  9109.50  38.539  0       0
# OCI         on     1000     32           16              2,112,923.9  471.92  888.83   1671.17  6.659   0       0
# Raw Nagle-off TPS repeats: 1,207,836.4 / 1,203,469.3 / 1,202,207.6.
# Raw Nagle-off worst max over those repeats: 24.996 ms.
# Raw Nagle-on TPS repeats: 2,177,406.0 / 2,165,395.3 / 2,167,732.3.
# Raw Nagle-on worst max over those repeats: 7.170 ms.
# OCI Nagle-off TPS repeats: 1,092,294.1 / 1,100,987.3 / 1,094,684.9.
# OCI Nagle-off worst max over those repeats: 38.539 ms.
# OCI Nagle-on TPS repeats: 2,116,257.1 / 2,112,923.9 / 2,100,983.2.
# OCI Nagle-on worst max over those repeats: 7.063 ms.
# Per peer: io_engines=1, io_workers=16, send_queue_capacity=4096.
# RDMA is explicitly disabled; CQ/SQ/RQ/registered-slot settings are n/a.
# Canonical JSON/CSV/TSV, full counters, errors and binary hashes:
# .crowdb-runtime/rdma-check/artifacts/bench/
# accepted-944274af-tcp-native-off-final/, accepted-944274af-tcp-native-on-final/,
# accepted-944274af-tcp-oci-off-final/ and accepted-944274af-tcp-oci-on-final/.
#
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
exec pixi run -- python tools/benchmark/rpc_regression.py --transport tcp "$@"
