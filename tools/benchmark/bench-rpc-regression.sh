#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
# Shared coroutine workload, structured readiness and canonical result files.
#
# Measured 2026-10-23, source 018da2fc, matching release CLI/server, protocol v4.
# Environment: AMD Ryzen 9 5950X, 16 cores / 32 threads, about 62 GiB RAM.
# HCA: rocep35s0f0 port 1, 100 Gb/s, PCIe 8 GT/s x4; one physical host.
# raw-server = two native host processes. OCI = two non-root UID 1000
# host-network containers sharing this HCA, with independent QPs and memlock
# unlimited. Image: sha256:e3c9c5c6d5fc3424ac3f22cf4cdcc9b1c872f47336484f54962b2541c4ed6cef.
# CPU affinity/frequency were not pinned. Native builds used -j32.
# Workload: coroutine, 128-byte payload, one in-flight RPC per loader,
# 1 s warmup + 10 s validated completion window, drain counted separately.
# Peak rows select median TPS of three repeats; latency is from that same row.
#
# topology     Nagle  loaders  connections  socket_workers  TPS          avg_us  p90_us   p99_us   errors  fallback
# raw-server   off    1000     32           16              1,206,800.2  827.23  1654.78  6815.74  0       0
# raw-server   on     1000     32           16              2,197,134.7  453.80  839.68   1515.52  0       0
# OCI          --     --       --           --              not measured in this performance snapshot
# Raw Nagle-off TPS repeats: 1,206,800.2 / 1,199,487.9 / 1,214,054.5.
# Raw Nagle-on TPS repeats: 2,197,134.7 / 2,186,038.9 / 2,198,891.2.
# Per peer: io_engines=1, io_workers=16, send_queue_capacity=4096.
# RDMA is explicitly disabled; CQ/SQ/RQ/registered-slot settings are n/a.
# The no-device OCI TCP functional corpus passed; no OCI TPS is inferred from it.
# Original canonical artifacts: .crowdb-runtime/rdma-check/artifacts/bench/
# accepted-018da2fc-tcp-off/ and accepted-018da2fc-tcp-on/.
#
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
exec pixi run -- python tools/benchmark/rpc_regression.py --transport tcp "$@"
