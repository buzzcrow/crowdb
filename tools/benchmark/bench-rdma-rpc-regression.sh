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
# topology     loaders  QPs  CQ/pollers  socket_workers  aggregation  TPS          avg_us  p90_us  p99_us   errors  fallback
# raw-server   2048     16   16          1               on           2,901,185.6  755.61  802.82  954.37   0       0
# OCI          2048     16   16          1               on           3,073,435.4  725.12  741.38  946.18   0       0
# Raw TPS repeats: 2,911,356.0 / 2,814,582.2 / 2,901,185.6.
# OCI TPS repeats: 3,073,435.4 / 3,067,371.3 / 3,121,091.5.
#
# Progression, raw-server (one 10 s trial each):
# loaders  QPs  CQ/pollers  aggregation  TPS          avg_us  p90_us  p99_us  errors
# 1        1    1           off          57,620.7     18.57   16.00   30.08   0
# 512      8    8           on           2,223,708.2  249.36  243.71  284.67  0
#
# Per peer: io_engines=1, io_workers=1; per QP: sq_depth=rq_depth=
# posted_receive_target=1024, send_queue_capacity=4096 logical RPCs.
# cq_depth=2048 requested per CQ (provider 4095); wire_slot_capacity=1024 B;
# send_arena_bytes=receive_arena_bytes=1 MiB per QP; registered total 34 MiB
# per peer, including shared pools. poll_batch/verbs_poll_batch/
# poll_empty_budget=256/32/256. aggregation_max_bytes=1024,
# aggregation_max_frames=256; complete frames only, bounded by both slots.
#
# Startup-tail caveat: observed cold first responses reached about 1.5-1.8 s;
# the single-loader trial had zero warmup completions and max_ns=1460998030.
# Completion-window latency includes requests submitted during warmup. P90 is
# not maximum latency; retain this snapshot during the startup investigation.
# Original canonical artifacts: .crowdb-runtime/rdma-check/artifacts/bench/
# accepted-018da2fc-rdma-native/ and accepted-018da2fc-rdma-oci/.
#
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
exec pixi run -- python tools/benchmark/rpc_regression.py --transport rdma "$@"
