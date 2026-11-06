// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstddef>
#include <cstdint>

extern "C" {

using ct_runtime = struct ct_runtime;
using ct_tree    = struct ct_tree;

struct ct_resource_usage_v1
{
    uint64_t limit, used, reserved, allocated;
    uint64_t denied_tree, denied_host, waits, wait_ns, waiters;
};

// Non-draining relaxed samples. Set bits identify supported resource indices:
// handoff, routing, reclaim, writer, gap_repair, io, cpu_queue_slots,
// backend_queue_slots, cpu_workers, backend_workers, cache_bytes, cache_pages,
// cache_request_slots, backend_operation_slots, apply_operation_slots,
// replay_operation_slots, source_entry_slots, source_key_bytes,
// gap_source_entry_slots, gap_source_key_bytes.
// Hosts expose 0..9; Trees expose 0..7 and 10..19. Source rows describe the
// active source and include admitted writers; their contention counters are
// not measured. Operation rows include accepted owners through callback tails.
// Partitions 0..4, cache_bytes and source key capacities use bytes; the others
// use counts. Queue and
// cache request slots include prepaid registrations even while idle.
struct ct_resource_usage_snapshot_v1
{
    size_t               struct_size;
    uint32_t             version;
    uint32_t             resource_mask;
    uint64_t             scope_id;
    ct_resource_usage_v1 resources[20];
};

int32_t ct_runtime_resource_usage(const ct_runtime *runtime, ct_resource_usage_snapshot_v1 *out);
int32_t ct_tree_resource_usage(ct_tree *tree, ct_resource_usage_snapshot_v1 *out);

struct ct_resource_limits
{
    size_t handoff_bytes;
    size_t routing_bytes;
    size_t reclaim_bytes;
    size_t writer_bytes;
    size_t gap_repair_bytes;
    size_t io_operations;
};

// Version 1 requires struct_size == sizeof(ct_runtime_options). A null options
// pointer selects defaults; supplied numeric values are used without defaults.
struct ct_runtime_options
{
    size_t             struct_size;
    size_t             workers;
    size_t             cpu_queue_limit;
    size_t             max_waiters_per_class;
    size_t             lifecycle_bytes;
    ct_resource_limits limits;
    size_t             backend_workers;
    size_t             backend_queue_limit;
};

struct ct_tree_runtime_options
{
    size_t             struct_size;
    size_t             cpu_queue_limit;
    size_t             lifecycle_bytes;
    ct_resource_limits limits;
    size_t             backend_queue_limit;
};

struct ct_executor_snapshot_v1
{
    uint64_t queued;
    uint64_t inflight;
    uint64_t submitted;
    uint64_t completed;
    uint64_t rejected;
    uint64_t queue_ns;
    uint64_t execute_ns;
};

struct ct_runtime_snapshot_v1
{
    size_t                  struct_size;
    uint32_t                version;
    uint32_t                stopped;
    ct_resource_limits      reserved;
    ct_resource_limits      allocated;
    uint64_t                bindings;
    ct_executor_snapshot_v1 cpu;
    ct_executor_snapshot_v1 backend;
    uint64_t                budget_waiters;
    uint64_t                budget_grants;
    uint64_t                budget_cancelled;
    uint64_t                budget_wait_ns;
};

// Requires exact struct_size and version 1. Does not drain counters, allocate,
// submit work or perform backend I/O; invalid inputs leave output unchanged.
int32_t ct_runtime_snapshot(const ct_runtime *runtime, ct_runtime_snapshot_v1 *out);

void    ct_runtime_options_default(ct_runtime_options *out);
void    ct_tree_runtime_options_default(ct_tree_runtime_options *out);
int32_t ct_runtime_create(const ct_runtime_options *options, ct_runtime **out);
void    ct_runtime_stop_admission(ct_runtime *runtime);
// Host blocking boundary after all Tree operations and backend tickets end.
// This closes CPU submission and waits for CPU jobs only, not external I/O.
void ct_runtime_drain_cpu(ct_runtime *runtime);
// Release the caller's reference. Live Trees/operations retain their owners;
// this call does not wait for them or stop their admission.
void ct_runtime_free(ct_runtime *runtime);
}
