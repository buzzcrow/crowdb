// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/resource_usage.h"
#include "crowdb-tree/status.h"

#include <cstddef>
#include <cstdint>
#include <memory>

namespace crowdb::tree
{
struct TreeResourceLimits
{
    size_t handoff_bytes    = 256ULL * 1024 * 1024;
    size_t routing_bytes    = 512ULL * 1024 * 1024;
    size_t reclaim_bytes    = 128ULL * 1024 * 1024;
    size_t writer_bytes     = 512ULL * 1024 * 1024;
    size_t gap_repair_bytes = 64ULL * 1024 * 1024;
    size_t io_operations    = 128;
};

struct TreeRuntimeOptions
{
    size_t workers               = 4;
    size_t cpu_queue_limit       = 256;
    size_t max_waiters_per_class = 256;
    size_t backend_workers       = 2;
    size_t backend_queue_limit   = 256;
    // Retained routing reserve for the CPU queue and host-owned descriptors.
    size_t             lifecycle_bytes = 4ULL * 1024 * 1024;
    TreeResourceLimits limits{};
};

struct TreeBindingOptions
{
    size_t cpu_queue_limit     = 64;
    size_t backend_queue_limit = 64;
    // Retained per-tree routing reserve for its lane and core metadata.
    size_t             lifecycle_bytes = 1024ULL * 1024;
    TreeResourceLimits limits{.handoff_bytes    = 64ULL * 1024 * 1024,
                              .routing_bytes    = 256ULL * 1024 * 1024,
                              .reclaim_bytes    = 64ULL * 1024 * 1024,
                              .writer_bytes     = 128ULL * 1024 * 1024,
                              .gap_repair_bytes = 16ULL * 1024 * 1024,
                              .io_operations    = 16};
};

struct TreeExecutorSnapshot
{
    uint64_t queued     = 0;
    uint64_t inflight   = 0;
    uint64_t submitted  = 0;
    uint64_t completed  = 0;
    uint64_t rejected   = 0;
    uint64_t queue_ns   = 0;
    uint64_t execute_ns = 0;
};

// Cumulative relaxed-atomic samples. CPU and blocking-backend durations are
// separate; concurrent worker durations are not a host wall-clock interval.
struct TreeRuntimeSnapshot
{
    TreeResourceLimits   reserved{.handoff_bytes    = 0,
                                  .routing_bytes    = 0,
                                  .reclaim_bytes    = 0,
                                  .writer_bytes     = 0,
                                  .gap_repair_bytes = 0,
                                  .io_operations    = 0};
    TreeResourceLimits   allocated{.handoff_bytes    = 0,
                                   .routing_bytes    = 0,
                                   .reclaim_bytes    = 0,
                                   .writer_bytes     = 0,
                                   .gap_repair_bytes = 0,
                                   .io_operations    = 0};
    uint64_t             bindings = 0;
    uint64_t             queued   = 0;
    uint64_t             inflight = 0;
    bool                 stopped  = false;
    TreeExecutorSnapshot cpu{};
    TreeExecutorSnapshot backend{};
    uint64_t             budget_waiters   = 0;
    uint64_t             budget_grants    = 0;
    uint64_t             budget_cancelled = 0;
    uint64_t             budget_wait_ns   = 0;
};

class TreeRuntimeBinding;
struct TreeRuntimeAccess;

// A host shares CPU and blocking-backend pools and a resource ledger across its
// Trees. Tree operations retain a binding until their CPU work and backend
// callbacks end. Page caches remain owned by their individual publication
// coordinators.
class TreeRuntime
{
    struct State;

  public:
    static Status create(TreeRuntimeOptions options, std::shared_ptr<TreeRuntime> *out);
    ~TreeRuntime();
    TreeRuntime(const TreeRuntime &)            = delete;
    TreeRuntime &operator=(const TreeRuntime &) = delete;

    // Reject new bindings/reservations and stop budget waiters. Existing
    // operations retain their allocation credit and can still use the CPU.
    void stop_admission() noexcept;
    // Host-only blocking boundary. Call after Tree operations and backend
    // tickets have drained. This closes CPU submission and waits for CPU jobs;
    // it does not wait for or cancel external I/O on the caller's behalf.
    void drain_cpu() noexcept;

    [[nodiscard]] bool                      stopped() const noexcept;
    [[nodiscard]] TreeRuntimeSnapshot       snapshot() const noexcept;
    [[nodiscard]] ResourceUsageSnapshot     resource_usage() const noexcept;
    [[nodiscard]] const TreeRuntimeOptions &options() const noexcept;
    [[nodiscard]] Status                    validate_binding(const TreeBindingOptions &options) const noexcept;

  private:
    friend class TreeRuntimeBinding;
    friend struct TreeRuntimeAccess;

    explicit TreeRuntime(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
