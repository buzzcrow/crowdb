// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/status.h"

#include <atomic>
#include <memory>

namespace crowdb::tree
{
// A host-owned backend service has no executor or Tree reference. The host
// retains and pumps it until stop_admission() followed by drained(). Stopping
// ingress does not invalidate previously registered readers or release nodes.
class BackendReleaseService : public std::enable_shared_from_this<BackendReleaseService>
{
  public:
    using Wake = void (*)(void *) noexcept;

    struct Node
    {
        // Each false result yields the same paid node for a later bounded step.
        // A true result must have destroyed/freed the node and its resource.
        bool (*dispose)(Node *) noexcept = nullptr;
        std::shared_ptr<BackendReleaseService> service;
        Node                                  *next   = nullptr;
        bool                                   queued = false;
        Node()                                        = default;
        Node(const Node &)                            = delete;
        Node &operator=(const Node &)                 = delete;
    };

    struct Snapshot
    {
        size_t registered = 0;
        size_t pending    = 0;
        bool   closed     = false;
    };

    struct Step
    {
        size_t visited   = 0;
        size_t completed = 0;
        bool   more      = false;
    };

    [[nodiscard]] static Status create(const std::shared_ptr<AllocationBudget> &budget, size_t capacity,
                                       std::shared_ptr<const void> wake_owner, Wake wake, void *context,
                                       std::shared_ptr<BackendReleaseService> *out);
    ~BackendReleaseService();
    // Bind once before admitting backend users; a host driver owns the wake
    // context and continues pumping after Tree/runtime shutdown.
    [[nodiscard]] Status bind_wake(std::shared_ptr<const void> owner, Wake wake, void *context);

    [[nodiscard]] bool externally_driven() const noexcept
    {
        return wake_ != nullptr;
    }

    [[nodiscard]] const std::shared_ptr<AllocationBudget> &allocation_budget() const noexcept
    {
        return budget_;
    }

    [[nodiscard]] static size_t allocation_bound() noexcept;
    // Registration precedes resource publication and never allocates. The node
    // is embedded in an independently paid owner that survives final release.
    [[nodiscard]] Status register_node(Node &node);
    static void          enqueue(Node &node) noexcept;
    // Only the host backend consumer may pump. Concurrent pumps do not wait.
    [[nodiscard]] Step     pump(size_t limit) noexcept;
    void                   stop_admission() noexcept;
    [[nodiscard]] Snapshot snapshot() const noexcept;
    [[nodiscard]] bool     drained() const noexcept;

  private:
    void                              wake() const noexcept;
    static constexpr size_t           kClosed = size_t{1} << ((sizeof(size_t) * 8) - 1);
    std::shared_ptr<AllocationBudget> budget_;
    std::shared_ptr<const void>       wake_owner_;
    Wake                              wake_     = nullptr;
    void                             *context_  = nullptr;
    size_t                            capacity_ = 0;
    std::atomic<size_t>               state_{0};
    std::atomic<size_t>               pending_{0};
    std::atomic<Node *>               incoming_{nullptr};
    Node                             *active_          = nullptr;
    Node                             *deferred_        = nullptr;
    bool                              prefer_deferred_ = false;
    std::atomic_flag                  consumer_        = ATOMIC_FLAG_INIT;
};
} // namespace crowdb::tree
