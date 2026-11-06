// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "flush_runtime.h"

namespace crowdb::tree
{
// One prepaid CPU registration per Tree serves all accepted applies. A ready
// operation carries its queue node, so a wakeup neither allocates nor consumes
// another ordinary runtime queue slot while waiting for handoff progress.
class ApplyDispatcher : public std::enable_shared_from_this<ApplyDispatcher>
{
  public:
    using Task = detail::FlushRuntime::Task;

    class Node
    {
      public:
        Node(Task task, void *context) : task_(task), context_(context)
        {
        }

        ~Node();
        Node(const Node &)            = delete;
        Node &operator=(const Node &) = delete;

      private:
        friend class ApplyDispatcher;
        std::atomic<Node *>         next_{nullptr};
        std::atomic<bool>           queued_{false};
        std::shared_ptr<const void> owner_;
        Task                        task_;
        void                       *context_;
    };

    static Status               create(std::shared_ptr<detail::FlushRuntime>       runtime,
                                       std::shared_ptr<detail::FlushRuntime::Lane> lane, size_t steps_per_turn,
                                       const std::shared_ptr<detail::AllocationReservation> &metadata,
                                       std::shared_ptr<ApplyDispatcher>                     *out);
    [[nodiscard]] static size_t allocation_bound() noexcept;

    void request(Node &node, std::shared_ptr<const void> owner) noexcept;
    // Host-only, after accepted apply owners and their wakeup producers join.
    void close_and_drain() noexcept;

  private:
    ApplyDispatcher(std::shared_ptr<detail::FlushRuntime> runtime, size_t steps_per_turn);
    static void         run(void *context) noexcept;
    void                enqueue(Node *node) noexcept;
    [[nodiscard]] Node *pop() noexcept;

    std::shared_ptr<detail::FlushRuntime>           runtime_;
    std::shared_ptr<detail::FlushRuntime::Progress> progress_;
    const size_t                                    steps_per_turn_;
    Node                                            stub_{nullptr, nullptr};
    std::atomic<Node *>                             tail_{&stub_};
    Node                                           *head_ = &stub_;
};
} // namespace crowdb::tree
