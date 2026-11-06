// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/retirement_queue.h"
#include "crowdb-tree/epoch.h"

namespace crowdb::tree
{
struct EpochManager::Lifetime : std::enable_shared_from_this<Lifetime>
{
    explicit Lifetime(std::shared_ptr<AllocationBudget>        budget     = {},
                      std::shared_ptr<detail::RetirementQueue> retirement = {});
    ~Lifetime();
    void request_collect() noexcept;
    void defer(Deferred *entry) noexcept;
    bool retire_step() noexcept;

    std::shared_ptr<detail::RetirementQueue> queue;
    ParticipantList                          live_participants;
    std::atomic<Deferred *>                  live_pending{nullptr};
    std::atomic<uint64_t>                    live_epoch{1};
    std::atomic<size_t>                      pending_count{0};
    std::atomic<size_t>                      freed_count{0};

    // Legacy detached guards use the same bounded final destruction path.
    Participant         *participants = nullptr;
    Deferred            *pending      = nullptr;
    Deferred            *collected    = nullptr;
    std::vector<Retired> retired;

  private:
    struct Collector : detail::RetirementNode
    {
        Lifetime *owner = nullptr;
    } collector;

    static bool               collect(detail::RetirementNode *node) noexcept;
    bool                      collect_step() noexcept;
    void                      return_blocked() noexcept;
    void                      finish_cycle() noexcept;
    std::shared_ptr<Lifetime> collector_owner;
    std::atomic<bool>         collecting{false};
    std::atomic<bool>         requested{false};
    enum class Phase : uint8_t { kStart, kParticipants, kDeferred };
    Phase        phase        = Phase::kStart;
    Participant *scan         = nullptr;
    Deferred    *batch        = nullptr;
    Deferred    *blocked      = nullptr;
    Deferred    *blocked_tail = nullptr;
    uint64_t     oldest       = 0;
};
} // namespace crowdb::tree
