// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/backing_metrics.h"
#include "btree/budget_memory_resource.h"
#include "extent_ledger.h"

#include <array>
#include <map>
#include <set>

namespace crowdb::tree
{
struct ExtentLedger::State : std::enable_shared_from_this<State>
{
    enum class Action : uint8_t { Recover, Activate, Reserve, Retain, ClaimFree, Exclude, Release };
    struct Record;

    struct Range
    {
        uint64_t length;
        Record  *record;
    };

    using Ranges     = std::pmr::map<uint64_t, Range>;
    using FreeRanges = std::pmr::set<std::pair<uint64_t, uint64_t>>; // length, address

    struct Command
    {
        Action   action;
        void    *context;
        Command *next = nullptr;
    };

    struct Request
    {
        Command                                        command;
        uint64_t                                       address;
        uint64_t                                       length;
        std::shared_ptr<detail::AllocationReservation> metadata;
        Complete                                       complete;
        std::shared_ptr<Request>                       queued_owner;
        std::shared_ptr<State>                         ledger;
    };

    struct Record
    {
        BackingMetricExtent                            metrics;
        Command                                        command{.action = Action::Release, .context = this};
        std::shared_ptr<State>                         ledger;
        std::shared_ptr<detail::AllocationReservation> metadata;
        std::weak_ptr<const void>                      owner;
        FreeRanges::node_type                          free_node;
        uint64_t                                       identity  = 0;
        uint64_t                                       address   = 0;
        uint64_t                                       length    = 0;
        bool                                           installed = false;
        bool                                           exclusive = false;
    };

    struct PreparedNodes
    {
        std::array<Ranges::node_type, 2>     ranges;
        std::array<FreeRanges::node_type, 2> free;
    };

    State(ExtentLedgerLimits bounds, const std::shared_ptr<detail::AllocationReservation> &metadata);
    ~State();

    bool retire_step() noexcept
    {
        size_t steps = 0;
        for (; steps < 64 && !ranges.empty(); ++steps) {
            ranges.erase(ranges.begin());
        }
        for (; steps < 64 && !free.empty(); ++steps) {
            free.erase(free.begin());
        }
        return ranges.empty() && free.empty();
    }

    Status                 submit(Action action, uint64_t address, uint64_t length,
                                  std::shared_ptr<detail::AllocationReservation> metadata, Complete complete);
    void                   enqueue(Command *command) noexcept;
    void                   drain() noexcept;
    void                   process(Request *request) noexcept;
    ExtentLedgerResult     apply(Request &request);
    ExtentLedgerResult     allocate(Request &request, Ranges::iterator gap, uint64_t address);
    ExtentLedgerResult     exclude(Request &request);
    ExtentLedgerResult     reserve_outside_exclusion(Request &request);
    PreparedNodes          prepare_nodes();
    void                   release(Record *record) noexcept;
    static ProtectedExtent describe(Record &record, std::shared_ptr<const void> owner);

    const ExtentLedgerLimits     limits;
    detail::BudgetMemoryResource resource;
    Ranges                       ranges{&resource};
    FreeRanges                   free{&resource};
    uint64_t                     append;
    uint64_t                     next_identity = 1;
    Record                      *excluded      = nullptr;
    bool                         active        = false;
    std::atomic<Command *>       incoming{nullptr};
    std::atomic<bool>            running{false};
    std::atomic<bool>            closed{false};
    std::atomic<size_t>          queued{0};
    std::atomic<uint64_t>        address_high_water;
    std::atomic<uint64_t>        protected_bytes{0};
    std::atomic<uint64_t>        protected_ranges{0};
    std::atomic<uint64_t>        reused_allocations{0};
    std::atomic<uint64_t>        appended_allocations{0};
};
} // namespace crowdb::tree
