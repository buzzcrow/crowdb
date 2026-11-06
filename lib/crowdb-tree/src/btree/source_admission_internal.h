// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "source_admission.h"

namespace crowdb::tree
{
struct SourceCapture::State : std::enable_shared_from_this<State>
{
    struct Node
    {
        State                 *owner = nullptr;
        MemTable::FrozenWaiter waiter{&Node::complete, this};
        static void            complete(void *context) noexcept;
    };

    State(std::shared_ptr<const SourceCatalog> previous, std::shared_ptr<const SourceCatalog> next,
          std::shared_ptr<detail::AllocationReservation> credit)
        : expected(std::move(previous)),
          successor_catalog(std::move(next)),
          admission(std::move(credit)),
          nodes(expected->tables.size(), detail::ReservationAllocator<Node>(admission))
    {
        for (auto &node : nodes) {
            node.owner = this;
        }
    }

    void start(detail::CallbackComplete callback, void *argument) noexcept;
    void one_drained() noexcept;
    void finish() noexcept;

    static constexpr uint8_t                              kRunning = 1;
    static constexpr uint8_t                              kStopped = 2;
    std::shared_ptr<const SourceCatalog>                  expected;
    std::shared_ptr<const SourceCatalog>                  successor_catalog;
    std::shared_ptr<SourceCatalog>                        covered_catalog;
    std::shared_ptr<detail::AllocationReservation>        admission;
    std::vector<Node, detail::ReservationAllocator<Node>> nodes;
    uint64_t                                              batch          = 0;
    uint64_t                                              frontier       = 0;
    uint64_t                                              owner_identity = 0;
    std::atomic<SourceCapturePhase>                       phase{SourceCapturePhase::kCaptured};
    std::atomic<bool>                                     activated{false};
    std::atomic<bool>                                     ready{false};
    std::atomic<uint8_t>                                  lifecycle{0};
    std::atomic<size_t>                                   remaining{0};
    bool                                                  registered = false;
    detail::CallbackComplete                              complete   = nullptr;
    void                                                 *context    = nullptr;
    std::shared_ptr<State>                                draining_owner;
};
} // namespace crowdb::tree
