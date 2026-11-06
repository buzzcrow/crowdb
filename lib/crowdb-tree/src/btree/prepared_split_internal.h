// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "prepared_split_overlay.h"
#include "tree_core_internal.h"
#include "tree_generation_lease.h"

namespace crowdb::tree
{
struct PreparedSplitOverlay::State
{
    enum Phase : uint8_t { Armed, Preparing, Ready, Installing, Installed, Failed };

    // C callers may release either facade while retaining this capability.
    // Core owners outlive the lease, its old controls, and source registration.
    std::shared_ptr<TreeCore>                      source;
    std::shared_ptr<TreeCore>                      destination;
    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<SourceTailDomain>              domain;
    std::shared_ptr<SourceTail>                    tail;
    std::shared_ptr<SourceTailDomain::Barrier>     subscription;
    std::shared_ptr<SplitSourceSession>            session;
    std::shared_ptr<SplitSourceSession>            previous_session;
    std::shared_ptr<TreeGenerationLease>           lease;
    std::atomic<uint8_t>                           phase{Armed};
    uint64_t                                       source_identity  = 0;
    uint64_t                                       start            = 0;
    uint64_t                                       replayed         = 0;
    uint64_t                                       installed        = 0;
    bool                                           source_gate      = false;
    bool                                           destination_gate = false;
};
} // namespace crowdb::tree
