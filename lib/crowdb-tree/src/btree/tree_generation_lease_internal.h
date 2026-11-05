// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "tree_core_internal.h"
#include "tree_generation_lease.h"

namespace crowdb::tree
{
struct TreeGenerationPreparation
{
    TreeCore                                            *owner = nullptr;
    std::shared_ptr<detail::AllocationReservation>       metadata;
    std::shared_ptr<detail::AllocationReservation>       source_lifecycle;
    std::shared_ptr<detail::AllocationReservation>       flush_lifecycle;
    detail::ReservedUnique<SourceAdmission>              sources;
    detail::ReservedUnique<FlushController>              flush;
    std::shared_ptr<PublicationCoordinator::Replacement> publication;
    std::shared_ptr<const PublishedTreeState>            expected;
    std::shared_ptr<PublishedSources>                    inherited;
    uint64_t                                             previous_generation = 0;
    uint64_t                                             coverage            = 0;
    uint64_t                                             frontier            = 0;
    std::shared_ptr<SplitSourceSession>                  previous_split_sources;
    std::shared_ptr<SplitHandoff>                        previous_split_handoff;
    bool                                                 held      = false;
    bool                                                 committed = false;
};
} // namespace crowdb::tree
