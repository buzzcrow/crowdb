// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/local/block_account.h"
#include "btree/tree_core.h"

namespace crowdb::tree::core_read_detail
{
struct ReadPreparation
{
    ReadPreparation()                                       = default;
    ReadPreparation(ReadPreparation &&) noexcept            = default;
    ReadPreparation &operator=(ReadPreparation &&) noexcept = default;

    std::shared_ptr<AllocationBudget>              credit;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<const PublishedTreeState>      view;
    std::shared_ptr<PageLoader>                    loader;
    PublishedReadLimits                            limits;
};

// All admission and source capture happens before the first CPU submission.
Status prepare(TreeCore &core, ReadPreparation *out);
Status prepare(TreeCore &core, std::shared_ptr<const PublishedTreeState> view, PublishedReadLimits limits,
               ReadPreparation *out);
} // namespace crowdb::tree::core_read_detail
