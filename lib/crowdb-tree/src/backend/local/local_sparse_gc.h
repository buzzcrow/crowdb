// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "local_block_gc.h"

namespace crowdb::tree
{
struct LocalSparseBlock
{
    Status          status;
    ProtectedExtent exclusion;
    uint64_t        next_block = 0;
};

// Backend lane only: inspect at most eight existing blocks and fence the first
// whose free fraction exceeds the selected threshold. The exclusion does not
// replace readable leases and must survive both anchor rotations and deletion.
LocalSparseBlock select_sparse_local_block(BlockPageStore &store, ExtentLedger &ledger, uint64_t first_block,
                                           const std::shared_ptr<detail::AllocationReservation> &metadata,
                                           double                                                free_threshold = 0.5,
                                           const std::shared_ptr<AllocationBudget>              &backend        = {});
} // namespace crowdb::tree
