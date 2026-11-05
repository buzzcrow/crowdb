// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/owned_allocation_c_api.h"
#include "external_owner_bridge.h"

struct ct_owned_allocation
{
    ct_owned_allocation(std::shared_ptr<crowdb::tree::AllocationBudget>          credit,
                        std::shared_ptr<const crowdb::tree::ExternalOwnerBridge> value)
        : budget(std::move(credit)),
          bridge(std::move(value))
    {
    }

    std::atomic<size_t>                                      references{1};
    std::shared_ptr<crowdb::tree::AllocationBudget>          budget;
    std::shared_ptr<const crowdb::tree::ExternalOwnerBridge> bridge;
};
