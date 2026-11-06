// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/resource_budget.h"
#include "crowdb-tree/owned_budget_c_api.h"

// Bounded headroom for downstream Bytes/RPC control storage. It is reserved
// at read admission; only actual controls are marked physically allocated.
inline constexpr size_t kReturnedOwnerControlBytes = 1024;

[[nodiscard]] std::shared_ptr<crowdb::tree::AllocationBudget>
c_api_result_controls(const std::shared_ptr<crowdb::tree::detail::ResourceBudget::Account> &account);
int32_t c_api_owned_budget(const std::shared_ptr<crowdb::tree::AllocationBudget> &budget, ct_owned_budget **out);
