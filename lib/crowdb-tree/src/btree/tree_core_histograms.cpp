// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_internal.h"

namespace crowdb::tree
{
Status TreeCore::enable_work_histogram(uint32_t stage)
try {
    if (stage >= kTreeStageCount) {
        return Status::invalid_argument();
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable();
    }
    const auto  selected = static_cast<TreeStage>(stage);
    const auto &metrics  = state_->work;
    if (metrics->histogram_enabled(selected)) {
        return Status::Ok();
    }
    const auto bound = detail::AllocationBound()
                           .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                           .add(TreeWorkMetrics::histogram_allocation_bytes())
                           .value();
    if (!bound) {
        return Status::resource_exhausted();
    }
    auto reserved = detail::ResourceBudget::try_reserve(binding()->account(), detail::BudgetClass::kRouting, *bound);
    if (!reserved) {
        return metrics->histogram_enabled(selected) ? Status::Ok() : Status::resource_exhausted();
    }
    auto       credit = detail::AllocationReservation::create(std::move(*reserved));
    const auto status = metrics->enable_histogram(selected, credit);
    credit->seal();
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
} // namespace crowdb::tree
