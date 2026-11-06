// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "admitted_batch.h"
#include "allocation_bound.h"
#include "tree_apply_operation.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
Status TreeCore::prepare_apply(size_t count, const CoreWriteReader &read, detail::BudgetClass category,
                               size_t retained_metadata_bytes, std::shared_ptr<TreeApplyOperation> *out)
try {
    if (out == nullptr || *out ||
        (category != detail::BudgetClass::kAdmittedWriter && category != detail::BudgetClass::kGapRepair)) {
        return Status::invalid_argument("apply admission requires an empty output and apply partition");
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("apply admission is closed");
    }
    const auto          sources = state_->sources->catalog();
    const auto         &limits  = sources->active_budget->options();
    AdmittedBatch::Plan plan;
    auto status = AdmittedBatch::plan(config(), limits, state_->loads.max_value_bytes, count, read, &plan);
    if (!status.ok()) {
        return status;
    }
    const auto control  = TreeApplyOperation::allocation_bound(retained_metadata_bytes);
    const auto complete = detail::AllocationBound(plan.staging_bytes).add(plan.source_bytes).add(control).value();
    if (!complete || plan.footprint.entries > limits.max_gap_entries ||
        plan.footprint.key_bytes > limits.max_gap_key_bytes ||
        *complete > binding()->options().limits.gap_repair_bytes) {
        return Status::invalid_argument("complete proposal cannot fit the reserved gap progress partition");
    }
    if (category == detail::BudgetClass::kAdmittedWriter && *complete > binding()->options().limits.writer_bytes) {
        return Status::invalid_argument("complete proposal cannot fit the ordinary writer partition");
    }
    auto grant = detail::ResourceBudget::try_reserve(binding()->account(), category, *complete);
    if (!grant) {
        (void)state_->source_rotation->request_pressure();
        return Status::unavailable("complete apply grant is currently occupied");
    }
    auto staging = grant->split(plan.staging_bytes);
    auto source  = plan.source_bytes != 0 ? grant->split(plan.source_bytes) : std::nullopt;
    if (!staging || (plan.source_bytes != 0 && !source)) {
        return Status::internal_error("complete apply grant could not be partitioned");
    }
    std::shared_ptr<AdmittedBatch> batch;
    status = AdmittedBatch::bind_prepaid(limits, count, read, category, plan, std::move(*staging),
                                         source ? std::move(*source) : detail::ResourceBudget::Credit{}, &batch);
    if (!status.ok()) {
        return status;
    }
    return TreeApplyOperation::bind_prepaid(shared_from_this(), std::move(batch), std::move(*grant), out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("apply admission allocation failed");
}
} // namespace crowdb::tree
