// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/local_compaction_operation.h"
#include "btree/tree_core_internal.h"
#include "snapshot/operation_wait.h"

namespace crowdb::tree
{
Status TreeCore::prepare_compaction(const LocalCompactionOptions              &options,
                                    std::shared_ptr<LocalCompactionOperation> *out)
try {
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable();
    }
    const auto bound = LocalCompactionOperation::allocation_bound();
    if (!bound) {
        return Status::resource_exhausted();
    }
    auto reserved = detail::ResourceBudget::try_reserve(binding()->account(), detail::BudgetClass::kReclaim, *bound);
    if (!reserved) {
        return Status::resource_exhausted();
    }
    auto credit = detail::AllocationReservation::create(std::move(*reserved));

    struct Seal
    {
        std::shared_ptr<detail::AllocationReservation> credit;
        bool                                           transferred = false;

        ~Seal()
        {
            if (!transferred) {
                credit->seal();
            }
        }
    } seal{.credit = credit};

    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    auto                                           status = operation_lifetime().acquire(credit, &ticket);
    if (!status.ok()) {
        return status;
    }
    status = LocalCompactionOperation::create(*this, options, state_->apply_dispatcher, credit, std::move(ticket), out);
    seal.transferred = status.ok();
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status TreeCore::compact_sparse_blocks(MergeGcStats *out)
{
    if (out == nullptr) {
        return Status::invalid_argument();
    }
    *out = {};
    std::shared_ptr<LocalCompactionOperation> operation;
    auto status = prepare_compaction({.free_threshold       = config().merge_gc_block_free_threshold,
                                      .max_relocation_bytes = config().merge_gc_max_relocation_bytes},
                                     &operation);
    if (!status.ok()) {
        return status;
    }
    status = detail::wait_snapshot_operation(operation);
    *out   = operation->result();
    return status;
}
} // namespace crowdb::tree
