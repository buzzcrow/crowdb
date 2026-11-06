// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_backend_state.h"
#include "backend/chunk/chunk_collection_operation.h"
#include "btree/tree_core_internal.h"

namespace crowdb::tree
{
Status TreeCore::prepare_chunk_collection(std::shared_ptr<ChunkCollectionOperation> *out)
try {
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable();
    }
    auto backend = std::dynamic_pointer_cast<ChunkBackendState>(chunk_backend());
    if (!backend) {
        return Status::not_supported();
    }
    using namespace detail;
    const auto bound = AllocationBound(ChunkCollectionOperation::allocation_bound())
                           .add(2 * shared_allocation_bound<AllocationReservation>())
                           .add(TreeOperationLifetime::ticket_allocation_bound())
                           .value();
    if (!bound) {
        return Status::resource_exhausted();
    }
    auto bytes = ResourceBudget::try_reserve(binding()->account(), BudgetClass::kReclaim, *bound);
    auto io    = ResourceBudget::try_reserve(binding()->account(), BudgetClass::kIo, 1);
    if (!bytes || !io) {
        return Status::resource_exhausted();
    }
    PageLoadBudget budget;
    budget.bytes = AllocationReservation::create(std::move(*bytes));
    budget.io    = reserved_shared<AllocationReservation>(budget.reservation(), std::move(*io));

    struct Credit
    {
        PageLoadBudget &budget;
        bool            transferred = false;

        ~Credit()
        {
            if (!transferred) {
                budget.reservation()->seal();
                budget.io->seal();
            }
        }
    } credit{.budget = budget};

    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    auto                                           status = operation_lifetime().acquire(budget.reservation(), &ticket);
    if (!status.ok()) {
        return status;
    }
    status = ChunkCollectionOperation::create(backend, ticket->backend(backend->store.get()), binding()->cpu(),
                                              binding()->lane(), budget, out);
    credit.transferred = status.ok();
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
} // namespace crowdb::tree
