// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_owned_budget.h"

#include "btree/allocation_reservation.h"
#include "c_api_future_internal.h"

using namespace crowdb::tree;

struct ct_owned_budget
{
    std::shared_ptr<AllocationBudget> budget;
};

std::shared_ptr<AllocationBudget> c_api_result_controls(const std::shared_ptr<detail::ResourceBudget::Account> &account)
{
    auto credit = detail::ResourceBudget::try_reserve(
        account, detail::BudgetClass::kRouting,
        kReturnedOwnerControlBytes + detail::shared_allocation_bound<detail::AllocationReservation>());
    if (!credit) {
        throw std::bad_alloc();
    }
    return detail::AllocationReservation::create(std::move(*credit));
}

int32_t c_api_owned_budget(const std::shared_ptr<AllocationBudget> &budget, ct_owned_budget **out)
try {
    if (out == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out = nullptr;
    if (!budget) {
        return 0;
    }
    MetadataAllocator<ct_owned_budget> allocator(budget);
    auto                              *owner = allocator.allocate(1);
    std::construct_at(owner, ct_owned_budget{budget});
    *out = owner;
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}

int32_t ct_future_owned_budget(const ct_future *future, ct_owned_budget **out)
{
    if (future == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    const auto &result = **reinterpret_cast<const ct_future_handle *>(future);
    if (!result.done.load(std::memory_order_acquire) || result.status != 0 ||
        (result.kind != ct_future_impl::Kind::kGet && result.kind != ct_future_impl::Kind::kScan)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    return c_api_owned_budget(result.result_controls, out);
}

int32_t ct_scan_result_owned_budget(const ct_scan_result *result, ct_owned_budget **out)
{
    return ct_future_owned_budget(reinterpret_cast<const ct_future *>(result), out);
}

int32_t ct_owned_budget_claim(ct_owned_budget *budget, size_t bytes)
{
    return budget == nullptr || budget->budget->claim(bytes) ? 0 : static_cast<int32_t>(Code::kResourceExhausted);
}

void ct_owned_budget_record(ct_owned_budget *budget, size_t bytes, int32_t acquired)
{
    if (budget != nullptr) {
        budget->budget->record_allocation(AllocationKind::kMetadata, bytes, acquired != 0);
    }
}

void ct_owned_budget_release(ct_owned_budget *budget, size_t bytes)
{
    if (budget != nullptr) {
        budget->budget->release(bytes);
    }
}

void ct_owned_budget_free(ct_owned_budget *budget)
{
    if (budget != nullptr) {
        MetadataAllocator<ct_owned_budget> allocator(budget->budget);
        std::destroy_at(budget);
        allocator.deallocate(budget, 1);
    }
}
