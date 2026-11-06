// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_owned_allocation.h"

#include "btree/allocation_account.h"
#include "btree/tree_core_access.h"
#include "c_api_internal.h"

namespace
{
using namespace crowdb::tree;

// The foreign control charge follows the payload owner, not the independently
// releasable C handle. Its destructor frees the foreign object before
// refunding.
struct ForeignOwner
{
    ForeignOwner(std::shared_ptr<AllocationBudget> credit, size_t bytes, ExternalAllocation::Owner value)
        : budget(std::move(credit)),
          control(budget, bytes, AllocationKind::kMetadata),
          context(std::move(value))
    {
    }

    std::shared_ptr<AllocationBudget> budget;
    AllocationLease                   control;
    ExternalAllocation::Owner         context;
};

void free_foreign(void *context)
{
    auto                           *owner = static_cast<ForeignOwner *>(context);
    MetadataAllocator<ForeignOwner> allocator(owner->budget);
    std::destroy_at(owner);
    allocator.deallocate(owner, 1);
}

ExternalAllocation::Owner retain_foreign(const std::shared_ptr<AllocationBudget> &budget, size_t bytes,
                                         ExternalAllocation::Owner context)
{
    MetadataAllocator<ForeignOwner> allocator(budget);
    auto                           *owner = allocator.allocate(1);
    try {
        std::construct_at(owner, budget, bytes, std::move(context));
    }
    catch (...) {
        allocator.deallocate(owner, 1);
        throw;
    }
    ExternalAllocation::Owner result(owner, free_foreign);
    return result;
}
} // namespace

extern "C" ct_status ct_owned_allocation_create(ct_tree *tree, const uint8_t *data, size_t length, size_t capacity,
                                                void (*drop_context)(void *), void *context, size_t control_bytes,
                                                ct_owned_allocation **out)
try {
    // A missing deleter is invalid and cannot consume an arbitrary pointer.
    if (drop_context == nullptr) {
        return static_cast<int>(Code::kInvalidArgument);
    }
    ExternalAllocation::Owner foreign(context, drop_context);
    if (out == nullptr) {
        return static_cast<int>(Code::kInvalidArgument);
    }
    *out = nullptr;
    if (context == nullptr || length > capacity || (capacity != 0 && data == nullptr)) {
        return static_cast<int>(Code::kInvalidArgument);
    }
    *out         = nullptr;
    auto *facade = c_api_tree(tree);
    auto *core   = facade == nullptr ? nullptr : TreeCoreAccess::get(*facade);
    if (core == nullptr || core->closed()) {
        return static_cast<int>(Code::kUnavailable);
    }
    std::shared_ptr<detail::AllocationAccount> budget;
    auto                                       status =
        detail::AllocationAccount::create(core->binding()->account(), detail::BudgetClass::kAdmittedWriter, &budget);
    if (!status.ok()) {
        return static_cast<int>(status.code());
    }
    auto                                       retained = retain_foreign(budget, control_bytes, std::move(foreign));
    std::shared_ptr<const ExternalOwnerBridge> bridge;
    status = ExternalOwnerBridge::create(data, length, capacity, std::move(retained), budget, &bridge);
    if (!status.ok()) {
        return static_cast<int>(status.code());
    }
    MetadataAllocator<ct_owned_allocation> allocator(budget);
    auto                                  *result = allocator.allocate(1);
    std::construct_at(result, std::move(budget), std::move(bridge));
    *out = result;
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int>(crowdb::tree::Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int>(crowdb::tree::Code::kInternal);
}

extern "C" void ct_owned_allocation_retain(ct_owned_allocation *owner)
{
    if (owner != nullptr && owner->references.fetch_add(1, std::memory_order_relaxed) == SIZE_MAX) {
        std::terminate();
    }
}

extern "C" void ct_owned_allocation_free(ct_owned_allocation *owner)
{
    if (owner == nullptr || owner->references.fetch_sub(1, std::memory_order_acq_rel) != 1) {
        return;
    }
    crowdb::tree::MetadataAllocator<ct_owned_allocation> allocator(owner->budget);
    std::destroy_at(owner);
    allocator.deallocate(owner, 1);
}
