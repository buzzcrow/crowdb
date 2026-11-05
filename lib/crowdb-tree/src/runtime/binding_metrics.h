// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/memory_budget.h"

namespace crowdb::tree::detail
{
// Account holds a weak metrics association. Keep its shared control on the
// host ledger, otherwise that weak control's allocator retains the same
// account and its lifecycle reservation forever. The object itself remains
// charged to the Tree and is freed before returning that charge.
template <class T>
std::shared_ptr<T> binding_metrics(const std::shared_ptr<AllocationBudget> &tree,
                                   const std::shared_ptr<AllocationBudget> &host)
{
    struct Storage
    {
        explicit Storage(std::shared_ptr<AllocationBudget> budget) : budget(std::move(budget))
        {
        }

        std::shared_ptr<AllocationBudget> budget;
        T                                 value;
    };

    MetadataAllocator<Storage> allocator(tree);
    auto                      *storage = allocator.allocate(1);
    try {
        std::construct_at(storage, tree);
    }
    catch (...) {
        allocator.deallocate(storage, 1);
        throw;
    }
    return {&storage->value,
            [storage](T *) noexcept {
                MetadataAllocator<Storage> allocator(storage->budget);
                std::destroy_at(storage);
                allocator.deallocate(storage, 1);
            },
            MetadataAllocator<T>(host)};
}
} // namespace crowdb::tree::detail
