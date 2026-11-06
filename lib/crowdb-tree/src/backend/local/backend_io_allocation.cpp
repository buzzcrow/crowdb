// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend_io_allocation.h"
#ifdef CROWDB_HAVE_LIBURING
namespace crowdb::tree::detail
{
::crowdb::common::IOAllocation io_allocation(const std::shared_ptr<AllocationBudget> &budget)
{
    return {.owner    = budget,
            .context  = budget.get(),
            .allocate = [](void *context, size_t bytes, size_t alignment) -> void * {
                auto &budget = *static_cast<AllocationBudget *>(context);
                if (!budget.claim(bytes)) {
                    throw std::bad_alloc();
                }
                try {
                    auto *storage = ::operator new(bytes, std::align_val_t{alignment});
                    budget.record_allocation(AllocationKind::kMetadata, bytes, true);
                    return storage;
                }
                catch (...) {
                    budget.release(bytes);
                    throw;
                }
            },
            .deallocate =
                [](void *context, void *storage, size_t bytes, size_t alignment) noexcept {
                    auto &budget = *static_cast<AllocationBudget *>(context);
                    ::operator delete(storage, std::align_val_t{alignment});
                    budget.record_allocation(AllocationKind::kMetadata, bytes, false);
                    budget.release(bytes);
                }};
}
} // namespace crowdb::tree::detail
#endif
