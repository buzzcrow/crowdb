// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "block_account.h"
#include "scheduled_page_store.h"

namespace crowdb::tree::detail
{
namespace
{
PageStore *block_source(AsyncPageStore *backend) noexcept
{
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(backend);
    return scheduled != nullptr ? scheduled->synchronous_store() : nullptr;
}
} // namespace

std::optional<size_t> backend_operation_bound(AsyncPageStore *backend, BackendOperationLimits limits) noexcept
{
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(backend);
    if (scheduled != nullptr) {
        limits.asynchronous = scheduled->uses_async_transport();
        limits.workers      = std::max(limits.workers, scheduled->allocation_workers());
    }
    return backend_operation_bound(block_source(backend), limits);
}

Status reserve_backend_operation(AsyncPageStore *backend, const std::shared_ptr<ResourceBudget::Account> &account,
                                 BackendOperationLimits limits, std::shared_ptr<AllocationBudget> *out)
{
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(backend);
    if (scheduled != nullptr) {
        limits.asynchronous = scheduled->uses_async_transport();
        limits.workers      = std::max(limits.workers, scheduled->allocation_workers());
    }
    return reserve_backend_operation(block_source(backend), account, limits, out);
}

Status bind_backend_operation(AsyncPageStore *backend, BackendOperationLimits limits, ResourceBudget::Credit credit,
                              std::shared_ptr<AllocationBudget> *out)
{
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(backend);
    if (scheduled != nullptr) {
        limits.asynchronous = scheduled->uses_async_transport();
        limits.workers      = std::max(limits.workers, scheduled->allocation_workers());
    }
    return bind_backend_operation(block_source(backend), limits, std::move(credit), out);
}
} // namespace crowdb::tree::detail
