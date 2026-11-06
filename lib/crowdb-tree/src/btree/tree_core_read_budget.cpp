// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_read_internal.h"

#include <algorithm>

namespace crowdb::tree::core_read_detail
{
Status prepare(TreeCore &core, ReadPreparation *out)
{
    return prepare(core, core.capture(), core.read_limits(), out);
}

Status prepare(TreeCore &core, std::shared_ptr<const PublishedTreeState> view, PublishedReadLimits limits,
               ReadPreparation *out)
{
    using namespace detail;
    ReadPreparation ready;
    ready.binding   = core.binding();
    ready.view      = std::move(view);
    ready.limits    = std::move(limits);
    const auto root = ready.view->mapping.get(ready.view->root_page_id);
    if (core.closed() || !root || root->level >= ready.limits.max_route_depth) {
        return Status::unavailable("read requires an open published root");
    }
    ready.limits.max_route_depth = root->level + 1ULL;
    ready.limits.max_memtables   = std::max<size_t>(1, ready.view->sources->tables.size());
    ready.credit                 = ready.limits.metadata;
    auto status                  = core.operation_lifetime().acquire(ready.credit, &ready.ticket);
    if (!status.ok()) {
        return status;
    }
    auto loads         = core.load_limits();
    loads.max_inflight = 1;
    auto loader = reserved_shared<AsyncStorePageLoader>(ready.credit, ready.ticket->backend(core.async_backend()),
                                                        ready.binding->cpu(), ready.binding->lane(), loads,
                                                        OverflowBackingResolver{},
                                                        PageLoadBudget{.bytes        = ready.credit,
                                                                       .account      = ready.binding->account(),
                                                                       .work_metrics = core.publisher().work_metrics(),
                                                                       .cpu_progress = core.cpu_progress(),
                                                                       .cause        = TreeWorkCause::Read});
    status      = core.cache()->bind_reserved(std::move(loader), ready.credit, &ready.loader);
    if (status.ok()) {
        *out = std::move(ready);
    }
    return status;
}
} // namespace crowdb::tree::core_read_detail
