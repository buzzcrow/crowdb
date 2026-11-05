// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/tree_core_access.h"
#include "c_api_maintenance_internal.h"

namespace crowdb::tree::c_api_detail
{
template <class Operation, class Prepare>
int32_t begin_report(ct_tree *tree, ct_future **out, ct_future_impl::Kind kind, Prepare prepare,
                     bool retain_ticket = true)
try {
    if (tree == nullptr || out == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out       = nullptr;
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    auto metadata = core->binding()->lifecycle_budget();
    auto result   = make_ct_future(metadata);
    result->kind  = kind;
    std::unique_ptr<ct_future_handle, decltype(&free_ct_future_handle)> handle(make_ct_future_handle(result),
                                                                               free_ct_future_handle);
    std::shared_ptr<TreeOperationLifetime::Ticket>                      ticket;
    Status                                                              status;
    if (retain_ticket) {
        status = core->operation_lifetime().acquire(metadata, &ticket);
        if (!status.ok()) {
            return static_cast<int32_t>(status.code());
        }
    }
    std::shared_ptr<Operation> operation;
    status = prepare(*core, &operation);
    if (!status.ok()) {
        return static_cast<int32_t>(status.code());
    }
    using Call = c_api_detail::MaintenanceCall<Operation>;
    auto call  = std::allocate_shared<Call>(MetadataAllocator<Call>(metadata), std::move(ticket), std::move(operation),
                                            result, c_api_completion_signal(tree));
    call->start();
    *out = reinterpret_cast<ct_future *>(handle.release());
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}
} // namespace crowdb::tree::c_api_detail
