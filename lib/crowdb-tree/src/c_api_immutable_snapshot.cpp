// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/runtime_range_rebuild.h"
#include "btree/tree_core_access.h"
#include "c_api_future_internal.h"
#include "c_api_immutable_snapshot_internal.h"
#include "c_api_internal.h"
#include "c_api_owned_budget.h"
#include "crowdb-common/log.h"

using namespace crowdb::tree;

ct_status ct_immutable_snapshot_capture(ct_tree *tree, uint64_t expected_sequence, uint64_t expected_coverage,
                                        ct_immutable_snapshot **out)
try {
    if (tree == nullptr || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *out         = nullptr;
    auto *facade = c_api_tree(tree);
    auto *core   = TreeCoreAccess::get(*facade);
    if (core == nullptr) {
        return static_cast<ct_status>(Code::kNotSupported);
    }
    const auto matches = [&] {
        return expected_sequence == 0 || (facade->durable_snapshot_seq() == expected_sequence &&
                                          facade->durable_snapshot_last_applied_slot() == expected_coverage);
    };
    if (!matches()) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    std::shared_ptr<PublishedSnapshotContext> context;
    auto                                      status = core->capture_snapshot(&context);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    if (!matches() || context->view()->coverage != expected_coverage || !context->detached()) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    *out = new ct_immutable_snapshot(std::move(context));
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

void ct_immutable_snapshot_free(ct_immutable_snapshot *snapshot)
{
    delete snapshot;
}

ct_status ct_immutable_snapshot_get(ct_immutable_snapshot *snapshot, const uint8_t *key, size_t size, ct_future **out)
try {
    if (snapshot == nullptr || out == nullptr || (key == nullptr && size != 0)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *out                   = nullptr;
    const auto &context    = snapshot->context;
    auto        value      = make_ct_future(context->binding()->lifecycle_budget());
    value->result_controls = c_api_result_controls(context->binding()->account());
    auto status            = context->get(Slice(key, size), &value->get_result);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    value->done.store(true, std::memory_order_release);
    *out = reinterpret_cast<ct_future *>(make_ct_future_handle(std::move(value)));
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

ct_status ct_immutable_snapshot_rebuild(ct_immutable_snapshot *snapshot, ct_tree *destination,
                                        ct_range_rebuild_stats *stats)
try {
    if (snapshot == nullptr || destination == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto *tree = c_api_tree(destination);
    auto *core = TreeCoreAccess::get(*tree);
    if (core == nullptr) {
        return static_cast<ct_status>(Code::kNotSupported);
    }
    RuntimeRangeImport staging;
    const char        *phase  = "range copy";
    auto               status = copy_snapshot_range(snapshot->context, *core, core->config().key_range, staging);
    if (status.ok()) {
        phase  = "range installation";
        status = core->install_readable(std::move(staging.catalog), staging.credit);
    }
    if (status.ok()) {
        phase  = "persistent flush";
        status = tree->snapshot();
    }
    if (status.ok() && stats != nullptr) {
        const auto &value = staging.stats;
        *stats            = {.entries_examined = value.entries_examined,
                             .entries_emitted  = value.entries_emitted,
                             .entries_filtered = value.entries_filtered,
                             .pages_reused     = value.pages_reused,
                             .pages_rebuilt    = value.pages_rebuilt,
                             .subtrees_skipped = value.subtrees_skipped};
    }
    if (!status.ok()) {
        CRB_LOG_WARN("immutable snapshot {} failed: {}", phase, status.to_string());
    }
    return static_cast<ct_status>(status.code());
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (const std::exception &error) {
    CRB_LOG_ERROR("immutable snapshot range rebuild threw: {}", error.what());
    return static_cast<ct_status>(Code::kInternal);
}
catch (...) {
    CRB_LOG_ERROR("immutable snapshot range rebuild threw an unknown exception");
    return static_cast<ct_status>(Code::kInternal);
}
