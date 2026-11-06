// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/local_compaction_operation.h"
#include "c_api_maintenance_start.h"

#include <chrono>

using namespace crowdb::tree;

int32_t ct_compact_sparse_blocks_async(ct_tree *tree, const ct_compaction_options_v1 *options, ct_future **out)
{
    const auto             started = std::chrono::steady_clock::now();
    LocalCompactionOptions policy;
    if (options != nullptr) {
        if (options->version != 1 || options->struct_size != sizeof(*options)) {
            return static_cast<int32_t>(Code::kInvalidArgument);
        }
        policy = {.free_threshold = options->free_threshold, .max_relocation_bytes = options->max_relocation_bytes};
    }
    return c_api_detail::begin_report<LocalCompactionOperation>(
        tree, out, ct_future_impl::Kind::kCompaction,
        [policy, tree, started](TreeCore &core, std::shared_ptr<LocalCompactionOperation> *operation) {
            auto status = core.prepare_compaction(policy, operation);
            if (status.ok()) {
                auto *facade = c_api_tree(tree);
                (*operation)->observe_completion([facade, started](const MergeGcStats &report) {
                    const auto elapsed = std::chrono::steady_clock::now() - started;
                    TreeCoreAccess::record_compaction(
                        *facade, report,
                        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()));
                });
            }
            return status;
        },
        false);
}

namespace crowdb::tree::c_api_detail
{
void store_report(ct_future_impl &result, const MergeGcStats &report, const Status &terminal) noexcept
{
    result.maintenance = ct_compaction_result_v1{.struct_size     = sizeof(ct_compaction_result_v1),
                                                 .version         = 1,
                                                 .terminal_status = static_cast<int32_t>(terminal.code()),
                                                 .blocks_selected = report.blocks_selected,
                                                 .pages_relocated = report.pages_relocated,
                                                 .bytes_relocated = report.bytes_relocated,
                                                 .blocks_deleted  = report.blocks_deleted};
}
} // namespace crowdb::tree::c_api_detail

int32_t ct_future_poll_compaction(ct_future *future, int32_t *done, ct_compaction_result_v1 *out)
{
    if (future == nullptr || done == nullptr || out == nullptr || out->version != 1 ||
        out->struct_size != sizeof(*out)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    auto       *handle = reinterpret_cast<ct_future_handle *>(future);
    const auto &result = **handle;
    if (result.kind != ct_future_impl::Kind::kCompaction) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    if (!result.done.load(std::memory_order_acquire)) {
        *done = 0;
        return 0;
    }
    const auto *report = std::get_if<ct_compaction_result_v1>(&result.maintenance);
    if (report == nullptr) {
        return static_cast<int32_t>(Code::kInternal);
    }
    *out  = *report;
    *done = 1;
    free_ct_future_handle(handle);
    return 0;
}
