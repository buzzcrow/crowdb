// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_maintenance_internal.h"
#include "crowdb-tree/btree/tree.h"

using namespace crowdb::tree;

uint64_t ct_durable_coverage(ct_tree *tree)
{
    return tree == nullptr ? 0 : c_api_tree(tree)->durable_snapshot_last_applied_slot();
}

namespace crowdb::tree::c_api_detail
{
void store_report(ct_future_impl &result, const FlushResult &report, const Status &terminal) noexcept
{
    auto status = terminal.code();
    if (terminal.ok()) {
        status = report.handoff_status.ok() ? report.maintenance_status.code() : report.handoff_status.code();
    }
    const bool covered = report.phase == FlushPhase::kCovered || report.phase == FlushPhase::kMaintenance ||
                         report.phase == FlushPhase::kComplete;
    result.maintenance =
        ct_flush_result_v1{.struct_size        = sizeof(ct_flush_result_v1),
                           .version            = 1,
                           .terminal_status    = static_cast<int32_t>(status),
                           .handoff_status     = static_cast<int32_t>(report.handoff_status.code()),
                           .maintenance_status = static_cast<int32_t>(report.maintenance_status.code()),
                           .phase              = static_cast<uint32_t>(report.phase),
                           .batch_id           = report.batch_id,
                           .floor              = report.floor,
                           .frontier           = report.frontier,
                           .published_coverage = report.published_coverage,
                           .deferred_pages     = report.deferred_pages,
                           .handoff_completed  = report.handoff_status.ok() && covered ? 1 : 0,
                           .handoff_advanced   = report.handoff_advanced ? 1 : 0};
}

void store_report(ct_future_impl &result, const CheckpointResult &report, const Status &terminal) noexcept
{
    const auto &status = terminal.ok() ? report.status : terminal;
    result.maintenance = ct_checkpoint_result_v1{.struct_size         = sizeof(ct_checkpoint_result_v1),
                                                 .version             = 1,
                                                 .terminal_status     = static_cast<int32_t>(status.code()),
                                                 .durable_committed   = report.durable_committed ? 1 : 0,
                                                 .writeback_completed = report.writeback_completed ? 1 : 0,
                                                 .durable_coverage    = report.durable_coverage,
                                                 .snapshot_sequence   = report.snapshot_sequence,
                                                 .checkpoint_token    = report.checkpoint_token,
                                                 .captured_coverage   = report.captured_coverage,
                                                 .primary_frames      = report.primary_frames,
                                                 .written_frames      = report.written_frames,
                                                 .reused_frames       = report.reused_frames,
                                                 .rebased_pages       = report.rebased_pages,
                                                 .rebase_deferred     = report.rebase_deferred};
}
} // namespace crowdb::tree::c_api_detail

namespace
{
template <class Report> int32_t poll_report(ct_future *future, int32_t *done, Report *out, ct_future_impl::Kind kind)
{
    if (future == nullptr || done == nullptr || out == nullptr || out->struct_size != sizeof(*out) ||
        out->version != 1) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    auto       *handle = reinterpret_cast<ct_future_handle *>(future);
    const auto &result = **handle;
    if (result.kind != kind) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    if (!result.done.load(std::memory_order_acquire)) {
        *done = 0;
        return 0;
    }
    const auto *report = std::get_if<Report>(&result.maintenance);
    if (report == nullptr) {
        return static_cast<int32_t>(Code::kInternal);
    }
    *out  = *report;
    *done = 1;
    free_ct_future_handle(handle);
    return 0;
}
} // namespace

int32_t ct_future_poll_flush_report(ct_future *future, int32_t *done, ct_flush_result_v1 *out)
{
    return poll_report(future, done, out, ct_future_impl::Kind::kFlushReport);
}

int32_t ct_future_poll_checkpoint_report(ct_future *future, int32_t *done, ct_checkpoint_result_v1 *out)
{
    return poll_report(future, done, out, ct_future_impl::Kind::kCheckpointReport);
}
