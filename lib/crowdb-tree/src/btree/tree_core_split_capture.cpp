// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_internal.h"
#include "btree/tree_core_split_wait.h"
#include "snapshot/published_snapshot.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
#ifdef CROWDB_TREE_TEST_UTIL
thread_local std::function<void()> between_captures_for_tests;
#endif

// The caller retains source ingress throughout this host-only wait. A busy
// prepare gate or an in-flight writer pause is waitable; quota failure is not.
Status prepare_split_capture(TreeCore &core, FlushController &controller, FlushPreparationOptions preparation,
                             ApplyProgress &progress, std::shared_ptr<FlushOperation> *out)
{
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    auto status = core.operation_lifetime().acquire(core.binding()->lifecycle_budget(), &ticket);
    if (!status.ok()) {
        return status;
    }
    preparation.store = ticket->backend(core.async_backend());
    for (;;) {
        const auto observed = progress.sequence();
        bool       blocked  = false;
        status              = controller.prepare(preparation, out, &blocked);
        if (status.ok() || !blocked) {
            return status;
        }
        status = progress.wait(observed);
        if (!status.ok()) {
            return status;
        }
    }
}

Status advance_split_capture(const std::shared_ptr<FlushOperation> &operation)
{
    auto status = operation->wait();
#ifdef CROWDB_TREE_TEST_UTIL
    if (status.ok() && between_captures_for_tests) {
        auto callback              = std::move(between_captures_for_tests);
        between_captures_for_tests = {};
        callback();
    }
#endif
    return status;
}
} // namespace

#ifdef CROWDB_TREE_TEST_UTIL
void set_between_split_captures_for_tests(std::function<void()> callback)
{
    between_captures_for_tests = std::move(callback);
}
#endif

Status SplitSourceSession::retain(const FrozenSourceView &frozen)
{
    if (!frozen.catalog) {
        return Status::Ok();
    }
    if (frozen.catalog->generation != generation || frozen.frontier < frozen.catalog->floor) {
        return Status::unavailable("split source generation changed");
    }
    size_t added = 0;
    for (const auto &table : frozen.catalog->tables) {
        if (std::find(tables.begin(), tables.begin() + static_cast<std::ptrdiff_t>(table_count), table) ==
            tables.begin() + static_cast<std::ptrdiff_t>(table_count)) {
            ++added;
        }
    }
    const bool same_interval = interval_count != 0 && intervals[interval_count - 1].floor == frozen.catalog->floor &&
                               intervals[interval_count - 1].frontier == frozen.frontier;
    if (added > kMaxSources - table_count || (!same_interval && interval_count == intervals.size())) {
        return Status::resource_exhausted("split source retention window is full");
    }
    for (const auto &table : frozen.catalog->tables) {
        if (std::find(tables.begin(), tables.begin() + static_cast<std::ptrdiff_t>(table_count), table) ==
            tables.begin() + static_cast<std::ptrdiff_t>(table_count)) {
            tables[table_count++] = table;
        }
    }
    if (!same_interval) {
        intervals[interval_count++] = {.floor = frozen.catalog->floor, .frontier = frozen.frontier};
    }
    if (frozen.catalog->active_budget) {
        const auto &source_limits = frozen.catalog->active_budget->options();
        limits.max_entries        = std::max(limits.max_entries, source_limits.max_entries);
        limits.max_key_bytes      = std::max(limits.max_key_bytes, source_limits.max_key_bytes);
        limits.max_gap_entries    = std::max(limits.max_gap_entries, source_limits.max_gap_entries);
        limits.max_gap_key_bytes  = std::max(limits.max_gap_key_bytes, source_limits.max_gap_key_bytes);
        limits.max_key_size       = std::max(limits.max_key_size, source_limits.max_key_size);
    }
    return Status::Ok();
}

bool SplitSourceSession::covers(uint64_t floor, uint64_t end) const noexcept
{
    auto covered = floor;
    for (size_t i = 0; i < interval_count && covered < end; ++i) {
        if (intervals[i].floor > covered) {
            return false;
        }
        covered = std::max(covered, intervals[i].frontier);
    }
    return covered >= end;
}

Status TreeCore::State::capture_split_sources(TreeCore &core, uint64_t target)
{
    if (detail::FlushRuntime::on_worker_thread()) {
        return Status::unavailable("a runtime worker cannot wait for split capture");
    }
    if (!split_sources) {
        const auto bound = detail::shared_allocation_bound<SplitSourceSession>() +
                           detail::shared_allocation_bound<detail::AllocationReservation>();
        auto reserved = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kRouting, bound);
        if (!reserved || split_sequence == UINT64_MAX) {
            return Status::resource_exhausted("split retention admission unavailable");
        }
        auto credit               = detail::AllocationReservation::create(std::move(*reserved));
        split_sources             = detail::reserved_shared<SplitSourceSession>(credit);
        split_sources->generation = publisher->capture()->generation;
        split_sources->session    = ++split_sequence;
        credit->seal();
    }
    if (split_sources->generation != publisher->capture()->generation) {
        return Status::unavailable("split source belongs to a replaced generation");
    }
    // An existing paused capture can precede target. Finish it once, then
    // capture the current successor. No source admission or epoch is held
    // across the host wait; both senders run on the shared CPU runtime.
    for (size_t pass = 0; pass < 2; ++pass) {
        std::shared_ptr<FlushOperation> operation;
        auto status = prepare_split_capture(core, *flush, preparation, *apply_progress, &operation);
        if (!status.ok()) {
            return status;
        }
        FlushResult report;
        status = operation->wait_capture(&report);
        if (!status.ok()) {
            return status;
        }
        if (!report.handoff_status.ok()) {
            return report.handoff_status;
        }
        // An unpublished first capture below the fixed target need not become
        // the session baseline. Other normal handoffs may consume intervening
        // sources before the successor is captured; choose its paired L1 once.
        if (!split_sources->baseline && report.frontier < target) {
            status = advance_split_capture(operation);
            if (!status.ok()) {
                return status;
            }
            continue;
        }
        const auto frozen = operation->frozen_sources();
        if (!frozen.base || (frozen.catalog && frozen.base->coverage != frozen.catalog->floor)) {
            return Status::unavailable("split capture has no complete paired L1 base");
        }
        if (split_sources->baseline && frozen.base->coverage > split_sources->frontier) {
            // An installed overlay already fixes the older baseline. Normal
            // handoffs can consume newer intervals that this session never
            // retained. Return its real contiguous prefix; the caller replays
            // beyond that boundary instead of inheriting unavailable history.
            return Status::Ok();
        }
        auto baseline = split_sources->baseline;
        if (!baseline) {
            status = core.capture_snapshot(frozen.base, &baseline);
            if (!status.ok()) {
                return status;
            }
        }
        status = split_sources->retain(frozen);
        if (!status.ok()) {
            return status;
        }
        split_sources->baseline = std::move(baseline);
        split_sources->frontier = std::max(split_sources->frontier, report.frontier);
        if (report.frontier >= target) {
            return Status::Ok();
        }
        status = advance_split_capture(operation);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::unavailable("split source did not reach its fixed capture target");
}
} // namespace crowdb::tree
