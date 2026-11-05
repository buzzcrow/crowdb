// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "btree/allocation_bound.h"
#include "btree/tree_core_scan_internal.h"

#include <algorithm>

namespace crowdb::tree::core_read_detail
{
namespace
{
std::optional<size_t> result_count_bound(const PublishedTreeState &view, const PageLoadLimits &loads,
                                         const TreeExecutionOptions &options, bool include_tombstones)
{
    using detail::AllocationBound;
    const auto            shape = view.mapping.copy_bounds();
    std::optional<size_t> count;
    if (!include_tombstones && view.stats.kv_unknown_pages == 0) {
        count = view.stats.kv_known_count;
    }
    else {
        const auto bases = AllocationBound().add_product(shape.leaf_pages, shape.max_leaf_bases).value();
        if (!bases) {
            return {};
        }
        count = AllocationBound(view.stats.overlay_entries).add_product(*bases, loads.max_leaf_entries).value();
    }
    if (!count) {
        return {};
    }
    AllocationBound total(*count);
    // Captured sources may still accept later writes. Their hard version quota,
    // not the current count, bounds those future visible keys. Core admission
    // keeps these hard quotas independent of the configurable soft thresholds;
    // inherited split tables are closed and drained, so use their exact count.
    const auto future = AllocationBound(options.source_max_entries).add(options.source_max_gap_entries).value();
    if (!future) {
        return {};
    }
    for (const auto &table : view.sources->tables) {
        total.add(table->closed() && table->writers() == 0 ? table->count() : *future);
    }
    return total.value();
}

PageKeyRange selection(CoreScanOptions options, const KeyRange &domain,
                       const std::shared_ptr<detail::AllocationReservation> &credit)
{
    PageKeyRange range;
    if (!options.prefix.empty()) {
        range.lower = OwnedKey(options.prefix, credit);
        KeyArenaStorage upper(options.prefix.data(), options.prefix.size(), detail::ReservationAllocator<char>(credit));
        while (!upper.empty() && static_cast<uint8_t>(upper.back()) == 255) {
            upper.pop_back();
        }
        if (!upper.empty()) {
            upper.back() = static_cast<char>(static_cast<uint8_t>(upper.back()) + 1);
            range.upper  = OwnedKey(Slice(upper.data(), upper.size()), credit);
        }
    }
    if (!options.begin.empty() && (!range.lower || options.begin > Slice(*range.lower))) {
        range.lower = OwnedKey(options.begin, credit);
    }
    if (!options.end.empty() && (!range.upper || options.end < Slice(*range.upper))) {
        range.upper = OwnedKey(options.end, credit);
    }
    if (domain.start() && (!range.lower || Slice(*domain.start()) > Slice(*range.lower))) {
        range.lower = OwnedKey(Slice(*domain.start()), credit);
    }
    if (domain.end() && (!range.upper || Slice(*domain.end()) < Slice(*range.upper))) {
        range.upper = OwnedKey(Slice(*domain.end()), credit);
    }
    return range;
}
} // namespace

Status prepare_scan(TreeCore &core, CoreScanOptions options, std::shared_ptr<CoreScan> *out)
{
    using namespace detail;
    auto       limits = core.read_limits();
    const auto view   = core.capture();
    for (const auto key : {options.prefix, options.start, options.end, options.begin}) {
        if (key.size() > limits.max_key_bytes) {
            return Status::invalid_argument("scan boundary exceeds the key limit");
        }
    }
    const auto maximum_count =
        result_count_bound(*view, core.load_limits(), core.config().execution, options.include_tombstones);
    if (!maximum_count) {
        return Status::resource_exhausted("scan result count bound overflow");
    }
    const size_t count        = options.limit == 0 ? *maximum_count : std::min(options.limit, *maximum_count);
    const size_t bytes        = options.byte_budget == 0 ? SIZE_MAX : options.byte_budget;
    const auto   window_bytes = std::min(bytes, limits.max_result_bytes);
    const auto   normal       = AllocationBound(window_bytes).add_product(count, 17).value();
    if (!normal) {
        return Status::resource_exhausted("scan result bound overflow");
    }
    const auto control =
        AllocationBound(shared_allocation_bound<CoreScan>())
            .add_product(core.binding()->cpu()->worker_count() + 1, shared_allocation_bound<CoreScan::Operation>())
            .add(FlushRuntime::progress_allocation_bound())
            .add_product(6, OwnedKey::allocation_overhead() + limits.max_key_bytes + 1)
            .add_product(2, OwnedKey::allocation_overhead() + 1)
            .add(core.config().key_range.start() ? core.config().key_range.start()->size() : 0)
            .add(core.config().key_range.end() ? core.config().key_range.end()->size() : 0)
            .value();
    if (!control) {
        return Status::resource_exhausted("scan control bound overflow");
    }
    // Keys are unique: all except the one possible empty key cost at least one
    // byte. A tiny byte window cannot contain more than bytes + 1 results;
    // the oversized-first exception contributes only one result.
    const auto byte_count   = AllocationBound(window_bytes).add(1).value().value_or(SIZE_MAX);
    limits.max_entries      = std::max<size_t>(1, std::min({count, limits.max_entries, byte_count}));
    limits.max_result_bytes = window_bytes;
    ReadPreparation ready;
    auto            status = prepare(core, view, limits, true, *control, 0, &ready);
    if (!status.ok()) {
        return status;
    }
    auto scan                     = reserved_shared<CoreScan>(ready.credit);
    scan->prepared                = std::move(ready);
    auto                &prepared = scan->prepared;
    PublishedScanOptions scan_options{.start_inclusive       = options.start_inclusive,
                                      .keys_only             = options.keys_only,
                                      .include_tombstones    = options.include_tombstones,
                                      .allow_oversized_first = !options.strict_byte_budget,
                                      .byte_budget           = bytes};
    if (options.has_start_bound || !options.start.empty()) {
        scan_options.start = OwnedKey(options.start, prepared.credit);
    }
    status = PublishedScan::create(prepared.view, prepared.loader,
                                   selection(options, core.config().key_range, prepared.credit), options.reverse,
                                   prepared.limits, &scan->read, std::move(scan_options));
    if (!status.ok()) {
        return status;
    }
    scan->limit           = count;
    scan->byte_limit      = bytes;
    scan->normal_capacity = *normal;
    scan->value_limit     = core.load_limits().max_value_bytes;
    scan->deadline_ms     = options.deadline_ms;
    scan->strict          = options.strict_byte_budget;
    std::shared_ptr<AllocationAccount> output;
    status = AllocationAccount::create(prepared.binding->account(), BudgetClass::kRouting, &output);
    if (!status.ok()) {
        return status;
    }
    scan->packed = ScanPackedBuf(std::move(output));
    status       = prepared.binding->cpu()->reserve_progress(prepared.binding->lane(), &CoreScan::step, scan.get(),
                                                             prepared.credit, &scan->progress);
    if (status.ok()) {
        *out = std::move(scan);
    }
    return status;
}
} // namespace crowdb::tree::core_read_detail
