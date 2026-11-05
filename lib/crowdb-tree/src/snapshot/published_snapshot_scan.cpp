// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "btree/allocation_bound.h"
#include "btree/read_value_bound.h"
#include "btree/scan_output.h"
#include "btree/tree_core.h"
#include "snapshot/operation_wait.h"
#include "snapshot/published_snapshot.h"
#include "snapshot/published_snapshot_budget.h"

#include <algorithm>
#include <chrono>
#include <utility>

namespace crowdb::tree
{
namespace
{
std::optional<size_t> result_count_bound(const PublishedTreeState &view, const PageLoadLimits &loads,
                                         bool include_tombstones)
{
    // Snapshot capture removes the live source catalog. Exact live-key counts
    // exclude tombstones; otherwise each captured base contributes at most its
    // configured leaf capacity in addition to the immutable overlay entries.
    if (!include_tombstones && view.stats.kv_unknown_pages == 0) {
        return view.stats.kv_known_count;
    }
    const auto shape = view.mapping.copy_bounds();
    const auto bases = detail::AllocationBound().add_product(shape.leaf_pages, shape.max_leaf_bases).value();
    if (!bases) {
        return {};
    }
    return detail::AllocationBound(view.stats.overlay_entries).add_product(*bases, loads.max_leaf_entries).value();
}

PageKeyRange scan_range(CoreScanOptions options, const PageKeyRange &domain,
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
    if (domain.lower && (!range.lower || *domain.lower > *range.lower)) {
        range.lower = domain.lower;
    }
    if (domain.upper && (!range.upper || *domain.upper < *range.upper)) {
        range.upper = domain.upper;
    }
    return range;
}

bool expired(uint64_t deadline)
{
    return deadline != 0 && std::cmp_greater_equal(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                       std::chrono::system_clock::now().time_since_epoch())
                                                       .count(),
                                                   deadline);
}

Status next_scan(const std::shared_ptr<PublishedScan> &scan)
{
    detail::SnapshotWaitCompletion completion;
    auto operation = stdexec::connect(scan->next_sender(), detail::SnapshotWaitReceiver{.completion = &completion});
    stdexec::start(operation);
    completion.done.wait();
    return completion.status.ok() ? scan->result().status : completion.status;
}

Status pack_window(const PublishedScanResult &result, CoreScanOptions options, size_t bytes_limit,
                   size_t normal_capacity, size_t value_limit, ScanPackedBuf *packed, size_t *count, size_t *bytes,
                   bool *truncated)
{
    for (const auto &entry : result.entries) {
        const Slice key(entry.key);
        const Slice value = entry.cell.value;
        if (key.size() > UINT32_MAX || value.size() > UINT32_MAX || value.size() > value_limit) {
            return Status::resource_exhausted("snapshot scan entry exceeds configured limits");
        }
        const auto size    = key.size() + value.size();
        const bool exceeds = size > bytes_limit - std::min(*bytes, bytes_limit);
        if (*count >= options.limit || (exceeds && *count != 0) || expired(options.deadline_ms)) {
            *truncated = true;
            return Status::Ok();
        }
        if (exceeds && options.strict_byte_budget) {
            *truncated = true;
            return Status::Ok();
        }
        detail::reserve_scan_output(*packed, size, normal_capacity);
        packed->pack_u32(static_cast<uint32_t>(key.size()));
        packed->append(key);
        packed->pack_u64(entry.cell.slot);
        packed->push_back(entry.cell.tombstone() ? 1 : 0);
        packed->pack_u32(static_cast<uint32_t>(value.size()));
        packed->append(value);
        ++*count;
        *bytes += size;
    }
    *truncated = !result.done && (result.byte_limit_reached || *count >= options.limit || *bytes >= bytes_limit);
    return Status::Ok();
}
} // namespace

Status PublishedSnapshotContext::scan(CoreScanOptions options, ScanPackedBuf *out, size_t *count, bool *truncated) const
try {
    if (out == nullptr || count == nullptr || truncated == nullptr ||
        (options.strict_byte_budget && options.byte_budget == 0)) {
        return Status::invalid_argument("snapshot scan requires bounded output");
    }
    *out        = {};
    *count      = 0;
    *truncated  = false;
    auto limits = read_limits();
    for (const auto key : {options.prefix, options.start, options.end, options.begin}) {
        if (key.size() > limits.max_key_bytes) {
            return Status::invalid_argument("snapshot scan boundary exceeds key limit");
        }
    }
    const auto root = view()->mapping.get(view()->root_page_id);
    if (!root || root->level >= limits.max_route_depth) {
        return Status::corruption("snapshot scan has no bounded root");
    }
    using detail::AllocationBound;
    const auto loads         = load_limits();
    const auto maximum_count = result_count_bound(*view(), loads, options.include_tombstones);
    if (!maximum_count) {
        return Status::resource_exhausted("snapshot scan result count bound overflow");
    }
    options.limit           = options.limit == 0 ? *maximum_count : std::min(options.limit, *maximum_count);
    limits.max_route_depth  = root->level + 1ULL;
    const auto byte_limit   = options.byte_budget == 0 ? SIZE_MAX : options.byte_budget;
    limits.max_result_bytes = std::min(byte_limit, limits.max_result_bytes);
    // The public limit bounds the whole packed result. Each read still uses a
    // fixed work window, with at most one zero-byte empty-key result.
    const auto byte_count  = AllocationBound(limits.max_result_bytes).add(1).value().value_or(SIZE_MAX);
    limits.max_entries     = std::max<size_t>(1, std::min({options.limit, limits.max_entries, byte_count}));
    const auto workers     = binding()->cpu()->worker_count();
    const auto normal      = AllocationBound(limits.max_result_bytes).add_product(options.limit, 17).value();
    const auto value_limit = overflow_value_bytes_bound(*view(), loads.max_value_bytes);
    const auto values      = AllocationBound(value_limit).add(value_limit == 0 ? 0 : limits.max_result_bytes).value();
    if (!normal || !values) {
        return Status::resource_exhausted("snapshot scan result bound overflow");
    }
    const auto leaves = snapshot_leaf_window(*view(), limits.max_entries);
    if (!leaves) {
        return Status::resource_exhausted("snapshot scan frame window overflow");
    }
    auto bounded_loads               = loads;
    bounded_loads.max_overflow_pages = std::min<uint64_t>(loads.max_overflow_pages, value_limit);
    const auto loader = value_limit == 0
                          ? AsyncStorePageLoader::primary_reserve_bound(loads, root->level, workers, *leaves)
                          : AsyncStorePageLoader::read_reserve_bound(bounded_loads, root->level, workers, *leaves,
                                                                     limits.max_entries, *values);
    const auto bound  = AllocationBound()
                            .add(PublishedScan::allocation_bound(limits, workers))
                            .add(loader)
                            .add_product(6, OwnedKey::allocation_overhead() + limits.max_key_bytes + 1)
                            .value();
    if (!bound) {
        return Status::resource_exhausted("snapshot scan read bound overflow");
    }
    SnapshotReadWindow window;
    auto               status = prepare_window(*bound, &window);
    if (!status.ok()) {
        return status;
    }
    limits.metadata = window.budget.bytes;
    PublishedScanOptions selection{.start_inclusive       = options.start_inclusive,
                                   .keys_only             = options.keys_only,
                                   .include_tombstones    = options.include_tombstones,
                                   .allow_oversized_first = !options.strict_byte_budget,
                                   .byte_budget           = byte_limit};
    if (options.has_start_bound || !options.start.empty()) {
        selection.start = OwnedKey(options.start, limits.metadata);
    }
    std::shared_ptr<PublishedScan> scan;
    status = PublishedScan::create(view(), window.loader, scan_range(options, range(), limits.metadata),
                                   options.reverse, limits, &scan, std::move(selection));
    std::shared_ptr<detail::AllocationAccount> output;
    if (status.ok()) {
        status = detail::AllocationAccount::create(binding()->account(), detail::BudgetClass::kRouting, &output);
    }
    ScanPackedBuf packed(std::move(output));
    size_t        bytes = 0;
    while (status.ok()) {
        if (expired(options.deadline_ms)) {
            *truncated = true;
            break;
        }
        status = next_scan(scan);
        if (!status.ok()) {
            break;
        }
        status = pack_window(scan->result(), options, byte_limit, *normal, loads.max_value_bytes, &packed, count,
                             &bytes, truncated);
        if (scan->result().done || *truncated) {
            break;
        }
    }
    if (status.ok()) {
        *out = std::move(packed);
    }
    window.seal();
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot scan allocation failed");
}
} // namespace crowdb::tree
