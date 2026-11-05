// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "extent_ledger_internal.h"

#include <cassert>

namespace crowdb::tree
{
ExtentLedger::State::PreparedNodes ExtentLedger::State::prepare_nodes()
{
    PreparedNodes nodes;
    Ranges        spare_ranges(&resource);
    FreeRanges    spare_free(&resource);
    for (size_t i = 0; i < nodes.ranges.size(); ++i) {
        spare_ranges.emplace(0, Range{.length = 0, .record = nullptr});
        nodes.ranges[i] = spare_ranges.extract(spare_ranges.begin());
        spare_free.emplace(0, 0);
        nodes.free[i] = spare_free.extract(spare_free.begin());
    }
    return nodes;
}

ExtentLedgerResult ExtentLedger::State::allocate(Request &request, Ranges::iterator gap, uint64_t address)
{
    const bool   reuse     = gap != ranges.end();
    const auto   old_begin = reuse ? gap->first : address;
    const auto   old_end   = reuse ? old_begin + gap->second.length : address + request.length;
    const auto   end       = address + request.length;
    const size_t extra     = static_cast<size_t>(old_begin < address) + static_cast<size_t>(end < old_end);
    if (next_identity == UINT64_MAX || extra + static_cast<size_t>(!reuse) > limits.max_ranges - ranges.size()) {
        return {.status = Status::resource_exhausted(), .extent = {}};
    }
    // Every node needed for splitting now or returning this extent later is
    // paid before changing either index. Final owner release never allocates.
    auto nodes          = prepare_nodes();
    auto allocated      = detail::reserved_unique<Record>(request.metadata);
    allocated->metadata = request.metadata;
    allocated->ledger   = shared_from_this();
    auto *record        = allocated.release();
    auto  owner         = std::shared_ptr<Record>(
        record,
        [](Record *released) {
            auto ledger = released->ledger;
            ledger->enqueue(&released->command);
        },
        detail::ReservationAllocator<Record>(request.metadata));
    record->exclusive = request.command.action == Action::ClaimFree;
    record->owner     = owner;
    record->identity  = next_identity;
    record->address   = address;
    record->length    = request.length;

    Ranges::node_type current;
    if (reuse) {
        record->free_node = free.extract({gap->second.length, gap->first});
        assert(!record->free_node.empty());
        current          = ranges.extract(gap);
        size_t index     = 0;
        auto   remainder = [&](uint64_t begin, uint64_t length) {
            auto range     = std::move(nodes.ranges[index]);
            range.key()    = begin;
            range.mapped() = {.length = length, .record = nullptr};
            ranges.insert(std::move(range));
            auto available    = std::move(nodes.free[index++]);
            available.value() = {length, begin};
            free.insert(std::move(available));
        };
        if (old_begin < address) {
            remainder(old_begin, address - old_begin);
        }
        if (end < old_end) {
            remainder(end, old_end - end);
        }
    }
    else {
        current           = std::move(nodes.ranges[0]);
        record->free_node = std::move(nodes.free[0]);
        append            = end;
    }
    current.key()    = address;
    current.mapped() = {.length = request.length, .record = record};
    ranges.insert(std::move(current));
    record->installed = true;
    if (!record->exclusive) {
        record->metrics.activate(request.metadata ? request.metadata->resource_metrics() : nullptr, request.length);
    }
    address_high_water.store(append, std::memory_order_release);
    ++next_identity;
    protected_bytes.fetch_add(request.length, std::memory_order_relaxed);
    protected_ranges.fetch_add(1, std::memory_order_relaxed);
    if (request.command.action == Action::Reserve) {
        (reuse ? reused_allocations : appended_allocations).fetch_add(1, std::memory_order_relaxed);
    }
    return {.status = Status::Ok(), .extent = describe(*record, std::move(owner))};
}

void ExtentLedger::State::release(Record *record) noexcept
{
    if (record == excluded) {
        excluded = nullptr;
    }
    if (record->installed) {
        auto at = ranges.find(record->address);
        assert(at != ranges.end() && at->second.record == record);
        auto node  = ranges.extract(at);
        auto begin = record->address;
        auto end   = begin + record->length;
        auto next  = ranges.lower_bound(begin);
        if (next != ranges.end() && next->first == end && next->second.record == nullptr) {
            end += next->second.length;
            free.erase({next->second.length, next->first});
            ranges.erase(next);
        }
        next = ranges.lower_bound(begin);
        if (next != ranges.begin()) {
            auto previous = std::prev(next);
            if (previous->second.record == nullptr && previous->first + previous->second.length == begin) {
                begin = previous->first;
                free.erase({previous->second.length, previous->first});
                ranges.erase(previous);
            }
        }
        node.key()    = begin;
        node.mapped() = {.length = end - begin, .record = nullptr};
        ranges.insert(std::move(node));
        record->free_node.value() = {end - begin, begin};
        free.insert(std::move(record->free_node));
        protected_bytes.fetch_sub(record->length, std::memory_order_relaxed);
        protected_ranges.fetch_sub(1, std::memory_order_relaxed);
    }
    detail::ReservationAllocator<Record> allocator(record->metadata);
    std::destroy_at(record);
    allocator.deallocate(record, 1);
}
} // namespace crowdb::tree
