// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "extent_ledger_internal.h"

#include <algorithm>

namespace crowdb::tree
{
ExtentLedgerResult ExtentLedger::State::exclude(Request &request)
{
    if (!active || excluded != nullptr) {
        return {.status = Status::unavailable(), .extent = {}};
    }
    if (request.address < limits.region_begin || request.address + request.length > append) {
        return {.status = Status::invalid_argument(), .extent = {}};
    }
    if (next_identity == UINT64_MAX) {
        return {.status = Status::resource_exhausted(), .extent = {}};
    }
    const auto end  = request.address + request.length;
    auto       next = ranges.upper_bound(request.address);
    if (next != ranges.begin()) {
        --next;
    }
    uint64_t occupied = 0;
    size_t   visits   = 0;
    for (; next != ranges.end() && next->first < end; ++next) {
        if (++visits > 4096) {
            return {.status = Status::resource_exhausted(), .extent = {}};
        }
        const auto overlap_begin = std::max(next->first, request.address);
        const auto overlap_end   = std::min(next->first + next->second.length, end);
        if (next->second.record != nullptr && overlap_begin < overlap_end) {
            occupied += overlap_end - overlap_begin;
        }
    }
    auto prepared      = detail::reserved_unique<Record>(request.metadata);
    prepared->metadata = request.metadata;
    prepared->ledger   = shared_from_this();
    auto *record       = prepared.release();
    auto  owner        = std::shared_ptr<Record>(
        record,
        [](Record *released) {
            auto ledger = released->ledger;
            ledger->enqueue(&released->command);
        },
        detail::ReservationAllocator<Record>(request.metadata));
    record->identity  = next_identity++;
    record->address   = request.address;
    record->length    = request.length;
    record->exclusive = true;
    record->owner     = owner;
    excluded          = record;
    return {.status = Status::Ok(), .extent = describe(*record, std::move(owner)), .occupied_bytes = occupied};
}

ExtentLedgerResult ExtentLedger::State::reserve_outside_exclusion(Request &request)
{
    const auto avoid_begin = excluded->address;
    const auto avoid_end   = avoid_begin + excluded->length;
    size_t     visits      = 0;
    for (auto gap = free.lower_bound({request.length, 0}); gap != free.end() && visits < 64; ++gap, ++visits) {
        const auto begin = gap->second;
        const auto end   = begin + gap->first;
        if (begin >= avoid_end || (begin < avoid_begin && request.length <= avoid_begin - begin)) {
            return allocate(request, ranges.find(begin), begin);
        }
        if (end > avoid_end && request.length <= end - avoid_end) {
            return allocate(request, ranges.find(begin), avoid_end);
        }
    }
    // Exclusions cover existing storage only, so append is always outside.
    if (request.length > limits.address_limit - append) {
        return {.status = Status::resource_exhausted(), .extent = {}};
    }
    return allocate(request, ranges.end(), append);
}
} // namespace crowdb::tree
