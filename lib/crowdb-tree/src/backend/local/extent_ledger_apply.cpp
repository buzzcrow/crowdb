// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "extent_ledger_internal.h"

namespace crowdb::tree
{
ExtentLedgerResult ExtentLedger::State::apply(Request &request)
{
    if (request.command.action == Action::Activate) {
        active = true;
        return {};
    }
    if (request.length == 0 || request.length % limits.iu_size != 0 || request.address % limits.iu_size != 0 ||
        request.address > UINT64_MAX - request.length) {
        return {.status = Status::invalid_argument(), .extent = {}};
    }
    if (request.command.action == Action::Exclude) {
        return exclude(request);
    }
    const bool claim = request.command.action == Action::ClaimFree;
    if (claim && !active) {
        return {.status = Status::unavailable(), .extent = {}};
    }
    if (request.command.action == Action::Reserve) {
        if (!active) {
            return {.status = Status::unavailable(), .extent = {}};
        }
        if (excluded != nullptr) {
            return reserve_outside_exclusion(request);
        }
        const auto gap = free.lower_bound({request.length, 0});
        if (gap != free.end()) {
            return allocate(request, ranges.find(gap->second), gap->second);
        }
        if (request.length > limits.address_limit - append) {
            return {.status = Status::resource_exhausted(), .extent = {}};
        }
        return allocate(request, ranges.end(), append);
    }
    if (request.address < limits.region_begin || request.address + request.length > append) {
        return {.status = claim ? Status::not_found() : Status::invalid_argument(), .extent = {}};
    }
    if (request.command.action == Action::Recover && active) {
        return {.status = Status::unavailable(), .extent = {}};
    }
    auto at = ranges.upper_bound(request.address);
    if (at == ranges.begin()) {
        return {.status = Status::not_found(), .extent = {}};
    }
    --at;
    if (request.address + request.length > at->first + at->second.length) {
        return {.status = claim ? Status::not_found() : Status::corruption(), .extent = {}};
    }
    if (at->second.record != nullptr) {
        auto &record = *at->second.record;
        if (claim || record.exclusive) {
            return {.status = Status::not_found(), .extent = {}};
        }
        if (request.command.action == Action::Recover &&
            (request.address != record.address || request.length != record.length)) {
            return {.status = Status::corruption(), .extent = {}};
        }
        auto owner = record.owner.lock();
        if (!owner) {
            return {.status = Status::unavailable(), .extent = {}};
        }
        return {.status = Status::Ok(), .extent = describe(record, std::move(owner))};
    }
    if (request.command.action == Action::Retain) {
        return {.status = Status::not_found(), .extent = {}};
    }
    return allocate(request, at, request.address);
}
} // namespace crowdb::tree
