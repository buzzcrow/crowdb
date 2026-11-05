// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/scan_output.h"
#include "btree/tree_core_internal.h"
#include "btree/tree_core_scan_internal.h"

#include <algorithm>
#include <chrono>
#include <utility>

namespace crowdb::tree::core_read_detail
{
bool CoreScan::expired() const noexcept
{
    if (deadline_ms == 0) {
        return false;
    }
    const auto now =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    return std::cmp_greater_equal(now, deadline_ms);
}

void CoreScan::finish(Status status, bool truncated) noexcept
{
    auto self = std::move(owner);
    detail::seal_backend_budget(prepared.backend);
    prepared.io->seal();
    prepared.credit->seal();
    try {
        complete(std::move(status), std::move(packed), truncated);
    }
    catch (...) {
        // Completion is terminal; a throwing consumer cannot be retried.
        return;
    }
}

void CoreScan::step(void *context) noexcept
{
    auto *self = static_cast<CoreScan *>(context);
    try {
        if (self->expired()) {
            self->finish(Status::Ok(), true);
            return;
        }
        auto operation  = detail::reserved_construct<Operation>(self->prepared.credit, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) Operation(stdexec::connect(self->read->next_sender(), Receiver{self}));
        });
        self->operation = operation;
        stdexec::start(*operation);
    }
    catch (const std::bad_alloc &) {
        self->finish(Status::resource_exhausted("scan continuation allocation failed"), false);
    }
    catch (...) {
        self->finish(Status::internal_error("scan continuation failed"), false);
    }
}

void CoreScan::window() noexcept
try {
    const auto &result = read->result();
    if (!result.status.ok()) {
        finish(result.status, false);
        return;
    }
    for (const auto &entry : result.entries) {
        const Slice key(entry.key);
        const Slice value = entry.cell.value;
        if (key.size() > UINT32_MAX || value.size() > UINT32_MAX || value.size() > value_limit) {
            finish(Status::resource_exhausted("scan entry exceeds the configured value limit"), false);
            return;
        }
        const auto size    = key.size() + value.size();
        const bool exceeds = size > byte_limit - std::min(bytes, byte_limit);
        if (count >= limit || (exceeds && count != 0) || expired()) {
            finish(Status::Ok(), true);
            return;
        }
        if (exceeds && strict) {
            finish(Status::Ok(), true);
            return;
        }
        detail::reserve_scan_output(packed, size, normal_capacity);
        packed.pack_u32(static_cast<uint32_t>(key.size()));
        packed.append(key);
        packed.pack_u64(entry.cell.slot);
        packed.push_back(entry.cell.tombstone() ? 1 : 0);
        packed.pack_u32(static_cast<uint32_t>(value.size()));
        packed.append(value);
        record_work(prepared.credit, TreeWorkCounter::MaterializeCopyBytes, size + 17, TreeWorkCause::Read);
        ++count;
        bytes += size;
    }
    if (result.done) {
        finish(Status::Ok(), false);
    }
    else if (result.byte_limit_reached || count >= limit || bytes >= byte_limit) {
        finish(Status::Ok(), true);
    }
    else {
        progress->request(owner);
    }
}
catch (const std::bad_alloc &) {
    finish(Status::resource_exhausted("scan output allocation failed"), false);
}
catch (...) {
    finish(Status::internal_error("scan result packing failed"), false);
}
} // namespace crowdb::tree::core_read_detail

namespace crowdb::tree
{
void TreeCore::scan_async(CoreScanOptions options, std::function<void(Status, ScanPackedBuf, bool)> complete)
{
    scan_impl(options, std::move(complete), true);
}

void TreeCore::scan_impl(CoreScanOptions options, std::function<void(Status, ScanPackedBuf, bool)> complete,
                         bool dispatch)
{
    if (!complete) {
        return;
    }
    std::shared_ptr<core_read_detail::CoreScan> operation;
    Status                                      status;
    try {
        State::Ingress ingress(*state_);
        status = ingress.owner == nullptr ? Status::unavailable("Tree closed")
                                          : core_read_detail::prepare_scan(*this, options, &operation);
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted("scan admission allocation failed");
    }
    if (!status.ok()) {
        complete(std::move(status), {}, false);
        return;
    }
    operation->complete = std::move(complete);
    operation->owner    = operation;
    if (dispatch) {
        operation->progress->request(operation);
    }
    else {
        core_read_detail::CoreScan::step(operation.get());
    }
}
} // namespace crowdb::tree
