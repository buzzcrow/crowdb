// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_core_batch.h"

#include "allocation_bound.h"
#include "apply_progress.h"
#include "crowdb-tree/external_allocation.h"
#include "flush_runtime.h"
#include "source_rotation.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <numeric>

namespace crowdb::tree
{
bool primary_write_fits(const Config &config, size_t key_bytes, size_t value_bytes, bool tombstone) noexcept
{
    constexpr size_t overhead = kFrameHeaderSize + kFrameTrailerSize;
    constexpr size_t inner    = overhead + (2 * sizeof(uint64_t)) + kInnerSlotSize;
    constexpr size_t leaf     = overhead + kLeafSlotSize;
    if (key_bytes > kMaxPhysicalPageBytes - inner || key_bytes > kMaxPhysicalPageBytes - leaf) {
        return false;
    }
    const auto inline_bytes = config.max_inline_value == 0 ? config.frame_bytes / 4 : config.max_inline_value;
    const auto available    = kMaxPhysicalPageBytes - leaf - key_bytes;
    if (tombstone) {
        return kCellHeaderSize <= available;
    }
    if (value_bytes > inline_bytes) {
        return kOverflowCellSize <= available;
    }
    return available >= kCellHeaderSize && value_bytes <= available - kCellHeaderSize;
}

namespace
{
struct StagedWrite
{
    Slice           key;
    AllocationLease charge;
    buffer          value;
    uint8_t         flags = 0;
};

size_t payload_capacity(const CoreWriteInput &input, const std::shared_ptr<AllocationBudget> &credit)
{
    if (input.encoded ? CellView(input.value).is_tombstone() : (input.flags & kFlagTombstone) != 0) {
        return 0;
    }
    if (input.encoded && input.movable->owned()) {
        return input.movable->inlined() ? 0 : input.movable->capacity();
    }
    if (!input.encoded && input.movable != nullptr) {
        const auto *allocation = input.movable->external_allocation();
        if (allocation != nullptr && allocation->charged_to(credit)) {
            return 0;
        }
    }
    const auto bytes = input.encoded ? input.value.size() : input.value.size() + kCellHeaderSize;
    return bytes <= buffer::kInlineCap ? 0 : bytes;
}

StagedWrite stage(CoreWriteInput input, uint64_t slot, const std::shared_ptr<detail::AllocationReservation> &credit)
{
    StagedWrite result{.key    = input.key,
                       .charge = AllocationLease(credit, payload_capacity(input, credit), AllocationKind::kPayload),
                       .value  = {},
                       .flags  = input.flags};
    if (input.encoded ? CellView(input.value).is_tombstone() : (input.flags & kFlagTombstone) != 0) {
        result.value = encode_cell_buf(slot, OpKind::kDelete);
        return result;
    }
    if (input.encoded) {
        result.value = input.movable->owned() ? std::move(*input.movable) : input.movable->clone();
        return result;
    }
    if (input.movable != nullptr) {
        const auto *allocation = input.movable->external_allocation();
        if (allocation != nullptr && allocation->charged_to(credit)) {
            result.value = std::move(*input.movable);
            return result;
        }
    }
    result.value = buffer::alloc(input.value.size(), kCellHeaderSize);
    for (size_t index = 0; index < sizeof(slot); ++index) {
        result.value.data()[index] = static_cast<uint8_t>(slot >> (8 * index));
    }
    result.value.data()[sizeof(slot)] = input.flags;
    if (!input.value.empty()) {
        std::memcpy(result.value.data() + kCellHeaderSize, input.value.data(), input.value.size());
    }
    return result;
}

Status validate_inputs(const Config &config, const SourceWriteOptions &options, size_t max_value_bytes, uint64_t slot,
                       size_t count, const CoreWriteReader &read)
{
    if (count > options.max_entries + options.max_gap_entries) {
        return Status::resource_exhausted("batch exceeds bounded ingress operation count");
    }
    for (size_t index = 0; index < count; ++index) {
        const auto input = read(index);
        if (!config.key_range.contains(input.key) || input.key.size() > options.max_key_size ||
            input.value.size() > SIZE_MAX - kCellHeaderSize) {
            return Status::invalid_argument("batch key or value exceeds Tree limits");
        }
        if (input.encoded) {
            const CellView cell(input.value);
            if (!cell.valid() || cell.slot() != slot) {
                return Status::invalid_argument("encoded cell slot must match its batch");
            }
            if ((cell.flags() & ~kFlagTombstone) != 0 ||
                (!cell.is_tombstone() && cell.value().size() > max_value_bytes)) {
                return Status::invalid_argument("encoded value exceeds Tree value limits");
            }
        }
        else if ((input.flags & ~kFlagTombstone) != 0 ||
                 ((input.flags & kFlagTombstone) == 0 && input.value.size() > max_value_bytes)) {
            return Status::invalid_argument("value exceeds Tree value limits");
        }
        const bool tombstone =
            input.encoded ? CellView(input.value).is_tombstone() : (input.flags & kFlagTombstone) != 0;
        const auto value_bytes = input.encoded ? CellView(input.value).value().size() : input.value.size();
        if (!primary_write_fits(config, input.key.size(), value_bytes, tombstone)) {
            return Status::invalid_argument("batch key/cell cannot fit a primary leaf or inner separator");
        }
    }
    return Status::Ok();
}

detail::ReservedVector<size_t> select_inputs(size_t count, const CoreWriteReader &read,
                                             const std::shared_ptr<detail::AllocationReservation> &credit)
{
    detail::ReservedVector<size_t> order(count, detail::ReservationAllocator<size_t>(credit));
    std::iota(order.begin(), order.end(), size_t{0});
    std::sort(order.begin(), order.end(), [&](size_t left, size_t right) {
        const auto compare = read(left).key.compare(read(right).key);
        return compare == 0 ? left < right : compare < 0;
    });
    size_t selected = 0;
    for (size_t index = 0; index < order.size(); ++index) {
        if (index + 1 == order.size() || read(order[index]).key != read(order[index + 1]).key) {
            order[selected++] = order[index];
        }
    }
    order.resize(selected);
    return order;
}

Status claim_auto_slot(ApplyFrontier &frontier, std::atomic<uint64_t> &automatic, uint64_t *out) noexcept
{
    auto previous = automatic.load(std::memory_order_acquire);
    for (;;) {
        const auto latest = std::max(previous, frontier.max_seen());
        if (latest == UINT64_MAX) {
            return Status::resource_exhausted();
        }
        const auto next   = latest + 1;
        auto       status = frontier.check_slot(next);
        if (!status.ok()) {
            return status;
        }
        if (automatic.compare_exchange_weak(previous, next, std::memory_order_acq_rel)) {
            *out = next;
            return Status::Ok();
        }
    }
}

Status write_inputs(SourceAdmission &sources, ApplyFrontier &frontier, uint64_t completed,
                    std::optional<uint64_t> supplied_slot, std::atomic<uint64_t> &automatic,
                    const detail::ReservedVector<size_t> &order, const CoreWriteReader &read,
                    const std::shared_ptr<detail::AllocationReservation> &credit, detail::BudgetClass category,
                    ApplyProgress *progress, SourceRotation *rotation)
{
    WriteFootprint          footprint{.entries = order.size()};
    detail::AllocationBound payload;
    for (const auto index : order) {
        const auto input = read(index);
        footprint.key_bytes += input.key.size();
        payload.add(payload_capacity(input, credit));
        footprint.max_key_size = std::max(footprint.max_key_size, input.key.size());
    }
    const auto bytes = payload.value();
    if (!bytes) {
        return Status::resource_exhausted("batch payload allocation bound overflow");
    }
    footprint.payload_bytes = *bytes;
    SourceAdmission::Writer writer;
    Status                  status;
    for (;;) {
        const auto observed = progress != nullptr ? progress->sequence() : 0;
        bool       paused   = false;
        status              = sources.try_admit(completed, footprint, &writer, category, &paused);
        if (status.ok()) {
            break;
        }
        if (!paused && rotation != nullptr && category == detail::BudgetClass::kAdmittedWriter &&
            status.code() == Code::kUnavailable && !detail::FlushRuntime::on_worker_thread()) {
            const auto source = sources.catalog()->active->id();
            rotation->wait(rotation->request());
            if (sources.catalog()->active->id() != source) {
                continue;
            }
        }
        if (!paused || progress == nullptr || detail::FlushRuntime::on_worker_thread()) {
            return status;
        }
        status = progress->wait(observed);
        if (!status.ok()) {
            return status;
        }
    }
    detail::ReservedVector<SourceAdmission::Writer::Prepared> prepared{
        detail::ReservationAllocator<SourceAdmission::Writer::Prepared>(credit)};
    prepared.reserve(order.size());
    for (const auto index : order) {
        auto input   = stage(read(index), supplied_slot.value_or(0), writer.allocation_budget());
        input.charge = {};
        prepared.emplace_back();
        status = writer.prepare_write(input.key, input.flags, std::move(input.value), &prepared.back());
        if (!status.ok()) {
            return status;
        }
    }
    uint64_t slot = supplied_slot.value_or(0);
    if (!supplied_slot) {
        status = claim_auto_slot(frontier, automatic, &slot);
        if (!status.ok()) {
            return status;
        }
    }
    // The source permit covers every prepared write through completion. All
    // payloads, node towers and maximum-size COW descriptors already exist.
    for (auto &write : prepared) {
        writer.commit_write(std::move(write), slot);
    }
    status = frontier.complete(slot);
    assert(status.ok());
    return status;
}
} // namespace

Status apply_source_batch(const Config &config, SourceAdmission &sources, ApplyFrontier &frontier,
                          TreeOperationLifetime &lifetime, size_t max_value_bytes,
                          std::optional<uint64_t> supplied_slot, std::atomic<uint64_t> &automatic, size_t count,
                          const CoreWriteReader &read, ApplyProgress *progress, SourceRotation *rotation)
{
    auto status = supplied_slot ? frontier.check_slot(*supplied_slot) : Status::Ok();
    if (!status.ok()) {
        return status;
    }
    if (count == 0) {
        uint64_t slot = supplied_slot.value_or(0);
        if (!supplied_slot) {
            status = claim_auto_slot(frontier, automatic, &slot);
        }
        return status.ok() ? frontier.complete(slot) : status;
    }
    const auto  catalog = sources.catalog();
    const auto &options = catalog->active_budget->options();
    status              = validate_inputs(config, options, max_value_bytes, supplied_slot.value_or(0), count, read);
    if (!status.ok()) {
        return status;
    }
    const auto bytes     = detail::shared_allocation_bound<detail::AllocationReservation>() +
                           TreeOperationLifetime::ticket_allocation_bound() +
                           (count * (sizeof(size_t) + sizeof(SourceAdmission::Writer::Prepared)));
    const auto completed = frontier.contiguous().load(std::memory_order_acquire);
    const bool fills_gap = supplied_slot && *supplied_slot > completed && *supplied_slot <= frontier.max_seen();
    const auto category  = fills_gap ? detail::BudgetClass::kGapRepair : detail::BudgetClass::kAdmittedWriter;
    auto       reserved  = detail::ResourceBudget::try_reserve(options.account, category, bytes);
    if (!reserved) {
        return Status::unavailable("batch ingress credit unavailable");
    }
    const auto                                     credit = detail::AllocationReservation::create(std::move(*reserved));
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    status = lifetime.acquire(credit, &ticket);
    if (!status.ok()) {
        return status;
    }
    auto order = select_inputs(count, read, credit);
    if (supplied_slot && *supplied_slot <= catalog->floor) {
        return frontier.complete(*supplied_slot);
    }
    return write_inputs(sources, frontier, completed, supplied_slot, automatic, order, read, credit, category, progress,
                        rotation);
}
} // namespace crowdb::tree
