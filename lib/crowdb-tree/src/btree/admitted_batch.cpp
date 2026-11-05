// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "admitted_batch.h"

#include "allocation_bound.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <numeric>

namespace crowdb::tree
{
namespace
{
Status validate(const Config &config, const SourceWriteOptions &options, size_t max_value_bytes, size_t count,
                const CoreWriteReader &read, size_t *bytes)
{
    if (!options.account || count > options.max_entries) {
        return Status::invalid_argument("proposal batch exceeds the configured entry limit");
    }
    detail::AllocationBound bound;
    for (size_t index = 0; index < count; ++index) {
        const auto input = read(index);
        if (input.encoded || (input.flags & ~kFlagTombstone) != 0 || !config.key_range.contains(input.key) ||
            input.key.size() > options.max_key_size || input.value.size() > SIZE_MAX - kCellHeaderSize ||
            ((input.flags & kFlagTombstone) == 0 && input.value.size() > max_value_bytes)) {
            return Status::invalid_argument("proposal contains an invalid key, value or encoding");
        }
        if (!primary_write_fits(config, input.key.size(), input.value.size(), (input.flags & kFlagTombstone) != 0)) {
            return Status::invalid_argument("proposal key/cell cannot fit a primary leaf or inner separator");
        }
        bound.add(input.key.size());
        if ((input.flags & kFlagTombstone) == 0) {
            bound.add(input.value.size());
        }
    }
    const auto size = bound.value();
    if (!size) {
        return Status::resource_exhausted("proposal input bound overflow");
    }
    *bytes = *size;
    return Status::Ok();
}

size_t encoded_capacity(size_t value_bytes, uint8_t flags)
{
    const auto size = (flags & kFlagTombstone) != 0 ? kCellHeaderSize : kCellHeaderSize + value_bytes;
    return size <= buffer::kInlineCap ? 0 : size;
}
} // namespace

AdmittedBatch::AdmittedBatch(std::shared_ptr<detail::AllocationReservation> metadata)
    : metadata_(std::move(metadata)),
      bytes_(decltype(bytes_)::allocator_type(metadata_)),
      cells_(detail::ReservationAllocator<Cell>(metadata_)),
      prepared_(detail::ReservationAllocator<SourceAdmission::Writer::Prepared>(metadata_))
{
}

Status AdmittedBatch::create(const Config &config, const SourceWriteOptions &options, size_t max_value_bytes,
                             size_t count, const CoreWriteReader &read, detail::BudgetClass category,
                             std::shared_ptr<AdmittedBatch> *out)
try {
    if (out == nullptr || *out ||
        (category != detail::BudgetClass::kAdmittedWriter && category != detail::BudgetClass::kGapRepair)) {
        return Status::invalid_argument("proposal admission requires an empty output and apply partition");
    }
    size_t incoming_bytes = 0;
    auto   status         = validate(config, options, max_value_bytes, count, read, &incoming_bytes);
    if (!status.ok()) {
        return status;
    }
    const auto bound =
        detail::AllocationBound(detail::shared_allocation_bound<AdmittedBatch>())
            .add(detail::shared_allocation_bound<detail::AllocationReservation>())
            .add(incoming_bytes)
            .add_product(count, sizeof(Cell) + sizeof(size_t) + sizeof(SourceAdmission::Writer::Prepared))
            .value();
    auto reserved = bound ? detail::ResourceBudget::try_reserve(options.account, category, *bound) : std::nullopt;
    if (!reserved) {
        return Status::unavailable("proposal staging credit unavailable");
    }
    auto credit = detail::AllocationReservation::create(std::move(*reserved));
    auto batch  = detail::reserved_construct<AdmittedBatch>(
        credit, [&](auto *storage) { ::new (static_cast<void *>(storage)) AdmittedBatch(credit); });
    status = batch->initialize(options, count, read, category, incoming_bytes);
    if (!status.ok()) {
        return status;
    }
    credit->seal();
    *out = std::move(batch);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("proposal admission allocation failed");
}

Status AdmittedBatch::initialize(const SourceWriteOptions &options, size_t count, const CoreWriteReader &read,
                                 detail::BudgetClass category, size_t incoming_bytes, SourceWriteReservation prepaid)
{
    detail::ReservedVector<size_t> order(count, detail::ReservationAllocator<size_t>(metadata_));
    std::iota(order.begin(), order.end(), size_t{0});
    std::sort(order.begin(), order.end(), [&](size_t left, size_t right) {
        const auto comparison = read(left).key.compare(read(right).key);
        return comparison == 0 ? left < right : comparison < 0;
    });
    WriteFootprint          footprint;
    detail::AllocationBound payload;
    size_t                  selected = 0;
    for (size_t index = 0; index < order.size(); ++index) {
        if (index + 1 != order.size() && read(order[index]).key == read(order[index + 1]).key) {
            continue;
        }
        const auto input  = read(order[index]);
        order[selected++] = order[index];
        footprint.key_bytes += input.key.size();
        footprint.max_key_size = std::max(footprint.max_key_size, input.key.size());
        payload.add(encoded_capacity(input.value.size(), input.flags));
    }
    order.resize(selected);
    footprint.entries   = selected;
    const auto capacity = payload.value();
    if (!capacity) {
        return Status::resource_exhausted("proposal payload bound overflow");
    }
    footprint.payload_bytes = *capacity;
    if (prepaid.valid()) {
        const auto paid = prepaid.footprint();
        if (paid.entries != footprint.entries || paid.key_bytes != footprint.key_bytes ||
            paid.payload_bytes != footprint.payload_bytes || paid.max_key_size != footprint.max_key_size) {
            return Status::invalid_argument("proposal inputs changed after resource planning");
        }
        source_ = std::move(prepaid);
    }
    else if (selected != 0) {
        auto status = SourceWriteReservation::create(options, footprint, category, &source_);
        if (!status.ok()) {
            return status;
        }
    }
    bytes_.resize(incoming_bytes);
    cells_.reserve(selected);
    prepared_.reserve(selected);
    size_t offset = 0;
    for (const auto index : order) {
        const auto input      = read(index);
        const auto value_size = (input.flags & kFlagTombstone) != 0 ? 0 : input.value.size();
        cells_.push_back({.key_offset   = offset,
                          .key_size     = input.key.size(),
                          .value_offset = offset + input.key.size(),
                          .value_size   = value_size,
                          .flags        = input.flags});
        if (!input.key.empty()) {
            std::memcpy(bytes_.data() + offset, input.key.data(), input.key.size());
        }
        offset += input.key.size();
        if (value_size != 0) {
            std::memcpy(bytes_.data() + offset, input.value.data(), value_size);
        }
        offset += value_size;
    }
    return Status::Ok();
}

Status AdmittedBatch::prepare_cells(SourceAdmission::Writer &writer)
{
    for (const auto &cell : cells_) {
        AllocationLease charge(writer.allocation_budget(), encoded_capacity(cell.value_size, cell.flags),
                               AllocationKind::kPayload);
        auto            value = buffer::alloc(cell.value_size, kCellHeaderSize);
        std::memset(value.data(), 0, kCellHeaderSize);
        value.data()[sizeof(uint64_t)] = cell.flags;
        if (cell.value_size != 0) {
            std::memcpy(value.data() + kCellHeaderSize, bytes_.data() + cell.value_offset, cell.value_size);
        }
        prepared_.emplace_back();
        charge          = {};
        const auto *key = cell.key_size == 0 ? nullptr : bytes_.data() + cell.key_offset;
        auto status = writer.prepare_write(Slice(key, cell.key_size), cell.flags, std::move(value), &prepared_.back());
        if (!status.ok()) {
            return status;
        }
    }
    return Status::Ok();
}

Status AdmittedBatch::try_commit(SourceAdmission &sources, ApplyFrontier &frontier, uint64_t slot)
try {
    if (consumed_) {
        return Status::invalid_argument("admitted batch has already been consumed");
    }
    auto status = frontier.check_slot(slot);
    if (!status.ok()) {
        return status;
    }
    if (cells_.empty() || slot <= sources.catalog()->floor) {
        consumed_ = true;
        source_   = {};
        return frontier.complete(slot);
    }
    const auto completed = frontier.contiguous().load(std::memory_order_acquire);
    // Higher missing slots use ordinary admission or wait. Only the next
    // contiguous slot may borrow the finite gap allowance; otherwise later
    // holes can prevent the frontier from ever advancing.
    const bool              gap       = slot > completed && slot - completed == 1 && slot <= frontier.max_seen();
    const auto              partition = gap ? detail::BudgetClass::kGapRepair : detail::BudgetClass::kAdmittedWriter;
    SourceAdmission::Writer writer;
    status = sources.try_admit_reserved(completed, &source_, &writer, partition);
    if (!status.ok()) {
        return status;
    }
    consumed_ = true;
    status    = prepare_cells(writer);
    if (!status.ok()) {
        return status;
    }
    for (auto &write : prepared_) {
        writer.commit_write(std::move(write), slot);
    }
    prepared_.clear();
    status = frontier.complete(slot);
    assert(status.ok());
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("prepaid batch preparation allocation failed");
}
} // namespace crowdb::tree
