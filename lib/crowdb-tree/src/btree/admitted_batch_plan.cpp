// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "admitted_batch.h"
#include "allocation_bound.h"

#include <algorithm>
#include <array>
#include <numeric>

namespace crowdb::tree
{
Status AdmittedBatch::plan(const Config &config, const SourceWriteOptions &options, size_t max_value_bytes,
                           size_t count, const CoreWriteReader &read, Plan *out)
{
    // Stack scratch is bounded independently of untrusted proposal lengths.
    std::array<size_t, 4096> indices{};
    if (out == nullptr || !options.account || count > options.max_entries || count > indices.size()) {
        return Status::invalid_argument("proposal input count exceeds its bounded planning window");
    }
    detail::AllocationBound incoming;
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
        incoming.add(input.key.size());
        if ((input.flags & kFlagTombstone) == 0) {
            incoming.add(input.value.size());
        }
    }
    const auto input_bytes = incoming.value();
    if (!input_bytes) {
        return Status::invalid_argument("proposal input bound overflow");
    }
    auto order = std::span(indices).first(count);
    std::iota(order.begin(), order.end(), size_t{0});
    std::sort(order.begin(), order.end(), [&](size_t left, size_t right) {
        const auto compared = read(left).key.compare(read(right).key);
        return compared == 0 ? left < right : compared < 0;
    });
    WriteFootprint          footprint;
    detail::AllocationBound keys;
    detail::AllocationBound payload;
    for (size_t index = 0; index < count; ++index) {
        const auto input = read(order[index]);
        if (index + 1 != count && input.key == read(order[index + 1]).key) {
            continue;
        }
        ++footprint.entries;
        keys.add(input.key.size());
        footprint.max_key_size = std::max(footprint.max_key_size, input.key.size());
        const auto bytes       = kCellHeaderSize + ((input.flags & kFlagTombstone) == 0 ? input.value.size() : 0);
        if (bytes > buffer::kInlineCap) {
            payload.add(bytes);
        }
    }
    const auto key_bytes     = keys.value();
    const auto payload_bytes = payload.value();
    if (!key_bytes || !payload_bytes) {
        return Status::invalid_argument("proposal unique-cell bound overflow");
    }
    footprint.key_bytes     = *key_bytes;
    footprint.payload_bytes = *payload_bytes;
    const auto staging =
        detail::AllocationBound(detail::shared_allocation_bound<AdmittedBatch>())
            .add(detail::shared_allocation_bound<detail::AllocationReservation>())
            .add(*input_bytes)
            .add_product(count, sizeof(Cell) + sizeof(size_t) + sizeof(SourceAdmission::Writer::Prepared))
            .value();
    const auto source = SourceWriteBudget::allocation_bound(options, footprint);
    if (!staging || !source) {
        return Status::invalid_argument("proposal complete allocation bound overflow");
    }
    *out = {.incoming_bytes = *input_bytes, .staging_bytes = *staging, .source_bytes = *source, .footprint = footprint};
    return Status::Ok();
}

Status AdmittedBatch::bind_prepaid(const SourceWriteOptions &options, size_t count, const CoreWriteReader &read,
                                   detail::BudgetClass category, const Plan &plan,
                                   detail::ResourceBudget::Credit staging, detail::ResourceBudget::Credit source,
                                   std::shared_ptr<AdmittedBatch> *out)
{
    if (out == nullptr || *out || !staging.belongs_to(options.account, category) || staging.allocated() ||
        staging.amount() < plan.staging_bytes) {
        return Status::invalid_argument("proposal staging requires its complete unused grant");
    }
    SourceWriteReservation reservation;
    if (plan.footprint.entries != 0) {
        auto status =
            SourceWriteReservation::bind_prepaid(options, plan.footprint, category, std::move(source), &reservation);
        if (!status.ok()) {
            return status;
        }
    }
    auto credit = detail::AllocationReservation::create(std::move(staging));
    auto batch  = detail::reserved_construct<AdmittedBatch>(
        credit, [&](auto *storage) { ::new (static_cast<void *>(storage)) AdmittedBatch(credit); });
    auto status = batch->initialize(options, count, read, category, plan.incoming_bytes, std::move(reservation));
    if (!status.ok()) {
        return status;
    }
    credit->seal();
    *out = std::move(batch);
    return Status::Ok();
}
} // namespace crowdb::tree
