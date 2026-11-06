// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "source_write_budget.h"

#include "crowdb-tree/memtable/skip_list.h"

namespace crowdb::tree
{
std::array<ResourceUsage, 4> SourceWriteBudget::resource_usage() const noexcept
{
    return {
        {{.limit = options_.max_entries, .used = entries_.load(std::memory_order_relaxed)},
         {.limit = options_.max_key_bytes, .used = key_bytes_.load(std::memory_order_relaxed)},
         {.limit = options_.max_gap_entries, .used = gap_entries_.load(std::memory_order_relaxed)},
         {.limit = options_.max_gap_key_bytes, .used = gap_key_bytes_.load(std::memory_order_relaxed)}}
    };
}

namespace
{
bool reserve_count(std::atomic<size_t> &counter, size_t amount, size_t maximum)
{
    auto current = counter.load(std::memory_order_relaxed);
    while (current <= maximum && amount <= maximum - current) {
        if (counter.compare_exchange_weak(current, current + amount, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

std::optional<size_t> write_allocation_bound(WriteFootprint footprint, size_t max_versions)
{
    // Includes the old/new buffers during vector growth, an unpublished
    // insert descriptor losing to another writer, cell control block, node
    // tower and descriptor owners. Payload capacity is supplied separately.
    constexpr size_t history_entry = 3 * sizeof(std::shared_ptr<const CellVersion>);
    constexpr size_t descriptors   = 4096;
    if (max_versions > (SIZE_MAX - descriptors) / history_entry) {
        return {};
    }
    const auto per_entry = descriptors + (max_versions * history_entry);
    if (footprint.entries > SIZE_MAX / per_entry) {
        return {};
    }
    auto bytes = footprint.entries * per_entry;
    if (footprint.key_bytes > SIZE_MAX - bytes) {
        return {};
    }
    bytes += footprint.key_bytes;
    if (footprint.payload_bytes > SIZE_MAX - bytes) {
        return {};
    }
    return bytes + footprint.payload_bytes;
}
} // namespace

std::optional<size_t> SourceWriteBudget::allocation_bound(const SourceWriteOptions &options,
                                                          WriteFootprint            footprint) noexcept
{
    if (options.max_gap_entries > SIZE_MAX - options.max_entries) {
        return {};
    }
    return write_allocation_bound(footprint, options.max_entries + options.max_gap_entries);
}

Status SourceWriteReservation::create(const SourceWriteOptions &options, WriteFootprint footprint,
                                      detail::BudgetClass category, SourceWriteReservation *out)
{
    if (out == nullptr || out->valid() || !options.account || footprint.entries == 0 ||
        (category != detail::BudgetClass::kAdmittedWriter && category != detail::BudgetClass::kGapRepair)) {
        return Status::invalid_argument("writer requires a nonempty footprint and dedicated apply budget");
    }
    const auto largest_key = footprint.max_key_size == 0 ? footprint.key_bytes : footprint.max_key_size;
    if (largest_key > footprint.key_bytes || largest_key > options.max_key_size) {
        return Status::invalid_argument("writer key footprint exceeds the source key limit");
    }
    const bool gap = category == detail::BudgetClass::kGapRepair;
    if (footprint.entries > (gap ? options.max_gap_entries : options.max_entries) ||
        footprint.key_bytes > (gap ? options.max_gap_key_bytes : options.max_key_bytes)) {
        return Status::resource_exhausted("writer footprint exceeds one source progress window");
    }
    const auto bytes = SourceWriteBudget::allocation_bound(options, footprint);
    if (!bytes) {
        return Status::resource_exhausted("writer allocation bound overflow");
    }
    if (*bytes > detail::ResourceBudget::capacity(options.account, category)) {
        return Status::resource_exhausted("writer allocation exceeds its budget partition");
    }
    auto credit = detail::ResourceBudget::try_reserve(options.account, category, *bytes);
    if (!credit) {
        if (!gap && detail::ResourceBudget::memory_exhausted(options.account)) {
            return Status::resource_exhausted("actual Tree memory exceeds 150% of the configured maximum");
        }
        return Status::unavailable("writer apply credit unavailable");
    }
    try {
        auto owned      = detail::AllocationReservation::create(std::move(*credit));
        out->options_   = options;
        out->footprint_ = footprint;
        out->category_  = category;
        out->credit_    = std::move(owned);
        return Status::Ok();
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted("writer reservation allocation failed");
    }
}

Status SourceWriteBudget::bind(SourceWriteReservation *reservation, std::shared_ptr<detail::AllocationReservation> *out,
                               std::optional<detail::BudgetClass> source_category)
{
    return bind(reservation, out, source_category, nullptr);
}

Status SourceWriteBudget::bind(SourceWriteReservation *reservation, std::shared_ptr<detail::AllocationReservation> *out,
                               std::optional<detail::BudgetClass> source_category, bool *paused)
{
    if (paused != nullptr) {
        *paused = false;
    }
    if (reservation == nullptr || !reservation->valid() || out == nullptr || *out) {
        return Status::invalid_argument("writer binding requires an unused reservation");
    }
    const auto &options = reservation->options_;
    if (options.account != options_.account || options.max_entries != options_.max_entries ||
        options.max_key_bytes != options_.max_key_bytes || options.max_gap_entries != options_.max_gap_entries ||
        options.max_gap_key_bytes != options_.max_gap_key_bytes || options.max_key_size != options_.max_key_size) {
        return Status::invalid_argument("writer reservation belongs to different source limits");
    }
    const auto category = source_category.value_or(reservation->category_);
    if (category != detail::BudgetClass::kAdmittedWriter && category != detail::BudgetClass::kGapRepair) {
        return Status::invalid_argument("source binding requires ordinary or gap capacity");
    }
    const auto footprint = reservation->footprint_;
    const bool gap       = category == detail::BudgetClass::kGapRepair;
    if (footprint.entries > (gap ? options_.max_gap_entries : options_.max_entries) ||
        footprint.key_bytes > (gap ? options_.max_gap_key_bytes : options_.max_key_bytes)) {
        return Status::resource_exhausted("writer footprint cannot fit the selected source partition");
    }
    if (!gap && !enter_bind()) {
        if (paused != nullptr) {
            *paused = true;
        }
        return Status::unavailable("ordinary source admission is paused for rotation");
    }

    struct Leave
    {
        SourceWriteBudget &owner;
        bool               ordinary;

        ~Leave()
        {
            if (ordinary) {
                owner.leave_bind();
            }
        }
    } leave{.owner = *this, .ordinary = !gap};

    auto &entries = gap ? gap_entries_ : entries_;
    auto &keys    = gap ? gap_key_bytes_ : key_bytes_;
    if (!reserve_count(entries, footprint.entries, gap ? options_.max_gap_entries : options_.max_entries)) {
        if (!gap) {
            capacity_rejected_.store(true, std::memory_order_release);
        }
        return Status::unavailable("source version admission limit reached");
    }
    if (!reserve_count(keys, footprint.key_bytes, gap ? options_.max_gap_key_bytes : options_.max_key_bytes)) {
        entries.fetch_sub(footprint.entries, std::memory_order_acq_rel);
        if (!gap) {
            capacity_rejected_.store(true, std::memory_order_release);
        }
        return Status::unavailable("source key admission limit reached");
    }
    *out = std::move(reservation->credit_);
    return Status::Ok();
}

Status SourceWriteBudget::admit(WriteFootprint footprint, detail::BudgetClass category,
                                std::shared_ptr<detail::AllocationReservation> *out)
{
    return admit(footprint, category, out, nullptr);
}

Status SourceWriteBudget::admit(WriteFootprint footprint, detail::BudgetClass category,
                                std::shared_ptr<detail::AllocationReservation> *out, bool *paused)
{
    if (paused != nullptr) {
        *paused = false;
    }
    if (out == nullptr || *out) {
        return Status::invalid_argument("writer output must be empty");
    }
    SourceWriteReservation reservation;
    auto                   status = SourceWriteReservation::create(options_, footprint, category, &reservation);
    return status.ok() ? bind(&reservation, out, {}, paused) : status;
}

void SourceWriteBudget::return_unused(WriteFootprint footprint, detail::BudgetClass category) noexcept
{
    const bool gap = category == detail::BudgetClass::kGapRepair;
    (gap ? gap_entries_ : entries_).fetch_sub(footprint.entries, std::memory_order_acq_rel);
    (gap ? gap_key_bytes_ : key_bytes_).fetch_sub(footprint.key_bytes, std::memory_order_acq_rel);
}
} // namespace crowdb::tree
