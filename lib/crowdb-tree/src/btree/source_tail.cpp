// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "source_tail.h"

#include "allocation_bound.h"
#include "source_write_budget.h"

#include <cassert>

namespace crowdb::tree
{
namespace
{
bool claim(std::atomic<size_t> &used, size_t value, size_t maximum) noexcept
{
    auto old = used.load(std::memory_order_relaxed);
    while (old <= maximum && value <= maximum - old) {
        if (used.compare_exchange_weak(old, old + value, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}
} // namespace

SourceTail::SourceTail(std::shared_ptr<detail::AllocationReservation> metadata, size_t records, size_t bytes)
    : metadata_(std::move(metadata)),
      table_(detail::reserved_shared<MemTable>(metadata_, 0, nullptr, metadata_)),
      max_records_(records),
      max_bytes_(bytes)
{
}

std::optional<size_t> SourceTail::allocation_bound(size_t records, size_t bytes) noexcept
{
    SourceWriteOptions options;
    options.max_entries     = records;
    options.max_gap_entries = 0;
    // No payload allocation occurs here. The byte cap includes copied keys
    // and retained values, conservatively treating all of it as possible keys.
    auto writes = SourceWriteBudget::allocation_bound(options, {.entries = records, .key_bytes = bytes});
    return detail::AllocationBound()
        .add(writes)
        .add(detail::shared_allocation_bound<SourceTail>())
        .add(detail::shared_allocation_bound<MemTable>())
        .add(EpochManager::infrastructure_allocation_bound())
        .add(Node::alloc_size(ConcurrentSkipList::kMaxHeight, 0))
        .add_product(64, EpochManager::participant_allocation_bytes())
        .value();
}

SourceTail::Write SourceTail::prepare(const std::shared_ptr<SourceTail> &owner,
                                      const MemTable::PreparedWrite     &parent) noexcept
{
    Write result;
    if (!owner) {
        return result;
    }
    auto state = owner->state_.load(std::memory_order_acquire);
    do {
        if ((state & (kStopped | kExpired)) != 0) {
            return result;
        }
    } while (!owner->state_.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel));
    result.owner     = owner;
    result.entered   = true;
    const auto bytes = detail::AllocationBound(parent.key().size()).add(parent.cell_owner()->cell.size()).value();
    if (!bytes || !claim(owner->records_, 1, owner->max_records_) || !claim(owner->bytes_, *bytes, owner->max_bytes_)) {
        owner->expire();
        return result;
    }
    try {
        // All allocations precede the parent CAS.
        // A failed observation expires only the split, never the parent write.
        result.prepared = owner->table_->prepare_reference(parent.key(), parent.cell_owner(), 0, owner->max_records_,
                                                           owner->metadata_);
    }
    catch (...) {
        owner->expire();
    }
    return result;
}

SourceTail::Write::Write(Write &&other) noexcept
    : owner(std::move(other.owner)),
      prepared(std::move(other.prepared)),
      entered(std::exchange(other.entered, false))
{
}

SourceTail::Write &SourceTail::Write::operator=(Write &&other) noexcept
{
    if (this != &other) {
        if (entered) {
            owner->leave();
        }
        owner    = std::move(other.owner);
        prepared = std::move(other.prepared);
        entered  = std::exchange(other.entered, false);
    }
    return *this;
}

SourceTail::Write::~Write()
{
    if (entered) {
        owner->leave();
    }
}

void SourceTail::Write::complete(bool changed, uint64_t slot) noexcept
{
    if (entered && !owner->expired() && changed) {
        owner->table_->commit_write(std::move(prepared), slot);
    }
    if (entered) {
        entered = false;
        owner->leave();
    }
}

void SourceTail::leave() noexcept
{
    state_.fetch_sub(1, std::memory_order_acq_rel);
}

bool SourceTail::freeze() noexcept
{
    size_t expected = 0;
    if (!state_.compare_exchange_strong(expected, kStopped, std::memory_order_acq_rel)) {
        return false;
    }
    table_->close();
    return true;
}

void SourceTail::expire() noexcept
{
    state_.fetch_or(kExpired, std::memory_order_acq_rel);
}

bool SourceTail::expired() const noexcept
{
    return (state_.load(std::memory_order_acquire) & kExpired) != 0;
}
} // namespace crowdb::tree
