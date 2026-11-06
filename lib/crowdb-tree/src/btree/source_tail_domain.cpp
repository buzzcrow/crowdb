// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "source_tail.h"

namespace crowdb::tree
{
struct SourceTailDomain::Registration : EpochManager::Deferred
{
    std::shared_ptr<Registration> owner;
    std::shared_ptr<SourceTail>   tail;
};

SourceTailDomain::SourceTailDomain(std::shared_ptr<detail::AllocationReservation> metadata)
    : metadata_(std::move(metadata)),
      epoch_(metadata_)
{
}

SourceTailDomain::~SourceTailDomain()
{
    close();
}

std::optional<size_t> SourceTailDomain::allocation_bound(size_t threads) noexcept
{
    return detail::AllocationBound(detail::shared_allocation_bound<SourceTailDomain>())
        .add(EpochManager::infrastructure_allocation_bound())
        .add_product(threads, EpochManager::participant_allocation_bytes())
        .value();
}

size_t SourceTailDomain::registration_bound() noexcept
{
    return detail::shared_allocation_bound<Registration>() + detail::shared_allocation_bound<Barrier>();
}

SourceTailDomain::Observation SourceTailDomain::observe()
{
    Observation result;
    result.domain = shared_from_this();
    result.guard  = epoch_.enter();
    if (auto *entry = registration_.load(std::memory_order_acquire)) {
        result.tail = entry->tail;
    }
    return result;
}
Status SourceTailDomain::arm(const std::shared_ptr<SourceTail>                    &tail,
                             const std::shared_ptr<detail::AllocationReservation> &metadata,
                             std::shared_ptr<Barrier>                             *out)
try {
    if (!tail || !metadata || !metadata_ || !metadata_->retirement_queue() || out == nullptr || *out ||
        closed_.load(std::memory_order_acquire)) {
        return Status::invalid_argument("tail registration requires a live source and owned output");
    }
    auto entry     = detail::reserved_shared<Registration>(metadata);
    auto barrier   = detail::reserved_shared<Barrier>(metadata);
    entry->tail    = tail;
    entry->destroy = [](EpochManager::Deferred *node) noexcept {
        auto owner = std::move(static_cast<Registration *>(node)->owner);
    };
    barrier->destroy = [](EpochManager::Deferred *node) noexcept {
        auto &signal = *static_cast<Barrier *>(node);
        auto  owner  = std::move(signal.owner);
        signal.ready.store(true, std::memory_order_release);
        signal.ready.notify_all();
    };
    Registration *expected = nullptr;
    entry->owner           = entry;
    if (!registration_.compare_exchange_strong(expected, entry.get(), std::memory_order_acq_rel)) {
        entry->owner.reset();
        return Status::unavailable("source already has a prepared tail");
    }
    barrier->registration = std::move(entry);
    barrier->owner        = barrier;
    epoch_.defer(barrier.get());
    *out = std::move(barrier);
    if (closed_.load(std::memory_order_acquire)) {
        tail->expire();
        detach(*out);
        return Status::unavailable("source closed during tail registration");
    }
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("tail registration credit exhausted");
}

void SourceTailDomain::retire(Registration *entry) noexcept
{
    if (entry != nullptr) {
        epoch_.defer(entry);
    }
}

bool SourceTailDomain::detach(const std::shared_ptr<Barrier> &subscription) noexcept
{
    if (!subscription || !subscription->registration) {
        return false;
    }
    auto *entry = subscription->registration.get();
    if (registration_.compare_exchange_strong(entry, nullptr, std::memory_order_acq_rel)) {
        retire(entry);
        return true;
    }
    return false;
}

void SourceTailDomain::close() noexcept
{
    closed_.store(true, std::memory_order_release);
    if (auto *entry = registration_.exchange(nullptr, std::memory_order_acq_rel)) {
        entry->tail->expire();
        retire(entry);
    }
}

void SourceTailDomain::Barrier::wait() const noexcept
{
    while (!ready.load(std::memory_order_acquire)) {
        ready.wait(false, std::memory_order_acquire);
    }
}
} // namespace crowdb::tree
