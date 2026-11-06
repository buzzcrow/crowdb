// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "btree/tree_metrics.h"

#include <algorithm>
#include <cassert>
#include <new>

namespace crowdb::tree
{
struct CheckpointPendingMetrics::CaptureRetirement
{
    explicit CaptureRetirement(std::pmr::memory_resource *resource, std::shared_ptr<const void> owner = {})
        : resource_owner(std::move(owner)),
          pages(resource)
    {
    }

    bool retire_step() noexcept
    {
        for (size_t i = 0; i < 64 && !pages.empty(); ++i) {
            pages.erase_first();
        }
        return pages.empty();
    }

    std::shared_ptr<const void> resource_owner;
    PendingPageTimes            pages;
};

CheckpointPendingMetrics::CheckpointPendingMetrics(std::pmr::memory_resource *resource)
    : pending_(resource),
      since_capture_(resource)
{
}

CheckpointPendingMetrics::PreparedDelta::PreparedDelta(CheckpointPendingMetrics &owner, size_t max_pages)
    : owner_(owner),
      edits_(owner.pending_.resource()),
      changed_at_ns_(tree_metrics_now_ns())
{
    edits_.reserve(max_pages);
}

CheckpointPendingMetrics::PreparedCapture::PreparedCapture(CheckpointPendingMetrics                      &owner,
                                                           std::shared_ptr<detail::AllocationReservation> metadata,
                                                           std::shared_ptr<const void> resource_owner)
    : owner_(owner),
      retired_(metadata && resource_owner
                   ? detail::reserved_shared<CaptureRetirement>(metadata, owner.pending_.resource(),
                                                                std::move(resource_owner))
                   : std::allocate_shared<CaptureRetirement>(
                         std::pmr::polymorphic_allocator<CaptureRetirement>{owner.pending_.resource()},
                         owner.pending_.resource()))
{
}

std::shared_ptr<const void> CheckpointPendingMetrics::PreparedCapture::commit(uint64_t token) noexcept
{
    assert(owner_.can_capture(token));
    retired_->pages.swap(owner_.since_capture_);
    owner_.mapping_since_capture_    = false;
    owner_.mapping_capture_since_ns_ = 0;
    owner_.token_                    = token;
    owner_.inflight_                 = true;
    return std::move(retired_);
}

CheckpointPendingMetrics::PreparedDelta::~PreparedDelta()
{
    if (!committed_) {
        for (const auto &edit : edits_) {
            if (edit.inserted_pending) {
                owner_.pending_.erase(edit.pid);
            }
            if (edit.inserted_since) {
                owner_.since_capture_.erase(edit.pid);
            }
        }
    }
}

void CheckpointPendingMetrics::PreparedDelta::change_page(uint64_t pid)
{
    if (edits_.size() == edits_.capacity()) {
        throw std::bad_alloc();
    }
    const bool inserted       = owner_.pending_.insert(pid, changed_at_ns_);
    bool       inserted_since = false;
    try {
        if (owner_.inflight_) {
            inserted_since = owner_.since_capture_.insert(pid, changed_at_ns_);
        }
    }
    catch (...) {
        if (inserted) {
            owner_.pending_.erase(pid);
        }
        throw;
    }
    edits_.push_back({.pid = pid, .inserted_pending = inserted, .inserted_since = inserted_since});
}

void CheckpointPendingMetrics::PreparedDelta::remove_page(uint64_t pid)
{
    if (edits_.size() == edits_.capacity()) {
        throw std::bad_alloc();
    }
    edits_.push_back({.pid = pid, .removed = true});
    change_mapping();
}

void CheckpointPendingMetrics::PreparedDelta::change_mapping() noexcept
{
    mapping_changed_ = true;
}

void CheckpointPendingMetrics::PreparedDelta::copy_to(TreeLogicalStats &stats) const noexcept
{
    owner_.copy_to(stats);
    for (const auto &edit : edits_) {
        if (edit.removed && owner_.pending_.contains(edit.pid)) {
            --stats.checkpoint_pending_pages;
        }
    }
    stats.checkpoint_mapping_pending = stats.checkpoint_mapping_pending || mapping_changed_;
}

void CheckpointPendingMetrics::PreparedDelta::commit() noexcept
{
    for (const auto &edit : edits_) {
        if (edit.removed) {
            owner_.pending_.erase(edit.pid);
            owner_.since_capture_.erase(edit.pid);
        }
    }
    if (mapping_changed_) {
        owner_.change_mapping();
    }
    committed_ = true;
}

void CheckpointPendingMetrics::change_page(uint64_t pid)
{
    PreparedDelta delta(*this, 1);
    delta.change_page(pid);
    delta.commit();
}

void CheckpointPendingMetrics::remove_page(uint64_t pid)
{
    pending_.erase(pid);
    since_capture_.erase(pid);
    change_mapping();
}

void CheckpointPendingMetrics::change_mapping() noexcept
{
    const auto now = tree_metrics_now_ns();
    if (!mapping_pending_) {
        mapping_since_ns_ = now;
    }
    if (inflight_ && !mapping_since_capture_) {
        mapping_capture_since_ns_ = now;
    }
    mapping_pending_       = true;
    mapping_since_capture_ = mapping_since_capture_ || inflight_;
}

bool CheckpointPendingMetrics::can_capture(uint64_t token) const noexcept
{
    return !inflight_ && token > token_;
}

bool CheckpointPendingMetrics::can_finish(uint64_t token) const noexcept
{
    return inflight_ && token == token_;
}

bool CheckpointPendingMetrics::capture(uint64_t token)
{
    if (!can_capture(token)) {
        return false;
    }
    PreparedCapture prepared(*this);
    (void)prepared.commit(token);
    return true;
}

bool CheckpointPendingMetrics::finish(uint64_t token, bool committed) noexcept
{
    if (!can_finish(token)) {
        return false;
    }
    if (committed) {
        pending_.swap(since_capture_);
        mapping_pending_  = mapping_since_capture_;
        mapping_since_ns_ = mapping_capture_since_ns_;
    }
    inflight_ = false;
    return true;
}

void CheckpointPendingMetrics::copy_to(TreeLogicalStats &stats) const noexcept
{
    stats.checkpoint_pending_pages   = pending_.size();
    stats.checkpoint_mapping_pending = mapping_pending_;
}

bool CheckpointPendingMetrics::contains(uint64_t pid) const
{
    return pending_.contains(pid);
}

bool CheckpointPendingMetrics::retire_step() noexcept
{
    size_t steps = 0;
    for (; steps < 64 && !pending_.empty(); ++steps) {
        pending_.erase_first();
    }
    for (; steps < 64 && !since_capture_.empty(); ++steps) {
        since_capture_.erase_first();
    }
    return pending_.empty() && since_capture_.empty();
}

uint64_t CheckpointPendingMetrics::oldest_since_ns() const noexcept
{
    const auto page_since = pending_.oldest_since_ns();
    if (!mapping_pending_) {
        return page_since;
    }
    return page_since == 0 ? mapping_since_ns_ : std::min(page_since, mapping_since_ns_);
}

} // namespace crowdb::tree
