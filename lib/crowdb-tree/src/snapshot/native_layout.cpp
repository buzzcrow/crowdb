// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/native_layout.h"

#include "btree/allocation_bound.h"
#include "crowdb-tree/maptable/compressor.h"

#include <algorithm>

namespace crowdb::tree
{
NativeLayoutPlanner::NativeLayoutPlanner(std::shared_ptr<const PublishedTreeState> view, NativeLayoutLimits limits,
                                         std::shared_ptr<detail::AllocationReservation> metadata, uint64_t backend_id,
                                         uint64_t incarnation)
    : view_(std::move(view)),
      limits_(limits),
      backend_id_(backend_id),
      incarnation_(incarnation),
      old_(metadata),
      primary_(std::less<>{}, detail::ReservationAllocator<uint64_t>(std::move(metadata)))
{
}

std::optional<size_t> NativeLayoutPlanner::allocation_bound(uint64_t next_page_id, NativeLayoutLimits limits) noexcept
{
    if (next_page_id > MappingView::kPageLimit || limits.max_frames == 0 ||
        limits.max_frames > MappingView::kPageLimit || limits.max_dependency_steps == 0) {
        return {};
    }
    // Primary and old-overflow PIDs are disjoint. The larger map-node bound
    // therefore also covers every primary set node without counting twice.
    return detail::AllocationBound(detail::shared_allocation_bound<NativeLayoutPlanner>())
        .add_product(std::min(next_page_id, limits.max_frames),
                     detail::shared_allocation_bound<OverflowBackingSet::Entry>())
        .value();
}

Status NativeLayoutPlanner::accept(CapturedFrameResult::Plan page)
{
    if (!view_ || page_ || limits_.max_frames == 0 || limits_.max_frames > MappingView::kPageLimit ||
        limits_.max_dependency_steps == 0) {
        return Status::invalid_argument("native layout has no bounded idle captured view");
    }
    if (page.page_id >= view_->next_page_id || primary_.contains(page.page_id) || old_.pages.contains(page.page_id)) {
        return Status::corruption("native primary PID is duplicate or outside the captured namespace");
    }
    const auto current = frame_count();
    if (current >= limits_.max_frames || page.summary.new_overflow_frames > limits_.max_frames - current - 1) {
        return Status::resource_exhausted("native frame count exceeds its bound");
    }
    primary_.insert(page.page_id);
    new_overflow_ += page.summary.new_overflow_frames;
    reference_ = 0;
    pid_       = kInvalidPageId;
    remaining_ = 0;
    page_      = std::move(page);
    return Status::Ok();
}

Status NativeLayoutPlanner::include(uint64_t pid, const OverflowBackingPage &page)
{
    if (pid >= view_->next_page_id || primary_.contains(pid) || view_->mapping.get(pid)) {
        return Status::corruption("native overflow PID aliases a primary or exceeds its namespace");
    }
    const auto found = old_.pages.find(pid);
    if (found != old_.pages.end()) {
        const auto &old = found->second;
        if (old.next != page.next || old.bytes != page.bytes || old.frame_bytes != page.frame_bytes ||
            old.logical_bytes != page.logical_bytes || old.backing->backend_id != page.backing->backend_id ||
            old.backing->generation != page.backing->generation || old.backing->address != page.backing->address ||
            old.backing->length != page.backing->length || old.backing->encoding != page.backing->encoding ||
            old.backing->storage != page.backing->storage ||
            !same_chunk_backing(old.backing->chunk, page.backing->chunk) ||
            old.backing->resident_frame.data() != page.backing->resident_frame.data() ||
            old.backing->resident_frame.size() != page.backing->resident_frame.size()) {
            return Status::corruption("one native PID names conflicting captured extents");
        }
    }
    else {
        if (frame_count() == limits_.max_frames) {
            return Status::resource_exhausted("native old overflow count exceeds its bound");
        }
        old_.pages.emplace(pid, page);
    }
    return Status::Ok();
}

Status NativeLayoutPlanner::step()
{
    if (!page_) {
        return Status::invalid_argument("native dependency step has no current page");
    }
    if (reference_ == page_->reused_overflow.size()) {
        page_.reset();
        return Status::Ok();
    }
    if (steps_ == limits_.max_dependency_steps) {
        return Status::resource_exhausted("native overflow dependency work exceeds its bound");
    }
    const auto &reference = page_->reused_overflow[reference_];
    if (!reference.backing) {
        return Status::unavailable("native overflow has no typed physical descriptors");
    }
    if (remaining_ == 0) {
        if (reference.head == kInvalidPageId || reference.bytes == 0) {
            return Status::corruption("native overflow reference has no head or declared length");
        }
        pid_       = reference.head;
        remaining_ = reference.bytes;
    }
    const auto found = reference.backing->pages.find(pid_);
    if (found == reference.backing->pages.end()) {
        return Status::corruption("native overflow is missing from its primary dependency catalog");
    }
    const auto &old = found->second;
    if (!old.backing || !old.backing->readable() || old.backing->encoding != 0 || old.bytes == 0 ||
        old.frame_bytes <= kFrameHeaderSize + kFrameTrailerSize || old.bytes > overflow_chunk_cap(old.frame_bytes) ||
        old.bytes > remaining_ || old.next == pid_ || ((old.next == kInvalidPageId) != (old.bytes == remaining_))) {
        return Status::corruption("native overflow descriptor disagrees with its logical chain");
    }
    if (old.backing->storage == BackingStorage::kResidentFrame) {
        if (old.backing->durability != BackingDurability::kResident ||
            old.backing->resident_frame.size() != old.frame_bytes || old.logical_bytes != old.frame_bytes) {
            return Status::corruption("native resident overflow has inconsistent physical geometry");
        }
    }
    else if (old.backing->backend_id != backend_id_ || old.backing->generation != incarnation_ ||
             old.logical_bytes < kDurableBlobHeader || old.logical_bytes > old.backing->length) {
        return Status::corruption("native durable overflow has inconsistent domain or blob geometry");
    }
    auto status = include(pid_, old);
    if (!status.ok()) {
        return status;
    }
    ++steps_;
    remaining_ -= old.bytes;
    pid_ = old.next;
    if (remaining_ == 0) {
        ++reference_;
    }
    return Status::Ok();
}

Status NativeLayoutPlanner::finish()
{
    if (!view_ || pending()) {
        return Status::invalid_argument("native layout is not complete");
    }
    if (view_->next_page_id > MappingView::kPageLimit ||
        new_overflow_ > MappingView::kPageLimit - view_->next_page_id) {
        return Status::resource_exhausted("native overflow namespace exhausted");
    }
    return Status::Ok();
}
} // namespace crowdb::tree
