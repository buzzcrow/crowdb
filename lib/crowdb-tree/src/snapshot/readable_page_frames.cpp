// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/readable_page_frames.h"

#include "btree/allocation_bound.h"

#include <stdexcept>

namespace crowdb::tree
{
struct ReadablePageFrames::Segment
{
    static constexpr bool observe_retirement = true;

    bool retire_step() noexcept
    {
        for (size_t count = 0; count < 64 && !frames.empty(); ++count) {
            frames.erase(frames.begin());
        }
        return frames.empty();
    }

    detail::BudgetMemoryResource                  resource;
    std::pmr::map<uint64_t, DurableCapturedFrame> frames{&resource};
    size_t                                        leaves       = 0;
    size_t                                        inners       = 0;
    size_t                                        overflows    = 0;
    uint64_t                                      capacity     = 0;
    bool                                          counts_known = true;

    void count(page_type type, bool add, uint32_t bytes)
    {
        auto *value = &overflows;
        if (type == page_type::kLeafBase) {
            value = &leaves;
        }
        else if (type == page_type::kInnerBase) {
            value = &inners;
        }
        if (add) {
            capacity += bytes;
        }
        else {
            capacity -= bytes;
        }
        if (value != nullptr) {
            if (add) {
                ++*value;
            }
            else {
                --*value;
            }
        }
    }
};

ReadablePageFrames::ReadablePageFrames(std::shared_ptr<detail::AllocationReservation> metadata)
    : segments_(&resource_),
      metadata_(std::move(metadata))
{
}

ReadablePageFrames::~ReadablePageFrames() = default;

ReadablePageFrames::ReadablePageFrames(const ReadablePageFrames &source) : ReadablePageFrames(source.metadata_)
{
    inherit(source);
}

ReadablePageFrames &ReadablePageFrames::operator=(const ReadablePageFrames &source)
{
    if (this != &source) {
        inherit(source);
    }
    return *this;
}

void ReadablePageFrames::budget(std::shared_ptr<detail::AllocationReservation> metadata)
{
    metadata_ = std::move(metadata);
}

void ReadablePageFrames::inherit(const ReadablePageFrames &source)
{
    detail::BudgetMemoryResource::Scope scope(resource_, metadata_);
    segments_.clear();
    segments_.insert(source.segments_.begin(), source.segments_.end());
    size_ = source.size_;
}

bool ReadablePageFrames::same_segment(const ReadablePageFrames &other, uint64_t segment) const noexcept
{
    auto a = segments_.find(segment);
    auto b = other.segments_.find(segment);
    return a != segments_.end() && b != other.segments_.end() && a->second == b->second;
}

ReadablePageFrames::Segment &ReadablePageFrames::mutable_segment(uint64_t segment)
{
    detail::BudgetMemoryResource::Scope scope(resource_, metadata_);
    auto [position, inserted] = segments_.try_emplace(segment);
    if (inserted || !position->second.unique()) {
        auto next = detail::reserved_shared<Segment>(metadata_);
        if (!inserted) {
            detail::BudgetMemoryResource::Scope entries(next->resource, metadata_);
            next->frames.insert(position->second->frames.begin(), position->second->frames.end());
            next->leaves       = position->second->leaves;
            next->inners       = position->second->inners;
            next->overflows    = position->second->overflows;
            next->capacity     = position->second->capacity;
            next->counts_known = position->second->counts_known;
        }
        position->second = std::move(next);
    }
    return *position->second;
}

ReadablePageFrames::value_type &ReadablePageFrames::entry(uint64_t pid)
{
    if (!contains(pid)) {
        throw std::out_of_range("durable mapping PID is absent");
    }
    auto &segment        = mutable_segment(pid / kMappingSegmentSize);
    segment.counts_known = false;
    return *segment.frames.find(pid);
}

const ReadablePageFrames::value_type &ReadablePageFrames::entry(uint64_t pid) const
{
    if (!contains(pid)) {
        throw std::out_of_range("durable mapping PID is absent");
    }
    return *segments_.at(pid / kMappingSegmentSize)->frames.find(pid);
}

bool ReadablePageFrames::contains(uint64_t pid) const
{
    const auto segment = segments_.find(pid / kMappingSegmentSize);
    return segment != segments_.end() && segment->second->frames.contains(pid);
}

uint64_t ReadablePageFrames::lower(uint64_t pid) const
{
    auto segment = segments_.lower_bound(pid / kMappingSegmentSize);
    if (segment == segments_.end()) {
        return UINT64_MAX;
    }
    auto frame = segment->second->frames.lower_bound(pid);
    if (frame != segment->second->frames.end()) {
        return frame->first;
    }
    ++segment;
    return segment == segments_.end() ? UINT64_MAX : segment->second->frames.begin()->first;
}

uint64_t ReadablePageFrames::next(uint64_t pid) const
{
    return pid == UINT64_MAX ? UINT64_MAX : lower(pid + 1);
}

std::pair<ReadablePageFrames::iterator, bool> ReadablePageFrames::emplace(uint64_t pid, DurableCapturedFrame frame)
{
    if (contains(pid)) {
        return {find(pid), false};
    }
    auto                               &segment = mutable_segment(pid / kMappingSegmentSize);
    detail::BudgetMemoryResource::Scope scope(segment.resource, metadata_);
    segment.count(frame.type, true, frame.frame_bytes);
    segment.frames.emplace(pid, std::move(frame));
    ++size_;
    return {find(pid), true};
}

std::pair<ReadablePageFrames::iterator, bool> ReadablePageFrames::insert_or_assign(uint64_t             pid,
                                                                                   DurableCapturedFrame frame)
{
    auto                               &segment = mutable_segment(pid / kMappingSegmentSize);
    detail::BudgetMemoryResource::Scope scope(segment.resource, metadata_);
    auto                                old = segment.frames.find(pid);
    if (old != segment.frames.end()) {
        segment.count(old->second.type, false, old->second.frame_bytes);
    }
    segment.count(frame.type, true, frame.frame_bytes);
    const auto result = segment.frames.insert_or_assign(pid, std::move(frame));
    size_ += static_cast<size_t>(result.second);
    return {find(pid), result.second};
}

size_t ReadablePageFrames::erase(uint64_t pid)
{
    if (!contains(pid)) {
        return 0;
    }
    auto &segment = mutable_segment(pid / kMappingSegmentSize);
    segment.count(segment.frames.at(pid).type, false, segment.frames.at(pid).frame_bytes);
    segment.frames.erase(pid);
    --size_;
    if (segment.frames.empty()) {
        segments_.erase(pid / kMappingSegmentSize);
    }
    return 1;
}

size_t ReadablePageFrames::segment_allocation_bound() noexcept
{
    // PMR nodes carry an allocation lease in addition to map linkage.
    return detail::shared_allocation_bound<Segment>() +
           (kMappingSegmentSize * (sizeof(value_type) + (8 * sizeof(void *)) + sizeof(AllocationLease)));
}

bool ReadablePageFrames::retire_step() noexcept
{
    for (size_t count = 0; count < 64 && !segments_.empty(); ++count) {
        segments_.erase(segments_.begin());
    }
    if (segments_.empty()) {
        size_ = 0;
    }
    return segments_.empty();
}

TreeSummary ReadablePageFrames::segment_shape(uint64_t segment) const
{
    const auto &value = *segments_.at(segment);
    TreeSummary result;
    if (value.counts_known) {
        result.reachable_leaf_pages          = value.leaves;
        result.reachable_inner_pages         = value.inners;
        result.reachable_overflow_pages      = value.overflows;
        result.reachable_page_capacity_bytes = value.capacity;
        return result;
    }
    for (const auto &[pid, frame] : value.frames) {
        (void)pid;
        result.reachable_leaf_pages += static_cast<uint64_t>(frame.type == page_type::kLeafBase);
        result.reachable_inner_pages += static_cast<uint64_t>(frame.type == page_type::kInnerBase);
        result.reachable_overflow_pages += static_cast<uint64_t>(frame.type == page_type::kOverflowFrame);
        result.reachable_page_capacity_bytes += frame.frame_bytes;
    }
    return result;
}

std::pair<size_t, size_t> ReadablePageFrames::primary_counts(uint64_t segment) const
{
    const auto result = segment_shape(segment);
    return {result.reachable_leaf_pages, result.reachable_inner_pages};
}

void ReadablePageFrames::refresh_counts()
{
    for (auto &[id, segment] : segments_) {
        if (!segment->counts_known && segment.unique()) {
            const auto part       = segment_shape(id);
            segment->leaves       = part.reachable_leaf_pages;
            segment->inners       = part.reachable_inner_pages;
            segment->overflows    = part.reachable_overflow_pages;
            segment->capacity     = part.reachable_page_capacity_bytes;
            segment->counts_known = true;
        }
    }
}

TreeSummary ReadablePageFrames::shape() const
{
    TreeSummary result;
    for (const auto &[id, segment] : segments_) {
        (void)segment;
        const auto part = segment_shape(id);
        result.reachable_leaf_pages += part.reachable_leaf_pages;
        result.reachable_inner_pages += part.reachable_inner_pages;
        result.reachable_overflow_pages += part.reachable_overflow_pages;
        result.reachable_page_capacity_bytes += part.reachable_page_capacity_bytes;
    }
    return result;
}
} // namespace crowdb::tree
