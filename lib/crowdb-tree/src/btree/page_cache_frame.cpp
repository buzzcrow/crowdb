// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_cache_frame.h"

#include "allocation_bound.h"

#include <cassert>

namespace crowdb::tree
{
namespace
{
void change(TreeResourceMetrics &metrics, TreeResourceMetric metric, size_t amount, bool add)
{
    const bool valid = add ? metrics.add(metric, amount) : metrics.remove(metric, amount);
    assert(valid);
    (void)valid;
}
} // namespace

PageCacheFrame::PageCacheFrame(std::shared_ptr<TreeResourceMetrics> metrics) : metrics_(std::move(metrics))
{
}

PageCacheFrame::~PageCacheFrame()
{
    leaf.reset();
    inner.reset();
    if (active_) {
        change(*metrics_, TreeResourceMetric::ResidentFrames, 1, false);
        change(*metrics_, TreeResourceMetric::CacheBytes, bytes_, false);
    }
}

void PageCacheFrame::activate(size_t bytes, std::shared_ptr<const LeafBase> leaf,
                              std::shared_ptr<const InnerPageState> inner) noexcept
{
    this->leaf  = std::move(leaf);
    this->inner = std::move(inner);
    bytes_      = bytes;
    active_     = true;
    change(*metrics_, TreeResourceMetric::ResidentFrames, 1, true);
    change(*metrics_, TreeResourceMetric::CacheBytes, bytes, true);
}

PageCacheFrame::Pin::Pin(std::shared_ptr<PageCacheFrame> frame) : frame_(std::move(frame))
{
    if (frame_->pins_.fetch_add(1, std::memory_order_acq_rel) == 0) {
        change(*frame_->metrics_, TreeResourceMetric::CachePinnedBytes, frame_->bytes_, true);
    }
}

PageCacheFrame::Pin::~Pin()
{
    if (frame_->pins_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        change(*frame_->metrics_, TreeResourceMetric::CachePinnedBytes, frame_->bytes_, false);
    }
}

size_t PageCacheFrame::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<PageCacheFrame>();
}

size_t PageCacheFrame::pin_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<Pin>();
}
} // namespace crowdb::tree
