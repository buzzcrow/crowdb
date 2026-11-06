// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "logical_page.h"
#include "published_state.h"

namespace crowdb::tree
{
// The cache and every delivered pin share one frame account. Eviction drops
// the cache reference; physical frame gauges end with the final frame owner.
class PageCacheFrame
{
  public:
    explicit PageCacheFrame(std::shared_ptr<TreeResourceMetrics> metrics);
    ~PageCacheFrame();
    void activate(size_t bytes, std::shared_ptr<const LeafBase> leaf,
                  std::shared_ptr<const InnerPageState> inner) noexcept;

    class Pin
    {
      public:
        explicit Pin(std::shared_ptr<PageCacheFrame> frame);
        ~Pin();
        Pin(const Pin &)            = delete;
        Pin &operator=(const Pin &) = delete;

      private:
        std::shared_ptr<PageCacheFrame> frame_;
    };

    [[nodiscard]] static size_t           allocation_bound() noexcept;
    [[nodiscard]] static size_t           pin_allocation_bound() noexcept;
    std::shared_ptr<const LeafBase>       leaf;
    std::shared_ptr<const InnerPageState> inner;

  private:
    std::shared_ptr<TreeResourceMetrics> metrics_;
    size_t                               bytes_ = 0;
    std::atomic<size_t>                  pins_{0};
    bool                                 active_ = false;
};
} // namespace crowdb::tree
