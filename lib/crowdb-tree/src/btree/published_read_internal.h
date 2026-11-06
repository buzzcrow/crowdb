// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/published_read.h"
#include "btree/published_route.h"

namespace crowdb::tree::published_read_detail
{
struct LoadEvent
{
    std::atomic<bool>                     ready{false};
    Status                                status;
    std::shared_ptr<const LeafBase>       leaf;
    std::shared_ptr<const InnerPageState> inner;
    std::shared_ptr<const buffer>         value;
};

class ReadPump : public std::enable_shared_from_this<ReadPump>
{
  public:
    ReadPump(std::shared_ptr<const PublishedTreeState> captured, std::shared_ptr<PageLoader> transport,
             PublishedReadLimits bounds)
        : view(std::move(captured)),
          loader(std::move(transport)),
          limits(std::move(bounds))
    {
    }

    virtual ~ReadPump() = default;
    void start(detail::CallbackComplete complete, void *context) noexcept;
    void stop() noexcept;

  protected:
    virtual void                              begin()                        = 0;
    virtual bool                              advance()                      = 0;
    virtual void                              loaded(const LoadEvent &event) = 0;
    virtual Status                           &status()                       = 0;
    void                                      load_base(BaseRef base);
    void                                      load_inner(std::shared_ptr<const BackingRef> backing);
    void                                      load_value(const LogicalCell &cell);
    std::shared_ptr<const PublishedTreeState> view;
    std::shared_ptr<PageLoader>               loader;
    PublishedReadLimits                       limits;

  private:
    void                       wake() noexcept;
    void                       drain() noexcept;
    void                       finish(detail::CallbackSignal signal) noexcept;
    std::atomic<bool>          gate_{false};
    std::atomic<bool>          wake_{false};
    std::atomic<uint8_t>       lifecycle_{0};
    std::atomic<bool>          stopped_{false};
    bool                       begin_pending_ = false;
    detail::CallbackComplete   complete_      = nullptr;
    void                      *context_       = nullptr;
    std::shared_ptr<LoadEvent> event_;
};

Status validate_read(const std::shared_ptr<const PublishedTreeState> &view, const PublishedReadLimits &limits);
[[nodiscard]] std::optional<size_t> read_metadata_allocation_bound(const PublishedReadLimits &limits,
                                                                   size_t                     workers) noexcept;
LogicalCell                         owned_cell(std::shared_ptr<const CellVersion> owner);
void                                materialize_value(LogicalCell *cell, const std::shared_ptr<const buffer> &value);
} // namespace crowdb::tree::published_read_detail
