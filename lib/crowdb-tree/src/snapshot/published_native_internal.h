// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/published_native.h"

namespace crowdb::tree
{
struct PublishedNativeCursor::State
{
    ~State()
    {
        if (metadata) {
            metadata->seal();
        }
    }

    std::shared_ptr<PublishedSnapshotContext>      snapshot;
    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<SnapshotPageLoader>            loader;
    std::shared_ptr<CapturedFrameStream>           stream;
    std::shared_ptr<const NativeLayoutPlanner>     layout;
    OverflowBackingSet::Pages::const_iterator      old;
    CapturedFrameLimits                            limits;
    NativeStreamInfo                               info;
    size_t                                         read_bound = 0;
    uint64_t                                       emitted    = 0;
    bool                                           generated  = false;
    std::atomic<bool>                              busy{false};
    Status                                         failure;

    Status                       restart();
    static std::optional<size_t> control_bound(const CapturedFrameLimits &limits) noexcept;
    Status                       generated_frame(const SnapshotReadWindow &window, std::optional<NativeFrameView> *out);
    Status                       old_frame(const SnapshotReadWindow &window, std::optional<NativeFrameView> *out);
    Status                       next(std::optional<NativeFrameView> *out, bool *complete);
};
} // namespace crowdb::tree
