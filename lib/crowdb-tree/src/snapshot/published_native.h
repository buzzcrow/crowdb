// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/snapshot/native_frame.h"
#include "snapshot/native_layout.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
// Host-thread cursor over a fixed immutable generation. Each next() owns a
// separate charged output window; idle cursors retain no I/O reservation.
class PublishedNativeCursor
{
    struct State;

  public:
    static Status                         create(const std::shared_ptr<TreeCore> &core, NativeLayoutLimits limits,
                                                 std::shared_ptr<PublishedNativeCursor> *out);
    Status                                next(std::optional<NativeFrameView> *out, bool *complete);
    Status                                rewind();
    [[nodiscard]] const NativeStreamInfo &info() const noexcept;
    [[nodiscard]] const std::shared_ptr<detail::AllocationReservation> &metadata() const noexcept;

  private:
    explicit PublishedNativeCursor(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
