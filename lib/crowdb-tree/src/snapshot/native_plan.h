// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/native_layout.h"

namespace crowdb::tree
{
// Runs the layout pass with one bounded dependency step per CPU continuation.
// The completed planner retains only charged physical descriptors.
class NativePlanOperation
{
    struct State;

  public:
    static Status create(std::shared_ptr<CapturedFrameStream> stream, std::shared_ptr<NativeLayoutPlanner> planner,
                         std::shared_ptr<detail::FlushRuntime>                 runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane>           lane,
                         const std::shared_ptr<detail::AllocationReservation> &metadata,
                         std::shared_ptr<NativePlanOperation>                 *out);
    static size_t allocation_bound() noexcept;
    detail::CallbackSender      sender() noexcept;
    [[nodiscard]] const Status &result() const noexcept;
    void                        request_stop() noexcept;

  private:
    explicit NativePlanOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
