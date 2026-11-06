// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/overflow_backing.h"
#include "btree/page_load.h"
#include "crowdb-tree/snapshot/native_frame.h"
#include "stdexec_adapter.h"

namespace crowdb::tree
{
struct NativeFrameReadResult
{
    Status                         status;
    std::optional<NativeFrameView> frame;
};

// Reads one captured physical overflow frame without assembling its value.
// The result owns both charged frame bytes and that exact extent protection.
class NativeFrameRead
{
    struct State;

  public:
    static Status create(uint64_t pid, OverflowBackingPage page, std::shared_ptr<AsyncPageStore> store,
                         std::shared_ptr<detail::FlushRuntime>       runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadLimits limits, PageLoadBudget budget,
                         std::shared_ptr<NativeFrameRead> *out);
    static std::optional<size_t>               allocation_bound(PageLoadLimits limits) noexcept;
    detail::CallbackSender                     sender() noexcept;
    [[nodiscard]] const NativeFrameReadResult &result() const noexcept;
    void                                       request_stop() noexcept;

  private:
    explicit NativeFrameRead(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete callback, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
