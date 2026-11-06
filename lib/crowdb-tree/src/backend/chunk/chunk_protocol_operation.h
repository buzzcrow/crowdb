// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/local/scheduled_page_store.h"
#include "btree/page_load.h"

namespace crowdb::tree
{
// A finite metadata protocol runs one blocking step per paid backend job.
// The protocol resolves cancellation/unknown results itself; the runner never
// drops an accepted mutation or destroys its context ahead of the callback.
class ChunkProtocolOperation
{
    struct State;

  public:
    using Step = detail::InlineFunction<Status(bool)>;
    using Done = detail::InlineFunction<bool()>;
    static Status create(std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadBudget budget,
                         std::shared_ptr<const void> owner, Step step, Done done,
                         std::shared_ptr<ChunkProtocolOperation> *out);
    [[nodiscard]] static size_t          allocation_bound() noexcept;
    [[nodiscard]] detail::CallbackSender sender() noexcept;
    void                                 request_stop() noexcept;

  private:
    explicit ChunkProtocolOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *, detail::CallbackComplete, void *);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
