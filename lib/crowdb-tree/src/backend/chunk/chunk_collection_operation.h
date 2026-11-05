// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_collector.h"
#include "chunk_protocol_operation.h"

namespace crowdb::tree
{
struct ChunkBackendState;

class ChunkCollectionOperation
{
    struct State;

  public:
    static Status create(std::shared_ptr<ChunkBackendState> backend, std::shared_ptr<AsyncPageStore> store,
                         std::shared_ptr<detail::FlushRuntime>       runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadBudget budget,
                         std::shared_ptr<ChunkCollectionOperation> *out);
    [[nodiscard]] static size_t                          allocation_bound() noexcept;
    [[nodiscard]] detail::CallbackSender                 sender() noexcept;
    [[nodiscard]] const detail::ChunkCollectionProgress &result() const noexcept;
    void                                                 request_stop() noexcept;

  private:
    explicit ChunkCollectionOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State>                  state_;
    std::shared_ptr<ChunkProtocolOperation> protocol_;
};
} // namespace crowdb::tree
