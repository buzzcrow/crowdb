// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/captured_write.h"

namespace crowdb::tree
{
struct RecoveredStateLimits
{
    size_t max_pages            = 1024 * 1024;
    size_t max_depth            = 64;
    size_t max_children         = 4096;
    size_t max_leaf_entries     = 4096;
    size_t max_key_bytes        = 256 * 1024;
    size_t max_step_entries     = 128;
    size_t max_dependency_steps = 1024 * 1024;
};

struct RecoveredStateResult
{
    Status                                    status;
    std::shared_ptr<const PublishedTreeState> state;
    uint64_t                                  pages     = 0;
    uint64_t                                  loads     = 0;
    uint64_t                                  cpu_steps = 0;
};

// Validates a recovered immutable namespace before its one-time installation.
// Only a bounded decoded path and one leaf are retained. The resulting mapping
// owns cold proxies; overflow extents remain protected by each leaf's backing.
class RecoveredStateOperation
{
    struct State;

  public:
    static Status create(std::shared_ptr<const CapturedDurablePages> captured, std::shared_ptr<PageLoader> loader,
                         std::shared_ptr<detail::FlushRuntime>       runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, RecoveredStateLimits limits,
                         std::shared_ptr<detail::AllocationReservation> metadata,
                         std::shared_ptr<RecoveredStateOperation>      *out);
    static Status create_readable(std::shared_ptr<const ReadablePageCatalog> captured,
                                  std::shared_ptr<PageLoader> loader, std::shared_ptr<detail::FlushRuntime> runtime,
                                  std::shared_ptr<detail::FlushRuntime::Lane> lane, RecoveredStateLimits limits,
                                  std::shared_ptr<detail::AllocationReservation> metadata,
                                  std::shared_ptr<RecoveredStateOperation>      *out);
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t frames, size_t primary_pages,
                                                                size_t max_key_bytes) noexcept;
    [[nodiscard]] detail::CallbackSender       sender() noexcept;
    [[nodiscard]] const RecoveredStateResult  &result() const noexcept;
    void                                       request_stop() noexcept;

  private:
    explicit RecoveredStateOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
