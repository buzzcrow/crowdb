// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/maintenance_operation.h"

#include <span>

namespace crowdb::tree
{
namespace detail
{
struct MaintenanceOperation;
}

// One exact captured leaf and at most one bounded structural transaction.
// No source handoff, candidate scan or unrelated merge is performed.
class PageSplitOperation
{
  public:
    static Status               create(PublicationCoordinator &publisher, detail::FlushRuntime &runtime,
                                       std::shared_ptr<detail::FlushRuntime::Lane> lane,
                                       std::shared_ptr<const PublishedTreeState> view, uint64_t pid,
                                       PageMaintenanceOperationOptions options, std::shared_ptr<PageLoader> loader,
                                       std::shared_ptr<ApplyDispatcher> progress, std::shared_ptr<const void> resources,
                                       TreeWorkCause cause, std::shared_ptr<PageSplitOperation> *out);
    [[nodiscard]] static size_t control_allocation_bound() noexcept;
    [[nodiscard]] detail::CallbackSender                           sender() noexcept;
    void                                                           request_stop() noexcept;
    [[nodiscard]] uint64_t                                         commit_sequence() const noexcept;
    [[nodiscard]] uint64_t                                         l1_sequence() const noexcept;
    [[nodiscard]] std::span<const uint64_t>                        output_leaf_pids() const noexcept;
    [[nodiscard]] const std::shared_ptr<const PublishedTreeState> &captured_view() const noexcept;

  private:
    explicit PageSplitOperation(std::shared_ptr<detail::MaintenanceOperation> state,
                                std::shared_ptr<const void>                   resources)
        : state_(std::move(state)),
          resources_(std::move(resources))
    {
    }

    static void submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<detail::MaintenanceOperation> state_;
    std::shared_ptr<const void>                   resources_;
};
} // namespace crowdb::tree
