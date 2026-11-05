// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/inline_function.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/page_store.h"
#include "runtime/tree_runtime_binding.h"

namespace crowdb::tree
{
// Dispatches blocking storage preparation and synchronous backends away from
// Tree CPU workers. Queue slots, operation controls and outstanding callbacks
// are bounded; an uncertain submission retains the caller's buffer owner.
class ScheduledPageStore final : public AsyncPageStore
{
    struct State;
    struct Job;

  public:
    static std::optional<size_t> allocation_bound(size_t operations) noexcept;
    static Status      create(PageStore *sync, AsyncPageStore *async, std::shared_ptr<const void> backend_owner,
                              std::shared_ptr<TreeRuntimeBinding>                   binding,
                              const std::shared_ptr<detail::AllocationReservation> &metadata,
                              std::shared_ptr<detail::FlushRuntime::Lane> lane, std::shared_ptr<AsyncPageStore> *out);
    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override;
    [[nodiscard]] PageStore *synchronous_store() const noexcept;
    [[nodiscard]] bool       uses_async_transport() const noexcept;
    [[nodiscard]] size_t     allocation_workers() const noexcept;
    uint64_t submit_read(PageAddr address, void *buffer, size_t length, AsyncCompletion complete) override;
    uint64_t submit_write(PageAddr address, const void *buffer, size_t length, AsyncCompletion complete) override;
    uint64_t submit_typed_write(PagePurpose purpose, PageAddr address, const void *buffer, size_t length,
                                AsyncCompletion complete) override;
    Status   submit_fsync(AsyncCompletion complete) override;
    void     cancel(uint64_t operation) override;
    // A finite, paid backend capability step uses the same admission and drain
    // boundary as file I/O; it never executes on the submitting CPU worker.
    Status submit_step(detail::InlineFunction<Status()> step, AsyncCompletion complete, size_t read_bytes = 0,
                       size_t write_bytes = 0);

  private:
    explicit ScheduledPageStore(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    Status submit(PageAddr address, void *read, const void *write, size_t length, PagePurpose purpose,
                  AsyncCompletion complete, detail::InlineFunction<Status()> step = {}, size_t read_bytes = 0,
                  size_t write_bytes = 0);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
