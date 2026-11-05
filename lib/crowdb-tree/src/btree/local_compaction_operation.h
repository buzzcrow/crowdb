// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/allocation_reservation.h"
#include "btree/inline_function.h"
#include "crowdb-tree/btree/diagnostics.h"
#include "runtime/tree_operation_lifetime.h"
#include "stdexec_adapter.h"

namespace crowdb::tree
{
class TreeCore;
class ApplyDispatcher;

struct LocalCompactionOptions
{
    double free_threshold = 0.70;
    // At most one eligible source block is selected per pass. Zero disables
    // relocation; the first source block may exceed a nonzero byte budget.
    uint64_t max_relocation_bytes = 1;
};

class LocalCompactionOperation
{
    struct State;

  public:
    static Status create(TreeCore &core, LocalCompactionOptions options, std::shared_ptr<ApplyDispatcher> cpu,
                         std::shared_ptr<detail::AllocationReservation> credit,
                         std::shared_ptr<TreeOperationLifetime::Ticket> ticket,
                         std::shared_ptr<LocalCompactionOperation>     *out);
    [[nodiscard]] static std::optional<size_t> allocation_bound() noexcept;
    [[nodiscard]] detail::CallbackSender       sender() noexcept;
    [[nodiscard]] const MergeGcStats          &result() const noexcept;
    void                                       request_stop() noexcept;
    // Install before connecting the one-shot sender. Storage is prepaid in State.
    void observe_completion(detail::InlineFunction<void(const MergeGcStats &), 32> observer) noexcept;

  private:
    explicit LocalCompactionOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
