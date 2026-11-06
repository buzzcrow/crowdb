// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "allocation_reservation.h"
#include "replay_controls.h"
#include "stdexec_adapter.h"
#include "tree_core_batch.h"

namespace crowdb::tree
{
class TreeCore;
class AdmittedBatch;

// Preparation retains paid bytes without preventing Tree close. arm() is the
// last fallible boundary before proposal and pins exactly that generation.
// After start(), dropping the caller handle never cancels an accepted apply.
class TreeApplyOperation
{
    struct State;

  public:
    ~TreeApplyOperation();
    TreeApplyOperation(const TreeApplyOperation &)             = delete;
    TreeApplyOperation  &operator=(const TreeApplyOperation &) = delete;
    [[nodiscard]] Status arm();
    void                 start(uint64_t slot, detail::CallbackComplete complete, void *context) noexcept;
    [[nodiscard]] const std::shared_ptr<detail::AllocationReservation> &allocation_budget() const noexcept;

  private:
    friend class TreeCore;
    static constexpr size_t kReplayCallerControlBytes = 4096;
    static Status           bind_replay(
        std::shared_ptr<TreeCore> core, uint64_t slot, size_t count, CoreWriteReader read,
        detail::InlineFunction<std::shared_ptr<const void>(const std::shared_ptr<detail::AllocationReservation> &)>
               retain,
        size_t retained_metadata_bytes, std::shared_ptr<TreeApplyOperation> *out);
    static std::optional<size_t> allocation_bound(size_t retained_metadata_bytes) noexcept;
    static Status bind_prepaid(std::shared_ptr<TreeCore> core, std::shared_ptr<AdmittedBatch> batch,
                               detail::ResourceBudget::Credit credit, std::shared_ptr<TreeApplyOperation> *out);

    explicit TreeApplyOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
