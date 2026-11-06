// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "crowdb-tree/memtable/memtable.h"

namespace crowdb::tree
{
struct WriteFootprint
{
    size_t entries       = 0;
    size_t key_bytes     = 0;
    size_t payload_bytes = 0;
    // Zero uses key_bytes as a conservative largest-key bound. Batch callers
    // supply this explicitly when total key bytes exceed one key's limit.
    size_t max_key_size = 0;
};

struct SourceWriteOptions
{
    std::shared_ptr<detail::ResourceBudget::Account> account;
    size_t                                           max_entries       = 4096;
    size_t                                           max_key_bytes     = 1024 * 1024;
    size_t                                           max_gap_entries   = 64;
    size_t                                           max_gap_key_bytes = 128 * 1024;
    size_t                                           max_key_size      = 256 * 1024;
};

// Transferable resource admission. It owns no source, writer entrance or epoch
// guard, so consensus may retain it while a source is captured or replaced.
// A successful bind consumes the permit exactly once; an unavailable source
// leaves it intact for the operation's bounded asynchronous continuation.
class SourceWriteReservation
{
  public:
    SourceWriteReservation()                                              = default;
    SourceWriteReservation(SourceWriteReservation &&) noexcept            = default;
    SourceWriteReservation &operator=(SourceWriteReservation &&) noexcept = default;
    SourceWriteReservation(const SourceWriteReservation &)                = delete;
    SourceWriteReservation &operator=(const SourceWriteReservation &)     = delete;

    static Status create(const SourceWriteOptions &options, WriteFootprint footprint, detail::BudgetClass category,
                         SourceWriteReservation *out);

    static Status bind_prepaid(const SourceWriteOptions &options, WriteFootprint footprint,
                               detail::BudgetClass category, detail::ResourceBudget::Credit credit,
                               SourceWriteReservation *out);

    [[nodiscard]] bool valid() const noexcept
    {
        return credit_ != nullptr;
    }

    [[nodiscard]] WriteFootprint footprint() const noexcept
    {
        return footprint_;
    }

    [[nodiscard]] detail::BudgetClass category() const noexcept
    {
        return category_;
    }

  private:
    friend class SourceWriteBudget;
    friend class SourceAdmission;
    SourceWriteOptions                             options_;
    WriteFootprint                                 footprint_;
    detail::BudgetClass                            category_ = detail::BudgetClass::kAdmittedWriter;
    std::shared_ptr<detail::AllocationReservation> credit_;
};

// Each source caps the number of admitted versions, including writers that
// have not applied yet. Every writer reserves its own worst-case COW history
// before acceptance, so another writer cannot consume its apply allowance.
class SourceWriteBudget
{
  public:
    explicit SourceWriteBudget(SourceWriteOptions options) : options_(std::move(options))
    {
    }

    Status admit(WriteFootprint footprint, detail::BudgetClass category,
                 std::shared_ptr<detail::AllocationReservation> *out);
    Status admit(WriteFootprint footprint, detail::BudgetClass category,
                 std::shared_ptr<detail::AllocationReservation> *out, bool *paused);
    Status bind(SourceWriteReservation *reservation, std::shared_ptr<detail::AllocationReservation> *out,
                std::optional<detail::BudgetClass> source_category, bool *paused);
    // Consumes only source counters; memory was reserved before proposal.
    Status bind(SourceWriteReservation *reservation, std::shared_ptr<detail::AllocationReservation> *out,
                std::optional<detail::BudgetClass> source_category = {});
    [[nodiscard]] static std::optional<size_t> allocation_bound(const SourceWriteOptions &options,
                                                                WriteFootprint            footprint) noexcept;
    void return_unused(WriteFootprint footprint, detail::BudgetClass category) noexcept;
    // Active-source admission counts include accepted writers, not just
    // materialized records. Source contention counters are not sampled here.
    [[nodiscard]] std::array<ResourceUsage, 4> resource_usage() const noexcept;

    // Stops new ordinary binds; previously admitted writers are already included
    // in the counters. Gap repair keeps its independent fixed allowance.
    using PauseComplete = void (*)(void *) noexcept;
    bool                         pause(PauseComplete complete, void *context) noexcept;
    bool                         resume() noexcept;
    [[nodiscard]] bool           paused() const noexcept;
    [[nodiscard]] WriteFootprint paused_bound() const noexcept;

    [[nodiscard]] bool full() const noexcept
    {
        return capacity_rejected_.load(std::memory_order_acquire) ||
               entries_.load(std::memory_order_acquire) >= options_.max_entries ||
               key_bytes_.load(std::memory_order_acquire) >= options_.max_key_bytes;
    }

    [[nodiscard]] const SourceWriteOptions &options() const noexcept
    {
        return options_;
    }

  private:
    bool                       enter_bind() noexcept;
    void                       leave_bind() noexcept;
    static constexpr size_t    kPaused = size_t{1} << ((sizeof(size_t) * 8) - 1);
    std::atomic<size_t>        binders_{0};
    std::atomic<PauseComplete> pause_complete_{nullptr};
    void                      *pause_context_ = nullptr;

    SourceWriteOptions options_;

    std::atomic<size_t> entries_{0};
    std::atomic<size_t> key_bytes_{0};
    std::atomic<size_t> gap_entries_{0};
    std::atomic<size_t> gap_key_bytes_{0};
    std::atomic<bool>   pause_waiter_{false};
    std::atomic<bool>   capacity_rejected_{false};
};
} // namespace crowdb::tree
