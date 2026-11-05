// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/local/extent_ledger.h"
#include "snapshot/captured_write.h"
#include "snapshot/commit_anchor.h"

#include <array>

namespace crowdb::tree
{
struct CheckpointMetadata
{
    explicit CheckpointMetadata(const std::shared_ptr<detail::AllocationReservation> &budget)
        : extents(detail::ReservationAllocator<ProtectedExtent>(budget))
    {
    }

    bool retire_step() noexcept
    {
        for (size_t i = 0; i < 64 && !extents.empty(); ++i) {
            extents.pop_back();
        }
        return extents.empty();
    }

    detail::ReservedVector<ProtectedExtent> extents;
};

struct ProtectedCheckpoint
{
    explicit ProtectedCheckpoint(const std::shared_ptr<detail::AllocationReservation> &budget)
        : metadata(detail::reserved_shared<CheckpointMetadata>(budget, budget))
    {
    }

    bool retire_step() noexcept
    {
        return true;
    }

    [[nodiscard]] static size_t allocation_bound() noexcept;

    snapshot::CommitAnchor                      anchor;
    std::shared_ptr<const CapturedDurablePages> pages;
    std::shared_ptr<CheckpointMetadata>         metadata;
};

class LocalCheckpointOperation;

// One registry per local allocation domain, retained for the domain lifetime.
// Both valid anchors pin their complete extent sets. An uncertain anchor write
// additionally pins its candidate and prevents another commit until recovery
// reconstructs a new registry from durable storage.
class LocalCheckpointRegistry
{
  public:
    static Status create(std::shared_ptr<ExtentLedger> ledger, std::shared_ptr<detail::AllocationReservation> metadata,
                         std::array<std::shared_ptr<const ProtectedCheckpoint>, 2> recovered,
                         std::shared_ptr<LocalCheckpointRegistry>                 *out);

    [[nodiscard]] uint64_t durable_coverage() const noexcept
    {
        return coverage_.load(std::memory_order_acquire);
    }

    [[nodiscard]] uint64_t snapshot_sequence() const noexcept
    {
        return sequence_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool needs_recovery() const noexcept
    {
        return uncertain_.load(std::memory_order_acquire);
    }

  private:
    friend class LocalCheckpointOperation;
    LocalCheckpointRegistry() = default;
    Status begin(uint64_t coverage, uint64_t generation, uint64_t *sequence) noexcept;
    void   touched(std::shared_ptr<const ProtectedCheckpoint> candidate) noexcept;
    void   finish(std::shared_ptr<const ProtectedCheckpoint> candidate, bool committed) noexcept;

    std::shared_ptr<ExtentLedger>                             ledger_;
    std::array<std::shared_ptr<const ProtectedCheckpoint>, 2> anchors_;
    std::shared_ptr<const ProtectedCheckpoint>                candidate_;
    std::atomic<bool>                                         busy_{false};
    std::atomic<bool>                                         uncertain_{false};
    std::atomic<uint64_t>                                     coverage_{0};
    std::atomic<uint64_t>                                     sequence_{0};
    uint64_t                                                  generation_ = 0;
};

struct LocalCheckpointLimits
{
    size_t max_segments = 1U << 16;
};

struct LocalCheckpointResult
{
    Status                                     status;
    std::shared_ptr<const ProtectedCheckpoint> checkpoint;
};

// Writes the existing segment/directory/anchor format through one aligned
// metadata window. The page barrier precedes the anchor, and durable coverage
// advances only after the anchor barrier. Cancellation after touching the
// anchor completes that barrier and reports the actual commit outcome.
class LocalCheckpointOperation
{
    struct State;

  public:
    static Status create(std::shared_ptr<const CapturedDurablePages> pages,
                         std::shared_ptr<LocalCheckpointRegistry> registry, std::shared_ptr<AsyncPageStore> store,
                         std::shared_ptr<detail::FlushRuntime>       runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, LocalCheckpointLimits limits,
                         PageLoadBudget budget, std::shared_ptr<LocalCheckpointOperation> *out);
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t segments, uint32_t iu) noexcept;
    [[nodiscard]] detail::CallbackSender       sender() noexcept;
    [[nodiscard]] const LocalCheckpointResult &result() const noexcept;
    void                                       request_stop() noexcept;

  private:
    explicit LocalCheckpointOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
