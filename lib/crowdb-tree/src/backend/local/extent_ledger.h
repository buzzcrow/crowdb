// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_reservation.h"
#include "btree/inline_function.h"
#include "crowdb-tree/status.h"

namespace crowdb::tree
{
class BackingMetricExtent;

struct ExtentLedgerLimits
{
    uint64_t backend_id     = 0;
    uint64_t generation     = 1;
    uint64_t region_begin   = 8192;
    uint64_t file_size      = 8192;
    uint64_t address_limit  = UINT64_MAX;
    uint32_t iu_size        = 4096;
    size_t   max_ranges     = 1024 * 1024;
    size_t   queue_capacity = 64;
};

struct ProtectedExtent
{
    uint64_t backend_id = 0;
    uint64_t generation = 0;
    uint64_t identity   = 0;
    uint64_t address    = 0;
    uint64_t length     = 0;
    // Share this owner through in-flight I/O, backing, snapshots and readers.
    // Its final release, rather than a checkpoint's current live map, permits reuse.
    std::shared_ptr<const void>        owner;
    std::weak_ptr<BackingMetricExtent> physical_metrics;
};

struct ExtentLedgerResult
{
    Status          status;
    ProtectedExtent extent;
    uint64_t        occupied_bytes = 0;
};

struct ExtentLedgerStats
{
    uint64_t address_high_water   = 0;
    uint64_t protected_bytes      = 0;
    uint64_t protected_ranges     = 0;
    uint64_t reused_allocations   = 0;
    uint64_t appended_allocations = 0;
};

// A shared byte allocation domain. Recovery first protects every retained
// anchor reference, then activates allocation. Short metadata commands use
// bounded MPSC admission; callbacks may run inline and must not block.
class ExtentLedger
{
    struct State;

  public:
    using Complete = detail::InlineFunction<void(ExtentLedgerResult)>;
    static Status create(ExtentLedgerLimits limits, std::shared_ptr<detail::AllocationReservation> metadata,
                         std::shared_ptr<ExtentLedger> *out);
    [[nodiscard]] static size_t creation_metadata_bound() noexcept;
    [[nodiscard]] static size_t reservation_metadata_bound() noexcept;
    // Submission failure does not invoke complete. Accepted commands invoke it once.
    Status recover(uint64_t address, uint64_t length, std::shared_ptr<detail::AllocationReservation> metadata,
                   Complete complete);
    Status activate(std::shared_ptr<detail::AllocationReservation> metadata, Complete complete);
    Status reserve(uint64_t length, std::shared_ptr<detail::AllocationReservation> metadata, Complete complete);
    Status retain(uint64_t address, uint64_t length, std::shared_ptr<detail::AllocationReservation> metadata,
                  Complete complete);
    // Exclusively protect an already free byte range during physical deletion.
    // Never borrows a data owner or extends the allocation frontier. Retain
    // rejects this temporary lease so it cannot become a readable backing.
    Status claim_free(uint64_t address, uint64_t length, std::shared_ptr<detail::AllocationReservation> metadata,
                      Complete complete);
    // Freeze allocation into one existing byte interval while compaction moves
    // its live references elsewhere. Existing data owners remain readable.
    // The returned owner is an allocation exclusion, never readable backing.
    // occupied_bytes counts protected bytes at admission. One exclusion per
    // ledger and at most 4096 range visits keep metadata work bounded.
    Status exclude_allocations(uint64_t address, uint64_t length,
                               std::shared_ptr<detail::AllocationReservation> metadata, Complete complete);
    [[nodiscard]] ExtentLedgerStats         stats() const noexcept;
    [[nodiscard]] const ExtentLedgerLimits &limits() const noexcept;
    void                                    close() noexcept;

  private:
    explicit ExtentLedger(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
