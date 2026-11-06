// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"

namespace crowdb::tree
{
// Bounded completion tracking for one Tree generation. Admission checks the
// slot distance before touching a source; completing an admitted batch needs
// no allocation. A NoOp completes only its own slot. Replays below the current
// frontier are harmless and cannot overwrite a newer ring generation.
class ApplyFrontier
{
  public:
    static constexpr size_t kMaxWindow = 65536;

    static Status                              create(uint64_t initial, size_t window,
                                                      const std::shared_ptr<detail::AllocationReservation> &metadata,
                                                      std::shared_ptr<ApplyFrontier>                       *out);
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t window) noexcept;
    [[nodiscard]] Status                       check_slot(uint64_t slot) const noexcept;
    [[nodiscard]] Status                       complete(uint64_t slot) noexcept;

    // Recovery or an explicitly authorized skip of old gaps only. Ordinary
    // writes and NoOps use complete().
    void force_advance(uint64_t slot) noexcept;
    // Only after generation admission and all accepted operations have drained.
    void reset_quiescent(uint64_t slot) noexcept;

    [[nodiscard]] const std::atomic<uint64_t> &contiguous() const noexcept
    {
        return contiguous_;
    }

    [[nodiscard]] uint64_t max_seen() const noexcept
    {
        return max_seen_.load(std::memory_order_acquire);
    }

  private:
    ApplyFrontier(uint64_t initial, size_t window, const std::shared_ptr<detail::AllocationReservation> &metadata);
    void advance() noexcept;

    detail::ReservedVector<std::atomic<uint64_t>> completed_;
    std::atomic<uint64_t>                         contiguous_;
    std::atomic<uint64_t>                         max_seen_;
};
} // namespace crowdb::tree
