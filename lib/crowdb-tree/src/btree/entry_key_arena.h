// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"

#include <vector>

namespace crowdb::tree
{
// Shared immutable key block after its builder has exposed the first entry.
class EntryKeyArena
{
    using Allocator = detail::ReservationAllocator<char>;

  public:
    EntryKeyArena(size_t bytes, const std::shared_ptr<AllocationBudget> &metadata)
        : bytes_(bytes, '\0', Allocator(metadata))
    {
    }

    [[nodiscard]] size_t size() const noexcept
    {
        return bytes_.size();
    }

    [[nodiscard]] const char *data() const noexcept
    {
        return bytes_.data();
    }

    [[nodiscard]] char *data() noexcept
    {
        return bytes_.data();
    }

  private:
    std::vector<char, Allocator> bytes_;
};
} // namespace crowdb::tree
