// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/slice.h"

#include <memory>
#include <string>
#include <vector>

namespace crowdb::tree
{
struct scan_entry
{
    std::string key;
    uint64_t    slot;
    std::string value;
    bool        tombstone = false;
};

struct get_result
{
    bool        found = false;
    uint64_t    slot  = 0;
    std::string value;
};

// Move-only read result. The independent owner keeps immutable cell or frame
// bytes and their allocation charge alive across callbacks, generation changes,
// and Tree close. Overflow results own their assembled value through the same
// interface. No thread-bound epoch guard crosses the callback boundary.
class GetView
{
  public:
    GetView()                               = default;
    GetView(GetView &&) noexcept            = default;
    GetView &operator=(GetView &&) noexcept = default;
    GetView(const GetView &)                = delete;
    GetView &operator=(const GetView &)     = delete;

    [[nodiscard]] static GetView from_owner(uint64_t slot, Slice value, std::shared_ptr<const void> owner)
    {
        GetView result;
        result.found_ = static_cast<bool>(owner);
        result.slot_  = slot;
        result.value_ = value;
        result.owner_ = std::move(owner);
        return result;
    }

    [[nodiscard]] bool found() const
    {
        return found_;
    }

    [[nodiscard]] uint64_t slot() const
    {
        return slot_;
    }

    // Valid while this result retains its owner.
    [[nodiscard]] Slice value() const
    {
        return value_;
    }

    // Compatibility diagnostic: address of the independently owned bytes.
    // It is not the beginning of a physical frame and also covers overflow.
    [[nodiscard]] const uint8_t *frame_base() const
    {
        return value_.bytes();
    }

  private:
    std::shared_ptr<const void> owner_;
    bool                        found_ = false;
    uint64_t                    slot_  = 0;
    Slice                       value_;
};
} // namespace crowdb::tree
