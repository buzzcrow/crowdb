// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/slice.h"
#include "crowdb-tree/status.h"

#include <optional>
#include <string>

namespace crowdb::tree
{

// Immutable half-open key policy. A missing endpoint is explicitly unbounded;
// present empty strings remain ordinary keys. Equal endpoints represent an
// empty range, while inverted endpoints are invalid.
class KeyRange
{
  public:
    static KeyRange unbounded()
    {
        return {};
    }

    static KeyRange bounded(std::optional<std::string> start, std::optional<std::string> end)
    {
        KeyRange range;
        range.bounded_ = true;
        range.start_   = std::move(start);
        range.end_     = std::move(end);
        return range;
    }

    [[nodiscard]] Status validate() const;
    [[nodiscard]] bool   contains(Slice key) const;
    [[nodiscard]] bool   before(Slice key) const;
    [[nodiscard]] bool   at_or_after_end(Slice key) const;

    [[nodiscard]] bool is_bounded() const
    {
        return bounded_;
    }

    [[nodiscard]] const std::optional<std::string> &start() const
    {
        return start_;
    }

    [[nodiscard]] const std::optional<std::string> &end() const
    {
        return end_;
    }

  private:
    bool                       bounded_ = false;
    std::optional<std::string> start_;
    std::optional<std::string> end_;
};

} // namespace crowdb::tree
