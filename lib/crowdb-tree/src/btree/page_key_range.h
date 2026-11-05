// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "owned_key.h"

#include <optional>

namespace crowdb::tree
{
// An absent bound is unbounded; an empty string is a real key boundary.
struct PageKeyRange
{
    std::optional<OwnedKey> lower = std::nullopt;
    std::optional<OwnedKey> upper = std::nullopt;

    [[nodiscard]] bool                 contains(Slice key) const;
    [[nodiscard]] bool                 empty() const;
    [[nodiscard]] bool                 overlaps(const PageKeyRange &other) const;
    [[nodiscard]] std::optional<Slice> lower_slice() const;
    [[nodiscard]] std::optional<Slice> upper_slice() const;
};
} // namespace crowdb::tree
