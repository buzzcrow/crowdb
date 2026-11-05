// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <concepts>

namespace crowdb::tree::detail
{
template <class T> struct RetirementTraits
{
    static constexpr bool enabled = requires(T &value) {
        { value.retire_step() } noexcept -> std::same_as<bool>;
    };
    static constexpr bool observed = requires { T::observe_retirement; };

    static bool release(T &value) noexcept
    {
        if constexpr (enabled) {
            return value.retire_step();
        }
        return true;
    }
};
} // namespace crowdb::tree::detail
