// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <folly/dynamic.h>

#include <charconv>
#include <cstdint>
#include <optional>

namespace crowdb::diskio
{

inline std::optional<uint64_t> decode_disk_id_word(const folly::dynamic &value)
{
    if (value.isInt()) {
        return value.asInt() < 0 ? std::nullopt : std::optional<uint64_t>(value.asInt());
    }
    if (!value.isString()) {
        return std::nullopt;
    }
    const auto &text   = value.asString();
    uint64_t    word   = 0;
    auto        result = std::from_chars(text.data(), text.data() + text.size(), word);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
        return std::nullopt;
    }
    return word;
}

} // namespace crowdb::diskio
