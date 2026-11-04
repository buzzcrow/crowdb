// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <map>
#include <span>
#include <utility>
#include <vector>

namespace crowdb::tree::detail
{

// Single-writer overlay. Fragments never cross a 64-KiB boundary, bounding
// temporary copies when a later write replaces part of an earlier fragment.
// Trailing zero bytes retain logical coverage without retaining padding buffers.
class ChunkPageStaging
{
  public:
    void reset(uint64_t size)
    {
        fragments_.clear();
        size_ = size;
    }

    void clear()
    {
        reset(0);
    }

    [[nodiscard]] size_t size() const
    {
        return static_cast<size_t>(size_);
    }

    void write(uint64_t offset, std::span<const uint8_t> bytes)
    {
        size_ = std::max(size_, offset + bytes.size());
        while (!bytes.empty()) {
            const auto length = std::min<uint64_t>(bytes.size(), kFragmentBytes - (offset % kFragmentBytes));
            assign(offset, bytes.first(length));
            offset += length;
            bytes = bytes.subspan(length);
        }
    }

    void overlay(uint64_t offset, std::span<uint8_t> bytes) const
    {
        auto next = fragments_.upper_bound(offset);
        if (next != fragments_.begin()) {
            --next;
        }
        const uint64_t end = offset + bytes.size();
        for (; next != fragments_.end() && next->first < end; ++next) {
            const uint64_t begin = std::max(offset, next->first);
            const uint64_t stop  = std::min(end, next->first + next->second.length);
            if (begin < stop) {
                auto target = bytes.subspan(begin - offset, stop - begin);
                std::fill(target.begin(), target.end(), 0);
                const uint64_t copied_end = std::min(stop, next->first + next->second.bytes.size());
                if (begin < copied_end) {
                    std::memcpy(target.data(), next->second.bytes.data() + begin - next->first, copied_end - begin);
                }
            }
        }
    }

    [[nodiscard]] bool covers(uint64_t offset, uint64_t length) const
    {
        const uint64_t end  = offset + length;
        auto           next = fragments_.upper_bound(offset);
        if (next != fragments_.begin()) {
            --next;
        }
        for (; next != fragments_.end() && offset < end; ++next) {
            if (next->first > offset) {
                return false;
            }
            offset = std::max(offset, next->first + next->second.length);
        }
        return offset >= end;
    }

  private:
    struct Fragment
    {
        uint64_t             length = 0;
        std::vector<uint8_t> bytes;

        static Fragment capture(std::span<const uint8_t> input)
        {
            const auto last     = std::find_if(input.rbegin(), input.rend(), [](uint8_t byte) { return byte != 0; });
            const auto kept     = input.size() - static_cast<size_t>(std::distance(input.rbegin(), last));
            const auto retained = input.first(kept);
            return {.length = input.size(), .bytes = std::vector<uint8_t>(retained.begin(), retained.end())};
        }

        [[nodiscard]] Fragment slice(uint64_t offset, uint64_t size) const
        {
            Fragment result;
            if (offset < bytes.size()) {
                result = capture(std::span(bytes).subspan(offset, std::min<uint64_t>(size, bytes.size() - offset)));
            }
            result.length = size;
            return result;
        }
    };

    void assign(uint64_t offset, std::span<const uint8_t> bytes)
    {
        const uint64_t end  = offset + bytes.size();
        auto           next = fragments_.upper_bound(offset);
        if (next != fragments_.begin()) {
            const auto previous = std::prev(next);
            if (previous->first + previous->second.length > offset) {
                next = previous;
            }
        }
        Fragment prefix;
        Fragment suffix;
        uint64_t prefix_offset = offset;
        while (next != fragments_.end() && next->first < end) {
            if (next->first < offset) {
                prefix_offset = next->first;
                prefix        = next->second.slice(0, offset - next->first);
            }
            if (next->first + next->second.length > end) {
                suffix = next->second.slice(end - next->first, next->first + next->second.length - end);
            }
            next = fragments_.erase(next);
        }
        if (prefix.length > 0) {
            fragments_.emplace(prefix_offset, std::move(prefix));
        }
        if (suffix.length > 0) {
            fragments_.emplace(end, std::move(suffix));
        }
        fragments_.emplace(offset, Fragment::capture(bytes));
    }

    static constexpr uint64_t    kFragmentBytes = 64U * 1024U;
    uint64_t                     size_          = 0;
    std::map<uint64_t, Fragment> fragments_;
};

} // namespace crowdb::tree::detail
