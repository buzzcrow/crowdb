// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_transport.h"
#include "crowdb-tree/backend/page_purpose.h"

#include <algorithm>
#include <map>

namespace crowdb::tree::detail
{

inline PagePurpose chunk_purpose(ChunkId id)
{
    switch (id.high >> 56U) {
    case 2:
        return PagePurpose::kBtreePage;
    case 3:
        return PagePurpose::kPageIndex;
    default:
        return PagePurpose::kInvalid;
    }
}

inline bool valid_page_purpose(PagePurpose purpose)
{
    return purpose == PagePurpose::kBtreePage || purpose == PagePurpose::kPageIndex;
}

// Single-writer staging state, matching the page store's existing write
// sequencer. Persisted pack references recover these boundaries from chunk IDs.
class PagePurposeRanges
{
  public:
    void clear()
    {
        boundaries_.clear();
    }

    [[nodiscard]] PagePurpose at(uint64_t offset) const
    {
        auto next = boundaries_.upper_bound(offset);
        return next == boundaries_.begin() ? PagePurpose::kBtreePage : std::prev(next)->second;
    }

    void assign(uint64_t offset, uint64_t length, PagePurpose purpose)
    {
        if (length == 0) {
            return;
        }
        const auto end   = offset + length;
        const auto after = at(end);
        boundaries_.erase(boundaries_.lower_bound(offset), boundaries_.lower_bound(end));
        boundaries_[offset] = purpose;
        boundaries_[end]    = after;
    }

    [[nodiscard]] size_t span(uint64_t offset, size_t maximum) const
    {
        const auto purpose = at(offset);
        for (auto next = boundaries_.upper_bound(offset); next != boundaries_.end(); ++next) {
            if (next->second != purpose) {
                return static_cast<size_t>(std::min<uint64_t>(maximum, next->first - offset));
            }
        }
        return maximum;
    }

  private:
    std::map<uint64_t, PagePurpose> boundaries_;
};

} // namespace crowdb::tree::detail
