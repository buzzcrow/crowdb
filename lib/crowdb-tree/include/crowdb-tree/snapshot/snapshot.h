// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Immutable snapshot interface. Runtime views retain independent mapping and backing owners.

#pragma once

#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/maptable/page.h"
#include "crowdb-tree/maptable/page_types.h"
#include "crowdb-tree/slice.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace crowdb::tree
{

struct engine_diff
{
    std::string key;

    enum Kind : uint8_t { kOnlyLeft, kOnlyRight, kSlotDiffers, kValueDiffers } kind;
};

class Snapshot
{
  public:
    Snapshot(uint64_t at_slot, std::vector<leaf_entry> sorted_with_tombstones)
        : at_slot_(at_slot),
          entries_(std::move(sorted_with_tombstones))
    {
    }

    virtual ~Snapshot() = default;

    [[nodiscard]] uint64_t at_slot() const
    {
        return at_slot_;
    }

    [[nodiscard]] virtual const std::vector<leaf_entry> &entries() const
    {
        return entries_;
    }

    [[nodiscard]] virtual size_t size() const
    {
        return entries().size();
    }

    [[nodiscard]] virtual int find(Slice key) const
    {
        const auto &e  = entries();
        size_t      lo = 0;
        size_t      hi = e.size();
        while (lo < hi) {
            size_t mid = lo + ((hi - lo) / 2);
            int    c   = Slice(e[mid].key).compare(key);
            if (c == 0) {
                return static_cast<int>(mid);
            }
            if (c < 0) {
                lo = mid + 1;
            }
            else {
                hi = mid;
            }
        }
        return -1;
    }

    [[nodiscard]] virtual bool get(Slice key, uint64_t *out_slot, std::string *out_value) const
    {
        int i = find(key);
        if (i < 0) {
            return false;
        }
        CellView v{Slice(entries()[i].cell)};
        if (v.is_tombstone()) {
            return false;
        }
        if (out_slot != nullptr) {
            *out_slot = v.slot();
        }
        if (out_value != nullptr) {
            *out_value = v.value().to_string();
        }
        return true;
    }

    [[nodiscard]] std::vector<engine_diff> compare(const Snapshot &other) const
    {
        std::vector<engine_diff> diffs;
        size_t                   i = 0;
        size_t                   j = 0;
        const auto              &a = entries();
        const auto              &b = other.entries();
        while (i < a.size() && j < b.size()) {
            int c = Slice(a[i].key).compare(Slice(b[j].key));
            if (c < 0) {
                diffs.push_back({.key = a[i].key, .kind = engine_diff::kOnlyLeft});
                ++i;
            }
            else if (c > 0) {
                diffs.push_back({.key = b[j].key, .kind = engine_diff::kOnlyRight});
                ++j;
            }
            else {
                CellView va{Slice(a[i].cell)};
                CellView vb{Slice(b[j].cell)};
                if (va.slot() != vb.slot()) {
                    diffs.push_back({.key = a[i].key, .kind = engine_diff::kSlotDiffers});
                }
                else if (va.raw() != vb.raw()) {
                    diffs.push_back({.key = a[i].key, .kind = engine_diff::kValueDiffers});
                }
                ++i;
                ++j;
            }
        }
        for (; i < a.size(); ++i) {
            diffs.push_back({.key = a[i].key, .kind = engine_diff::kOnlyLeft});
        }
        for (; j < b.size(); ++j) {
            diffs.push_back({.key = b[j].key, .kind = engine_diff::kOnlyRight});
        }
        return diffs;
    }

  protected:
    uint64_t                        at_slot_;
    mutable std::vector<leaf_entry> entries_;
};

} // namespace crowdb::tree