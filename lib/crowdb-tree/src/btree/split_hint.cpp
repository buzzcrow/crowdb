// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "btree/logical_page.h"
#include "btree/tree_core.h"
#include "crowdb-tree/btree/tree.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
struct HintRange
{
    const KeyRange     &tree;
    const PageKeyRange *page = nullptr;
    const PageKeyRange *base = nullptr;

    [[nodiscard]] bool before(Slice key) const
    {
        return (tree.start() && key.compare(Slice(*tree.start())) <= 0) ||
               (page != nullptr && page->lower && key.compare(Slice(*page->lower)) < 0) ||
               (base != nullptr && base->lower && key.compare(Slice(*base->lower)) < 0);
    }

    [[nodiscard]] bool after(Slice key) const
    {
        return tree.at_or_after_end(key) || (page != nullptr && page->upper && key.compare(Slice(*page->upper)) >= 0) ||
               (base != nullptr && base->upper && key.compare(Slice(*base->upper)) >= 0);
    }
};

template <class Key> std::optional<std::string> resident_hint(size_t count, Key key, const HintRange &range)
{
    size_t first = 0;
    size_t end   = count;
    while (first < end) {
        const auto mid = first + ((end - first) / 2);
        if (range.before(key(mid))) {
            first = mid + 1;
        }
        else {
            end = mid;
        }
    }
    const auto begin = first;
    end              = count;
    while (first < end) {
        const auto mid = first + ((end - first) / 2);
        if (range.after(key(mid))) {
            end = mid;
        }
        else {
            first = mid + 1;
        }
    }
    return begin < end ? std::optional(key(begin + ((end - begin) / 2)).to_string()) : std::nullopt;
}

std::optional<std::string> published_hint(const PublishedTreeState &view, const KeyRange &range)
{
    auto pid = view.root_page_id;
    for (size_t depth = 0; depth < 8; ++depth) {
        const auto page = view.mapping.get(pid);
        if (!page) {
            return {};
        }
        if (page->inner) {
            const auto &inner = *page->inner;
            if (auto hint = resident_hint(
                    inner.separators.size(), [&](size_t i) { return Slice(inner.separators[i]); },
                    HintRange{.tree = range})) {
                return hint;
            }
            size_t child = 0;
            if (range.start()) {
                const auto start = Slice(*range.start());
                child = static_cast<size_t>(std::upper_bound(inner.separators.begin(), inner.separators.end(), start,
                                                             [](Slice key, const OwnedKey &separator) {
                                                                 return key.compare(Slice(separator)) < 0;
                                                             }) -
                                            inner.separators.begin());
            }
            if (child >= inner.children.size()) {
                return {};
            }
            pid = inner.children[child];
            continue;
        }
        if (!page->leaf) {
            return {};
        }
        const auto &leaf    = *page->leaf;
        size_t      visited = 0;
        for (auto it = leaf.overlays.begin(); it != leaf.overlays.end() && visited < 16; ++it, ++visited) {
            if (!*it) {
                continue;
            }
            const auto entries = (*it)->entries();
            if (auto hint = resident_hint(
                    entries.size(), [&](size_t i) { return entries[i].key(); },
                    HintRange{.tree = range, .page = &leaf.range})) {
                return hint;
            }
        }
        visited = 0;
        for (const auto &base : leaf.bases) {
            if (++visited > 16) {
                break;
            }
            if (!base.resident) {
                continue;
            }
            if (auto hint = resident_hint(
                    base.resident->count(), [&](size_t i) { return base.resident->key(i); },
                    HintRange{.tree = range, .page = &leaf.range, .base = &base.range})) {
                return hint;
            }
        }
        return {};
    }
    return {};
}
} // namespace

std::optional<std::string> Crowdbtree::approximate_split_key() const
{
    return published_hint(*core_->capture(), opt_.key_range);
}
} // namespace crowdb::tree
