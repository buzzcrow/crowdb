// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_scan_internal.h"

namespace crowdb::tree
{
using namespace published_read_detail;

bool PublishedScan::State::fill_memtable(size_t index)
{
    auto       &source = sources[index];
    const auto &table  = view->sources->tables[index];
    while (!source.done && !source.candidate) {
        if (report.work >= limits.max_work) {
            return false;
        }
        ++report.work;
        OwnedKey                           key;
        std::shared_ptr<const CellVersion> owner;
        {
            auto                       guard = table->read_guard(limits.metadata);
            ConcurrentSkipList::Cursor position;
            if (reverse) {
                auto bound     = source.after ? std::optional<Slice>(Slice(*source.after)) : range.upper_slice();
                bool inclusive = false;
                if (!source.after && options.start && (!bound || Slice(*options.start) < *bound)) {
                    bound     = Slice(*options.start);
                    inclusive = options.start_inclusive;
                }
                position = table->cursor_reverse(bound.value_or(Slice()), bound.has_value(), inclusive);
            }
            else if (source.after) {
                position = table->cursor_from(Slice(*source.after), false);
            }
            else {
                auto bound     = range.lower_slice();
                bool inclusive = true;
                if (options.start && (!bound || Slice(*options.start) >= *bound)) {
                    bound     = Slice(*options.start);
                    inclusive = options.start_inclusive;
                }
                position = bound ? table->cursor_from(*bound, inclusive) : table->cursor({});
            }
            if (!position.valid() || !range.contains(position.key())) {
                source.done = true;
                break;
            }
            if (position.key().size() > limits.max_key_bytes) {
                report.status = Status::resource_exhausted("scan MemTable key exceeds the owned-key budget");
                return false;
            }
            key   = OwnedKey(position.key(), limits.metadata);
            owner = position.cell_owner_at(view->sources->frontier(index));
        }
        source.after = key;
        if (owner && owner->slot > view->sources->floor) {
            source.candidate = PublishedScanEntry{.key = std::move(key), .cell = owned_cell(std::move(owner))};
        }
    }
    return true;
}

bool PublishedScan::State::fill_leaf()
{
    if (leaf_candidate || leaf_done) {
        return true;
    }
    if (!walked) {
        if (report.work >= limits.max_work) {
            return false;
        }
        ++report.work;
        auto bound     = reverse ? range.upper_slice() : range.lower_slice();
        bool exclusive = reverse;
        if (options.start && (!bound || (reverse ? Slice(*options.start) < *bound : Slice(*options.start) >= *bound))) {
            bound     = Slice(*options.start);
            exclusive = !options.start_inclusive;
        }
        report.status = walker.seek(bound, exclusive);
        walked        = true;
        if (!report.status.ok()) {
            return false;
        }
        leaf = walker.leaf();
    }
    while (report.work < limits.max_work) {
        if (auto backing = walker.needs_inner()) {
            loading_index = true;
            load_inner(std::move(backing));
            return false;
        }
        if (!leaf) {
            leaf_done = true;
            return true;
        }
        if (!range.overlaps(leaf->range)) {
            const bool past = reverse ? range.lower && leaf->range.upper && *leaf->range.upper <= *range.lower
                                      : range.upper && leaf->range.lower && *leaf->range.lower >= *range.upper;
            if (past) {
                leaf_done = true;
                return true;
            }
        }
        else if (!cursor) {
            std::optional<LogicalCursorStart> start;
            if (options.start) {
                start = LogicalCursorStart{.key = *options.start, .inclusive = options.start_inclusive};
            }
            cursor = detail::reserved_unique<LogicalPageCursor>(limits.metadata, leaf, range, UINT64_MAX,
                                                                limits.max_page_sources, reverse, limits.metadata,
                                                                std::move(start));
            if (!cursor->status().ok()) {
                report.status = cursor->status();
                return false;
            }
        }
        if (cursor && !cursor->status().ok()) {
            report.status = cursor->status();
            return false;
        }
        const auto needed = cursor ? cursor->needs_base() : std::nullopt;
        if (needed) {
            loading_base = *needed;
            cursor.reset();
            load_base(leaf->bases[loading_base]);
            return false;
        }
        if (cursor && cursor->valid()) {
            if (cursor->key().size() > limits.max_key_bytes) {
                report.status = Status::resource_exhausted("scan leaf key exceeds the owned-key budget");
                return false;
            }
            leaf_candidate =
                PublishedScanEntry{.key = OwnedKey(cursor->key(), limits.metadata), .cell = cursor->cell()};
            cursor->next();
            ++report.work;
            return true;
        }
        cursor.reset();
        report.status = walker.next();
        ++report.work;
        if (!report.status.ok()) {
            return false;
        }
        leaf = walker.leaf();
    }
    return false;
}
} // namespace crowdb::tree
