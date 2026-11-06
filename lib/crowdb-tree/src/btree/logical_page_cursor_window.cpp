// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"

namespace crowdb::tree
{
std::pair<std::optional<Slice>, bool> LogicalPageCursor::initial_bound() const noexcept
{
    auto bound = reverse_ ? range_.upper_slice() : range_.lower_slice();
    if (start_ && (!bound || (reverse_ ? Slice(start_->key) < *bound : Slice(start_->key) >= *bound))) {
        return {Slice(start_->key), !start_->inclusive};
    }
    return {bound, reverse_};
}

void LogicalPageCursor::seek(Source *source, std::optional<Slice> bound, bool exclusive)
{
    if (source->base != nullptr) {
        const auto base_bound = reverse_ ? source->base->range.upper_slice() : source->base->range.lower_slice();
        if (base_bound && (!bound || (reverse_ ? *base_bound < *bound : *base_bound > *bound))) {
            bound     = base_bound;
            exclusive = reverse_;
        }
    }
    size_t lower = 0;
    size_t upper = source_size(*source);
    if (bound) {
        while (lower < upper) {
            source->index        = lower + ((upper - lower) / 2);
            const auto candidate = source_key(*source);
            if (candidate < *bound || (candidate == *bound && (reverse_ ? !exclusive : exclusive))) {
                lower = source->index + 1;
            }
            else {
                upper = source->index;
            }
        }
    }
    else {
        lower = reverse_ ? upper : 0;
    }
    source->exhausted = reverse_ ? lower == 0 : lower == source_size(*source);
    source->index     = reverse_ && lower != 0 ? lower - 1 : lower;
    position(source);
}

void LogicalPageCursor::select_windowed(std::optional<Slice> after)
{
    // Keep only the winning position. Immutable run/base indices can be sought
    // again without materializing values or allocating a cursor per source.
    auto [bound, exclusive] = initial_bound();
    if (after) {
        bound     = after;
        exclusive = true;
    }
    Source best;
    bool   found    = false;
    auto   consider = [&](Source candidate) {
        seek(&candidate, bound, exclusive);
        if (candidate.exhausted) {
            return;
        }
        const auto order = found ? source_key(candidate).compare(source_key(best)) : 0;
        if (!found || (reverse_ ? order > 0 : order < 0) ||
            (order == 0 && source_slot(candidate) > source_slot(best))) {
            best  = candidate;
            found = true;
        }
    };
    for (const auto &run : page_->overlays) {
        consider({.run = run.get()});
    }
    for (const auto &base : page_->bases) {
        if (base.range.overlaps(range_)) {
            consider({.base = &base});
        }
    }
    sources_.clear();
    winner_.reset();
    if (found) {
        sources_.push_back(best);
        winner_ = 0;
    }
}
} // namespace crowdb::tree
