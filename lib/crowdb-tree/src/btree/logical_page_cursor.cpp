// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/logical_page.h"

#include <algorithm>

namespace crowdb::tree
{
std::optional<size_t> LogicalPageCursor::allocation_bound(size_t max_sources) noexcept
{
    return detail::AllocationBound(sizeof(LogicalPageCursor)).add_product(max_sources, sizeof(Source)).value();
}

LogicalPageCursor::~LogicalPageCursor() = default;

LogicalPageCursor::LogicalPageCursor(std::shared_ptr<const PageState> page, PageKeyRange range, uint64_t frontier,
                                     size_t max_sources, bool reverse, std::shared_ptr<AllocationBudget> metadata,
                                     std::optional<LogicalCursorStart> start)
    : page_(std::move(page)),
      range_(std::move(range)),
      frontier_(frontier),
      reverse_(reverse),
      sources_(detail::ReservationAllocator<Source>(metadata)),
      start_(std::move(start))
{
    if (page_ == nullptr) {
        status_ = Status::invalid_argument("logical cursor needs a page owner");
        return;
    }
    if (max_sources == 0) {
        status_ = Status::invalid_argument("logical cursor needs a nonzero source window");
        return;
    }
    windowed_ = page_->overlays.size() > max_sources ||
                page_->bases.size() > max_sources - std::min(max_sources, page_->overlays.size());
    if (!range_.overlaps(page_->range)) {
        return;
    }
    if (page_->range.lower.has_value() &&
        (!range_.lower.has_value() || Slice(*range_.lower) < Slice(*page_->range.lower))) {
        range_.lower = page_->range.lower;
    }
    if (page_->range.upper.has_value() &&
        (!range_.upper.has_value() || Slice(*range_.upper) > Slice(*page_->range.upper))) {
        range_.upper = page_->range.upper;
    }
    sources_.reserve(windowed_ ? 1 : page_->overlays.size() + page_->bases.size());
    for (const auto &run : page_->overlays) {
        if (run == nullptr) {
            status_ = Status::invalid_argument("logical cursor overlay has no owner");
            return;
        }
        if (!windowed_) {
            sources_.push_back({.run = run.get(), .base = nullptr, .index = 0, .exhausted = false});
        }
    }
    for (size_t i = 0; i < page_->bases.size(); ++i) {
        const auto &base = page_->bases[i];
        if (!base.range.overlaps(range_)) {
            continue;
        }
        if (!base.valid()) {
            status_ = Status::corruption("logical base has neither resident nor reloadable owner");
            return;
        }
        if (base.resident == nullptr) {
            needs_base_ = i;
            return;
        }
        if (!windowed_) {
            sources_.push_back({.run = nullptr, .base = &base, .index = 0, .exhausted = false});
        }
    }
    if (windowed_) {
        select_windowed({});
        return;
    }
    for (auto &source : sources_) {
        const auto [bound, exclusive] = initial_bound();
        seek(&source, bound, exclusive);
    }
    select();
}

size_t LogicalPageCursor::source_size(const Source &source) const
{
    return source.run != nullptr ? source.run->entries().size() : source.base->resident->count();
}

Slice LogicalPageCursor::source_key(const Source &source) const
{
    return source.run != nullptr ? source.run->entries()[source.index].key() : source.base->resident->key(source.index);
}

LogicalCell LogicalPageCursor::source_cell(const Source &source) const
{
    return source.run != nullptr ? LogicalCell::from_entry(source.run->entries()[source.index])
                                 : LogicalCell::from_base(source.base->resident, source.index, source.base->backing);
}

uint64_t LogicalPageCursor::source_slot(const Source &source) const
{
    return source.run != nullptr ? source.run->entries()[source.index].slot()
                                 : CellView(source.base->resident->cell(source.index)).slot();
}

void LogicalPageCursor::step(Source *source)
{
    if (reverse_) {
        source->exhausted = source->index == 0;
        if (!source->exhausted) {
            --source->index;
        }
    }
    else {
        ++source->index;
        source->exhausted = source->index == source_size(*source);
    }
}

void LogicalPageCursor::position(Source *source)
{
    while (!source->exhausted) {
        const Slice key = source_key(*source);
        if ((!reverse_ && range_.upper.has_value() && key >= Slice(*range_.upper)) ||
            (reverse_ && range_.lower.has_value() && key < Slice(*range_.lower))) {
            source->exhausted = true;
            return;
        }
        if (range_.contains(key) && (source->base == nullptr || source->base->range.contains(key)) &&
            source_slot(*source) <= frontier_) {
            return;
        }
        step(source);
    }
}

void LogicalPageCursor::select()
{
    winner_.reset();
    for (size_t i = 0; i < sources_.size(); ++i) {
        if (sources_[i].exhausted) {
            continue;
        }
        if (!winner_.has_value()) {
            winner_ = i;
            continue;
        }
        const auto &candidate = sources_[i];
        const auto &winner    = sources_[*winner_];
        const int   order     = source_key(candidate).compare(source_key(winner));
        if ((reverse_ ? order > 0 : order < 0) || (order == 0 && source_slot(candidate) > source_slot(winner))) {
            winner_ = i;
        }
    }
}

Slice LogicalPageCursor::key() const
{
    return winner_.has_value() ? source_key(sources_[*winner_]) : Slice();
}

LogicalCell LogicalPageCursor::cell() const
{
    return winner_.has_value() ? source_cell(sources_[*winner_]) : LogicalCell();
}

void LogicalPageCursor::next()
{
    if (!winner_.has_value()) {
        return;
    }
    const Slice emitted = key();
    if (windowed_) {
        select_windowed(emitted);
        return;
    }
    for (auto &source : sources_) {
        if (!source.exhausted && source_key(source) == emitted) {
            step(&source);
            position(&source);
        }
    }
    select();
}
} // namespace crowdb::tree
