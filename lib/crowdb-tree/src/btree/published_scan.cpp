// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/published_scan_internal.h"

#include <algorithm>

namespace crowdb::tree
{
using namespace published_read_detail;

PublishedScan::State::State(std::shared_ptr<const PublishedTreeState> captured, std::shared_ptr<PageLoader> transport,
                            PageKeyRange selection, bool descending, PublishedReadLimits bounds,
                            PublishedScanOptions selection_options)
    : ReadPump(std::move(captured), std::move(transport), bounds),
      range(std::move(selection)),
      reverse(descending),
      options(std::move(selection_options)),
      report{.status  = Status::Ok(),
             .entries = detail::ReservedVector<PublishedScanEntry>(
                 detail::ReservationAllocator<PublishedScanEntry>(bounds.metadata))},
      sources(view->sources->tables.size(), Source{}, detail::ReservationAllocator<Source>(bounds.metadata)),
      walker(view->root_page_id, view->mapping, bounds.max_route_depth, descending, bounds.metadata, range)
{
    remaining_bytes = options.byte_budget.value_or(limits.max_result_bytes);
}

Status &PublishedScan::State::status()
{
    return report.status;
}

void PublishedScan::State::begin()
{
    report.entries.clear();
    report.work               = 0;
    report.done               = finished || range.empty();
    report.byte_limit_reached = false;
    result_bytes              = 0;
    if (!options.byte_budget) {
        remaining_bytes = limits.max_result_bytes;
    }
}

bool PublishedScan::State::prepare_sources()
{
    for (size_t i = 0; i < sources.size(); ++i) {
        if (!fill_memtable(i)) {
            return false;
        }
    }
    return fill_leaf();
}

const PublishedScanEntry *PublishedScan::State::winner() const
{
    const PublishedScanEntry *best     = nullptr;
    auto                      consider = [&](const std::optional<PublishedScanEntry> &entry) {
        if (!entry) {
            return;
        }
        if (!best) {
            best = &*entry;
            return;
        }
        const int order = Slice(entry->key).compare(best->key);
        if ((reverse ? order > 0 : order < 0) || (order == 0 && entry->cell.slot > best->cell.slot)) {
            best = &*entry;
        }
    };
    for (const auto &source : sources) {
        consider(source.candidate);
    }
    consider(leaf_candidate);
    return best;
}

void PublishedScan::State::consume(Slice key)
{
    for (auto &source : sources) {
        if (source.candidate && Slice(source.candidate->key) == key) {
            source.candidate.reset();
        }
    }
    if (leaf_candidate && Slice(leaf_candidate->key) == key) {
        leaf_candidate.reset();
    }
}

void PublishedScan::State::emit(PublishedScanEntry entry)
{
    if (options.keys_only || entry.cell.tombstone()) {
        entry.cell.value = {};
    }
    const size_t value_bytes = entry.cell.value.size();
    const auto   bytes       = entry.key.size() + value_bytes;
    result_bytes += bytes;
    remaining_bytes -= std::min(remaining_bytes, bytes);
    emitted = true;
    consume(Slice(entry.key));
    report.entries.push_back(std::move(entry));
    ++report.work;
}

bool PublishedScan::State::advance()
{
    if (report.done) {
        return true;
    }
    if (pending_entry && !loading_value) {
        auto entry = std::move(*pending_entry);
        pending_entry.reset();
        emit(std::move(entry));
    }
    while (report.work < limits.max_work) {
        if (!prepare_sources()) {
            return !report.status.ok() || report.work >= limits.max_work;
        }
        if (report.work >= limits.max_work) {
            return true;
        }
        const auto *selected = winner();
        if (selected == nullptr) {
            finished    = true;
            report.done = true;
            return true;
        }
        const int  start_order  = options.start ? Slice(selected->key).compare(*options.start) : 0;
        const bool before_start = options.start && ((reverse ? start_order > 0 : start_order < 0) ||
                                                    (start_order == 0 && !options.start_inclusive));
        if (before_start || (selected->cell.tombstone() && !options.include_tombstones)) {
            const OwnedKey key = selected->key;
            consume(Slice(key));
            ++report.work;
            continue;
        }
        uint64_t value_bytes = 0;
        if (!options.keys_only && !selected->cell.tombstone()) {
            value_bytes = selected->cell.overflow() && selected->cell.encoded.size() >= kOverflowCellSize
                            ? CellView(selected->cell.encoded).overflow_len()
                            : selected->cell.value.size();
        }
        // Test the public budget before loading overflow. A work window may
        // end earlier without exhausting that budget or consuming this key.
        const bool first = options.byte_budget ? !emitted : report.entries.empty();
        if ((selected->key.size() > remaining_bytes || value_bytes > remaining_bytes - selected->key.size()) &&
            !(first && options.allow_oversized_first)) {
            report.byte_limit_reached = true;
            return true;
        }
        if (report.entries.size() >= limits.max_entries) {
            return true;
        }
        const auto window_left = limits.max_result_bytes - std::min(result_bytes, limits.max_result_bytes);
        if (!report.entries.empty() &&
            (selected->key.size() > window_left || value_bytes > window_left - selected->key.size())) {
            return true;
        }
        if (selected->cell.overflow() && !options.keys_only && !selected->cell.tombstone()) {
            pending_entry = *selected;
            loading_value = true;
            load_value(selected->cell);
            return false;
        }
        emit(*selected);
    }
    return true;
}

void PublishedScan::State::loaded(const LoadEvent &event)
{
    if (loading_index) {
        loading_index = false;
        report.status = walker.resume_inner(event.inner);
        leaf          = walker.leaf();
        return;
    }
    if (loading_value) {
        if (!event.value || !pending_entry || pending_entry->cell.encoded.size() < kOverflowCellSize ||
            event.value->size() != CellView(pending_entry->cell.encoded).overflow_len()) {
            report.status = Status::corruption("overflow loader returned no value owner");
            return;
        }
        materialize_value(&pending_entry->cell, event.value);
        loading_value = false;
        return;
    }
    if (!event.leaf) {
        report.status = Status::corruption("leaf loader returned no frame owner");
        return;
    }
    auto resident = detail::reserved_shared<PageState>(limits.metadata, *leaf, limits.metadata);
    resident->bases[loading_base].resident = event.leaf;
    leaf                                   = std::move(resident);
}

std::optional<size_t> PublishedScan::allocation_bound(const PublishedReadLimits &limits, size_t workers) noexcept
{
    using detail::AllocationBound;
    using detail::shared_allocation_bound;
    const auto keys = AllocationBound().add_product(2, limits.max_memtables).add(limits.max_entries).add(6).value();
    const auto key  = AllocationBound(OwnedKey::allocation_overhead()).add(limits.max_key_bytes).add(1).value();
    if (!keys || !key) {
        return {};
    }
    return AllocationBound(shared_allocation_bound<State>() + shared_allocation_bound<PublishedScan>())
        .add(read_metadata_allocation_bound(limits, workers))
        .add(LogicalPageCursor::allocation_bound(limits.max_page_sources))
        .add_product(limits.max_memtables, sizeof(State::Source))
        .add_product(limits.max_entries, 3 * sizeof(PublishedScanEntry))
        .add_product(*keys, *key)
        .value();
}

Status PublishedScan::create(std::shared_ptr<const PublishedTreeState> view, std::shared_ptr<PageLoader> loader,
                             PageKeyRange range, bool reverse, PublishedReadLimits limits,
                             std::shared_ptr<PublishedScan> *out, PublishedScanOptions options)
{
    auto status = validate_read(view, limits);
    if (!status.ok()) {
        return status;
    }
    if (out == nullptr || (range.lower && range.lower->size() > limits.max_key_bytes) ||
        (range.upper && range.upper->size() > limits.max_key_bytes) ||
        (options.start && options.start->size() > limits.max_key_bytes)) {
        return Status::invalid_argument("scan bounds exceed key budget or output is absent");
    }
    try {
        if (limits.metadata) {
            for (auto *bound : {&range.lower, &range.upper}) {
                if (*bound) {
                    *bound = OwnedKey(Slice(**bound), limits.metadata);
                }
            }
            if (options.start) {
                options.start = OwnedKey(Slice(*options.start), limits.metadata);
            }
        }
        auto state = detail::reserved_shared<State>(limits.metadata, std::move(view), std::move(loader),
                                                    std::move(range), reverse, limits, std::move(options));
        *out       = detail::reserved_construct<PublishedScan>(limits.metadata, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) PublishedScan(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted("scan allocation failed");
    }
    return Status::Ok();
}

detail::CallbackSender PublishedScan::next_sender() noexcept
{
    return {this, &PublishedScan::submit};
}

void PublishedScan::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<PublishedScan *>(source)->state_->start(complete, context);
}

const PublishedScanResult &PublishedScan::result() const noexcept
{
    return state_->report;
}

uint64_t PublishedScan::skipped_subtrees() const noexcept
{
    return state_->walker.skipped_subtrees();
}

void PublishedScan::request_stop() noexcept
{
    state_->stop();
}
} // namespace crowdb::tree
