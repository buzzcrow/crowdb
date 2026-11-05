// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/published_read_internal.h"

namespace crowdb::tree
{
using namespace published_read_detail;

struct PublishedGet::State : ReadPump
{
    State(std::shared_ptr<const PublishedTreeState> captured, std::shared_ptr<PageLoader> transport, Slice wanted,
          PublishedReadLimits bounds)
        : ReadPump(std::move(captured), std::move(transport), bounds),
          key(wanted, bounds.metadata),
          walker(view->root_page_id, view->mapping, bounds.max_route_depth, false, bounds.metadata)
    {
    }

    OwnedKey                         key;
    PublishedRouteCursor             walker;
    PublishedGetResult               report;
    LogicalCell                      memtable;
    std::shared_ptr<const PageState> leaf;
    size_t                           loading_base  = 0;
    bool                             started       = false;
    bool                             loading_value = false;
    bool                             loading_index = false;

    Status &status() override
    {
        return report.status;
    }

    void begin() override
    {
        if (started) {
            report.status = Status::unavailable("point read is a single operation");
            return;
        }
        started = true;
        for (size_t index = 0; index < view->sources->tables.size(); ++index) {
            const auto &table  = view->sources->tables[index];
            auto        guard  = table->read_guard(limits.metadata);
            auto        cursor = table->cursor_from(Slice(key), true);
            if (cursor.valid() && cursor.key() == Slice(key)) {
                auto owner = cursor.cell_owner_at(view->sources->frontier(index));
                if (owner && owner->slot > view->sources->floor && (!memtable.owner || owner->slot > memtable.slot)) {
                    memtable = owned_cell(std::move(owner));
                }
            }
        }
        report.status = walker.seek(Slice(key), false);
        leaf          = walker.leaf();
    }

    bool advance() override
    {
        if (auto backing = walker.needs_inner()) {
            loading_index = true;
            load_inner(std::move(backing));
            return false;
        }
        if (loading_value) {
            return true;
        }
        LogicalLookup lookup;
        if (leaf) {
            lookup = leaf->lookup(Slice(key));
        }
        if (!lookup.status.ok()) {
            report.status = lookup.status;
            return true;
        }
        const bool use_memtable = memtable.owner && (!lookup.cell.owner || memtable.slot >= lookup.cell.slot);
        if (lookup.state == LogicalLookupState::kNeedsBase) {
            std::optional<size_t> needed;
            if (use_memtable) {
                for (size_t i = 0; i < leaf->bases.size(); ++i) {
                    const auto &base = leaf->bases[i];
                    if (base.range.contains(Slice(key)) && !base.resident &&
                        (!base.max_slot || memtable.slot < *base.max_slot)) {
                        needed = i;
                        break;
                    }
                }
            }
            else {
                needed = lookup.base_index;
            }
            if (needed) {
                loading_base = *needed;
                load_base(leaf->bases[*needed]);
                return false;
            }
        }
        if (leaf) {
            uint64_t avoided = 0;
            for (const auto &base : leaf->bases) {
                avoided += static_cast<uint64_t>(!base.resident && base.range.contains(Slice(key)));
            }
            record_work(limits.metadata, TreeWorkCounter::ProxyAvoidedLoads, avoided, TreeWorkCause::Read);
        }
        report.cell  = use_memtable ? memtable : lookup.cell;
        report.found = report.cell.owner && !report.cell.tombstone();
        if (report.found && report.cell.overflow()) {
            loading_value = true;
            load_value(report.cell);
            return false;
        }
        return true;
    }

    void loaded(const LoadEvent &event) override
    {
        if (loading_index) {
            loading_index = false;
            report.status = walker.resume_inner(event.inner);
            leaf          = walker.leaf();
            return;
        }
        if (loading_value) {
            if (!event.value || report.cell.encoded.size() < kOverflowCellSize ||
                event.value->size() != CellView(report.cell.encoded).overflow_len()) {
                report.status = Status::corruption("overflow loader returned no value owner");
                return;
            }
            materialize_value(&report.cell, event.value);
            return;
        }
        if (!event.leaf) {
            report.status = Status::corruption("leaf loader returned no frame owner");
            return;
        }
        auto  resident = detail::reserved_shared<PageState>(limits.metadata, *leaf, limits.metadata);
        auto &base     = resident->bases[loading_base];
        base.resident  = event.leaf;
        leaf           = std::move(resident);
    }
};

std::optional<size_t> PublishedGet::allocation_bound(const PublishedReadLimits &limits, size_t workers) noexcept
{
    return detail::AllocationBound(detail::shared_allocation_bound<State>() +
                                   detail::shared_allocation_bound<PublishedGet>())
        .add(read_metadata_allocation_bound(limits, workers))
        .add(OwnedKey::allocation_overhead())
        .add(limits.max_key_bytes)
        .add(1)
        .value();
}

Status PublishedGet::create(std::shared_ptr<const PublishedTreeState> view, std::shared_ptr<PageLoader> loader,
                            Slice key, PublishedReadLimits limits, std::shared_ptr<PublishedGet> *out)
{
    auto status = validate_read(view, limits);
    if (!status.ok()) {
        return status;
    }
    if (out == nullptr || key.size() > limits.max_key_bytes) {
        return Status::invalid_argument("point read key exceeds bounds or output is absent");
    }
    try {
        auto state = detail::reserved_shared<State>(limits.metadata, std::move(view), std::move(loader), key, limits);
        *out       = detail::reserved_construct<PublishedGet>(limits.metadata, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) PublishedGet(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted("point read allocation failed");
    }
    return Status::Ok();
}

detail::CallbackSender PublishedGet::sender() noexcept
{
    return {this, &PublishedGet::submit};
}

void PublishedGet::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<PublishedGet *>(source)->state_->start(complete, context);
}

const PublishedGetResult &PublishedGet::result() const noexcept
{
    return state_->report;
}

void PublishedGet::request_stop() noexcept
{
    state_->stop();
}
} // namespace crowdb::tree
