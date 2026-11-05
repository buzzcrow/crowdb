// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_maintenance.h"

#include <queue>

namespace crowdb::tree
{
namespace
{
class OverlayCoalescer
{
  public:
    [[nodiscard]] size_t removed_primary_bytes() const noexcept
    {
        return removed_primary_bytes_;
    }

    OverlayCoalescer(const PageState &page, const PageMaintenanceLimits &limits, uint64_t first_run_id,
                     std::span<const std::shared_ptr<const OverlayBatch>> inputs)
        : page_(page),
          inputs_(inputs),
          limits_(limits),
          next_id_(first_run_id),
          positions_(inputs.size(), 0, detail::ReservationAllocator<size_t>(limits.metadata)),
          window_(EntryAllocator(limits.metadata))
    {
    }

    Status run(OverlayStorage *output)
    {
        auto later = [&](size_t a, size_t b) {
            const auto &left  = entry(a);
            const auto &right = entry(b);
            const int   order = left.key().compare(right.key());
            if (order != 0) {
                return order > 0;
            }
            return left.slot() != right.slot() ? left.slot() < right.slot() : a > b;
        };
        detail::ReservedVector<size_t> heap{detail::ReservationAllocator<size_t>(limits_.metadata)};
        heap.reserve(inputs_.size());
        std::priority_queue<size_t, detail::ReservedVector<size_t>, decltype(later)> queue(later, std::move(heap));
        for (size_t i = 0; i < inputs_.size(); ++i) {
            const auto &run = inputs_[i];
            if (run == nullptr) {
                return Status::invalid_argument("coalesce overlay has no owner");
            }
            positions_[i] = page_.range.lower.has_value() ? run->lower_bound(Slice(*page_.range.lower)) : 0;
            if (valid(i)) {
                queue.push(i);
            }
        }
        while (!queue.empty()) {
            const EntryRef winner = entry(queue.top());
            auto           status = append(winner, output);
            if (!status.ok()) {
                return status;
            }
            do {
                if (work_ >= limits_.max_work_entries) {
                    return Status::resource_exhausted("overlay coalesce work budget exhausted");
                }
                ++work_;
                const size_t source = queue.top();
                queue.pop();
                ++positions_[source];
                if (valid(source)) {
                    queue.push(source);
                }
            } while (!queue.empty() && entry(queue.top()).key() == winner.key());
        }
        return finish(output);
    }

  private:
    [[nodiscard]] const EntryRef &entry(size_t source) const
    {
        return inputs_[source]->entries()[positions_[source]];
    }

    [[nodiscard]] bool valid(size_t source) const
    {
        return positions_[source] < inputs_[source]->entries().size() && page_.range.contains(entry(source).key());
    }

    Status finish(OverlayStorage *output)
    {
        if (window_.empty()) {
            return Status::Ok();
        }
        if (output->size() >= limits_.max_sources || next_id_ == UINT64_MAX) {
            return Status::resource_exhausted("coalesce output run/identity budget exhausted");
        }
        std::shared_ptr<const OverlayBatch> run;
        EntryStorage                        entries(window_.get_allocator());
        entries.swap(window_);
        auto status = OverlayBatch::build_owned(next_id_, std::move(entries), limits_.run, &run);
        if (!status.ok()) {
            return status;
        }
        output->push_back(std::move(run));
        ++next_id_;
        window_.clear();
        key_bytes_ = 0;
        return Status::Ok();
    }

    Status append(const EntryRef &entry, OverlayStorage *output)
    {
        // Without a base, the merged overlay includes every older version in
        // this leaf. A captured GC floor then permits removing the winner.
        // Cold or resident bases still need the tombstone to mask their value.
        if (page_.bases.empty() && inputs_.size() == page_.overlays.size() && (entry.flags() & kFlagTombstone) != 0 &&
            entry.slot() <= limits_.gc_floor) {
            const size_t bytes = entry.key_length + kCellHeaderSize + kLeafSlotSize;
            if (bytes > SIZE_MAX - removed_primary_bytes_) {
                return Status::resource_exhausted("coalesced tombstone size overflow");
            }
            removed_primary_bytes_ += bytes;
            return Status::Ok();
        }
        if (limits_.run.max_entries == 0 || entry.key_length > limits_.run.max_key_bytes ||
            emitted_ >= limits_.max_metadata_bytes / sizeof(EntryRef)) {
            return Status::resource_exhausted("coalesce entry metadata budget exhausted");
        }
        if (window_.size() >= limits_.run.max_entries || entry.key_length > limits_.run.max_key_bytes - key_bytes_) {
            auto status = finish(output);
            if (!status.ok()) {
                return status;
            }
        }
        window_.push_back(entry);
        key_bytes_ += entry.key_length;
        ++emitted_;
        return Status::Ok();
    }

    const PageState                                     &page_;
    std::span<const std::shared_ptr<const OverlayBatch>> inputs_;
    const PageMaintenanceLimits                         &limits_;
    uint64_t                                             next_id_;
    detail::ReservedVector<size_t>                       positions_;
    EntryStorage                                         window_;
    size_t                                               key_bytes_             = 0;
    size_t                                               emitted_               = 0;
    size_t                                               work_                  = 0;
    size_t                                               removed_primary_bytes_ = 0;
};
} // namespace

PageMaintenanceResult coalesce_overlays(std::shared_ptr<const PageState> page, const PageMaintenanceLimits &limits,
                                        uint64_t representation_id, uint64_t first_run_id)
{
    PageMaintenanceResult result(limits.metadata);
    if (page == nullptr) {
        result.status = Status::invalid_argument("coalesce needs a page owner");
        return result;
    }
    size_t first = page->overlays.size();
    size_t work  = 0;
    while (first != 0 && page->overlays.size() - first < limits.max_sources) {
        const auto &run = page->overlays[first - 1];
        if (!run) {
            result.status = Status::corruption("coalesce overlay has no owner");
            return result;
        }
        const auto begin = page->range.lower ? run->lower_bound(*page->range.lower) : 0;
        const auto end   = page->range.upper ? run->lower_bound(*page->range.upper) : run->entries().size();
        if (end - begin > limits.max_work_entries - work ||
            end - begin > (limits.max_metadata_bytes / sizeof(EntryRef)) - work) {
            break;
        }
        work += end - begin;
        --first;
    }
    if ((first != 0 && first == page->overlays.size()) || (first != 0 && page->overlays.size() - first < 2)) {
        result.status    = Status::resource_exhausted("coalesce needs a complete bounded input window");
        result.readiness = MaintenanceReadiness::kDeferred;
        return result;
    }
    OverlayStorage   merged(OverlayAllocator(limits.metadata));
    OverlayCoalescer merge(*page, limits, first_run_id,
                           std::span<const std::shared_ptr<const OverlayBatch>>(page->overlays).subspan(first));
    result.status = merge.run(&merged);
    if (!result.status.ok()) {
        result.readiness = MaintenanceReadiness::kDeferred;
        return result;
    }
    auto replacement = detail::reserved_shared<PageState>(limits.metadata, *page, limits.metadata);
    replacement->overlays.resize(first);
    replacement->overlays.insert(replacement->overlays.end(), merged.begin(), merged.end());
    // A partial merge also keeps tombstones masking untouched runs.
    replacement->representation_id = representation_id;
    result.page                    = std::move(replacement);
    result.removed_primary_bytes   = merge.removed_primary_bytes();
    return result;
}
} // namespace crowdb::tree
