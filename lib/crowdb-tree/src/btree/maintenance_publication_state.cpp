// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_publication_internal.h"

#include <algorithm>
#include <array>

namespace crowdb::tree::detail
{
MaintenanceBuilder::MaintenanceBuilder(std::shared_ptr<const PublishedTreeState> captured,
                                       std::shared_ptr<InstallRecord>            record,
                                       const MaintenancePublicationOptions      &limits,
                                       InlineFunction<Status(uint64_t *)>        allocate)
    : view(std::move(captured)),
      options(limits),
      allocate_(std::move(allocate)),
      record_(std::move(record)),
      changes_(ReservationAllocator<Change>(limits.metadata)),
      next_pid_(view ? view->next_page_id : 0),
      root_pid_(view ? view->root_page_id : kInvalidPageId)
{
    options.pages.metadata = options.metadata;
}

Status MaintenanceBuilder::validate() const
{
    constexpr size_t minimum_inner = kFrameHeaderSize + kFrameTrailerSize + (2 * sizeof(uint64_t)) + kInnerSlotSize;
    if (!view || !record_ || record_->token.generation != view->generation || options.max_inner_children < 2 ||
        options.max_inner_bytes < minimum_inner || options.max_inner_bytes > UINT32_MAX ||
        options.max_path_pages == 0 || options.max_changed_pages == 0 ||
        options.pages.max_metadata_bytes < minimum_inner) {
        return Status::invalid_argument("invalid maintenance view, token or bounds");
    }
    return Status::Ok();
}

std::shared_ptr<const PublishedPage> MaintenanceBuilder::lookup(uint64_t pid) const
{
    const auto changed = changes_.find(pid);
    return changed == changes_.end() ? view->mapping.get(pid) : changed->second.replacement;
}

Status MaintenanceBuilder::allocate_pid(uint64_t *pid)
{
    if (allocate_) {
        return allocate_(pid);
    }
    if (next_pid_ >= MappingView::kPageLimit) {
        return Status::resource_exhausted("maintenance PID space exhausted");
    }
    *pid = next_pid_++;
    return Status::Ok();
}

Status MaintenanceBuilder::replace(uint64_t pid, std::shared_ptr<const PublishedPage> replacement)
{
    const auto expected = view->mapping.get(pid);
    if (!expected && !replacement) {
        changes_.erase(pid);
        return Status::Ok();
    }
    if (!changes_.contains(pid) && changes_.size() >= options.max_changed_pages) {
        return Status::resource_exhausted("maintenance transaction page bound exhausted");
    }
    changes_.insert_or_assign(
        pid, PageReplacement{.page_id = pid, .expected = expected, .replacement = std::move(replacement)});
    return Status::Ok();
}

Status next_page_version(const PageState &left, const PageState *right, PageVersion *version)
{
    const uint64_t route          = std::max(left.route_generation, right != nullptr ? right->route_generation : 0);
    const uint64_t content        = std::max(left.content_id, right != nullptr ? right->content_id : 0);
    const uint64_t representation = std::max(left.representation_id, right != nullptr ? right->representation_id : 0);
    if (route == UINT64_MAX || content == UINT64_MAX || representation == UINT64_MAX) {
        return Status::resource_exhausted("logical page identity exhausted");
    }
    *version = {.route_generation = route + 1, .content_id = content + 1, .representation_id = representation + 1};
    return Status::Ok();
}

Status summarize_leaf(const std::shared_ptr<const PageState> &page, const PageMaintenanceLimits &limits,
                      PageMetricsContribution *metrics)
{
    LogicalPageCursor cursor(page, page->range, UINT64_MAX, limits.max_sources, false, limits.metadata);
    if (!cursor.status().ok()) {
        return cursor.status();
    }
    if (cursor.needs_base().has_value()) {
        return Status::unavailable("exact maintenance summary still needs a base");
    }
    metrics->kv_known = true;
    metrics->kv_count = 0;
    metrics->kv_bytes = 0;
    size_t visited    = 0;
    while (cursor.valid()) {
        if (++visited > limits.max_work_entries) {
            return Status::resource_exhausted("maintenance summary work bound exhausted");
        }
        const auto cell = cursor.cell();
        if (!cell.tombstone()) {
            if (cell.overflow() && cell.encoded.size() < kOverflowCellSize) {
                return Status::corruption("maintenance found an invalid overflow cell");
            }
            const uint64_t value_bytes = cell.overflow() ? CellView(cell.encoded).overflow_len() : cell.value.size();
            if (value_bytes > UINT64_MAX - cursor.key().size() ||
                value_bytes + cursor.key().size() > UINT64_MAX - metrics->kv_bytes) {
                return Status::resource_exhausted("maintenance summary size overflow");
            }
            ++metrics->kv_count;
            metrics->kv_bytes += value_bytes + cursor.key().size();
        }
        cursor.next();
    }
    if (!cursor.status().ok()) {
        return cursor.status();
    }
    if (cursor.needs_base()) {
        return Status::unavailable("exact maintenance summary requires another physical frame");
    }
    return Status::Ok();
}

Status MaintenanceBuilder::make_leaf(uint64_t pid, const std::shared_ptr<const PublishedPage> &old,
                                     std::shared_ptr<const PageState> logical, std::optional<size_t> exact_bytes,
                                     std::shared_ptr<const void> second_owner, const LeafLayoutStats *stats,
                                     size_t removed_primary_bytes)
{
    const auto previous = view->mapping.get(pid);
    if (previous && previous->revision == UINT64_MAX) {
        return Status::resource_exhausted("leaf revision exhausted");
    }
    auto page              = reserved_shared<PublishedPage>(options.metadata);
    page->revision         = previous ? previous->revision + 1 : 1;
    page->leaf             = std::move(logical);
    page->allocation_owner = old->allocation_owner;
    if (second_owner && second_owner != old->allocation_owner) {
        page->allocation_owner = reserved_shared<std::array<std::shared_ptr<const void>, 2>>(
            options.metadata,
            std::array<std::shared_ptr<const void>, 2>{old->allocation_owner, std::move(second_owner)});
    }
    auto &metrics = page->metrics;
    if (exact_bytes.has_value()) {
        metrics.dirty                    = true;
        metrics.writeback_candidate      = true;
        metrics.page_size_known          = true;
        metrics.page_size_bytes          = *exact_bytes;
        metrics.page_size_estimate_bytes = *exact_bytes;
        if (stats != nullptr) {
            metrics.kv_known = true;
            metrics.kv_count = stats->kv_count;
            metrics.kv_bytes = stats->kv_bytes;
        }
        else {
            auto status = summarize_leaf(page->leaf, options.pages, &metrics);
            if (!status.ok()) {
                return status;
            }
        }
    }
    else {
        metrics                       = old->metrics;
        metrics.consolidate_candidate = false;
        if (metrics.page_size_known && removed_primary_bytes != 0) {
            constexpr size_t overhead = kFrameHeaderSize + kFrameTrailerSize;
            if (metrics.page_size_bytes < overhead || removed_primary_bytes > metrics.page_size_bytes - overhead) {
                return Status::corruption("coalesced tombstones exceed the captured primary size");
            }
            metrics.page_size_bytes -= removed_primary_bytes;
        }
        metrics.page_size_estimate_bytes -= std::min(metrics.page_size_estimate_bytes, removed_primary_bytes);
    }
    metrics.overlay_runs    = page->leaf->overlays.size();
    metrics.overlay_entries = 0;
    for (const auto &run : page->leaf->overlays) {
        const auto  &range = page->leaf->range;
        const size_t first = range.lower ? run->lower_bound(*range.lower) : 0;
        const size_t last  = range.upper ? run->lower_bound(*range.upper) : run->entries().size();
        metrics.overlay_entries += last - first;
    }
    return replace(pid, std::move(page));
}

Status MaintenanceBuilder::finish(MaintenancePublicationResult *result)
{
    PublicationTransaction transaction{
        .pages = PublicationTransaction::Pages(ReservationAllocator<PageReplacement>(options.metadata))};
    transaction.record                = record_;
    transaction.root_page_id          = root_pid_;
    transaction.expected_root_page_id = view->root_page_id;
    transaction.expected_next_page_id = view->next_page_id;
    transaction.metadata              = options.metadata;
    transaction.snapshot_metadata     = options.snapshot_metadata;
    transaction.snapshot_effect       = SnapshotChangeEffect::SourceStructure;
    transaction.pages.reserve(changes_.size());
    for (auto &[pid, replacement] : changes_) {
        (void)pid;
        transaction.pages.push_back(replacement);
    }
    result->transaction        = std::move(transaction);
    result->merged_inner_pairs = inner_merges_;
    return Status::Ok();
}
} // namespace crowdb::tree::detail
