// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_subtree.h"
#include "page_load.h"
#include "snapshot/flush_persistence.h"
#include "snapshot/primary_backing.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace crowdb::tree
{
void FlushBatch::State::Subtree::prepare_builder()
{
    if (builder) {
        return;
    }
    if (!owner.options.consolidation) {
        throw std::logic_error("flush consolidation options are absent");
    }
    auto limits = *owner.options.consolidation;
    // This is a whole-operation result, not one bounded idle transaction.
    limits.max_changed_pages = std::numeric_limits<size_t>::max();
    builder                  = detail::reserved_unique<detail::MaintenanceBuilder>(
        owner.metadata, owner.publisher.capture(), owner.coverage_record, limits, [this](uint64_t *output) {
            auto next = owner.next_pid.load(std::memory_order_relaxed);
            do {
                if (next >= MappingView::kPageLimit) {
                    return Status::resource_exhausted("flush PID space exhausted");
                }
            } while (!owner.next_pid.compare_exchange_weak(next, next + 1, std::memory_order_relaxed));
            *output = next;
            return Status::Ok();
        });
}

void FlushBatch::State::Subtree::leaf()
{
    if (!owner.options.consolidation) {
        complete(Status::invalid_argument("flush consolidation options are absent"));
        return;
    }
    const auto first = owner.first_input(bounds.lower_slice());
    if (!handed_off && (!first || !bounds.contains(Slice(*first)))) {
        handed_off = true;
    }
    const auto oldest            = expected->primary && expected->primary->frame.summary
                                     ? expected->primary->frame.summary->oldest_tombstone_slot
                                     : UINT64_MAX;
    const bool collect_tombstone = oldest != UINT64_MAX && oldest <= owner.options.consolidation->pages.gc_floor;
    if (!collect_tombstone && !range && handed_off && !expected->metrics.dirty &&
        !expected->metrics.consolidate_candidate && !expected->metrics.split_candidate) {
        output.pids.push_back(pid);
        finish_pages();
        return;
    }
    if (!range && !handed_off) {
#ifdef CROWDB_TREE_TEST_UTIL
        if (!handoff_test_waited && owner.options.handoff_wait_for_tests) {
            handoff_test_waited = true;
            if (owner.options.handoff_wait_for_tests(pid,
                                                     [this, keep = owner.weak_from_this().lock()] { schedule(); })) {
                return;
            }
        }
        if (owner.options.before_handoff_for_tests) {
            owner.options.before_handoff_for_tests(pid);
        }
#endif
        const auto index = owner.next_range.fetch_add(1, std::memory_order_relaxed);
        auto record      = detail::reserved_shared<InstallRecord>(owner.metadata,
                                                                  InstallToken{.generation = owner.captured->generation,
                                                                               .batch      = owner.report.batch_id,
                                                                               .range      = index,
                                                                               .sequence   = 0});
        range            = detail::reserved_construct<Range>(owner.metadata, [&](Range *storage) {
            ::new (static_cast<void *>(storage))
                Range{.index    = index,
                      .page_id  = pid,
                      .bounds   = bounds,
                      .expected = expected,
                      .record   = std::move(record),
                      .runs     = OverlayStorage{OverlayAllocator(owner.metadata)},
                      .installs =
                          detail::ReservedVector<std::shared_ptr<InstallRecord>>{
                              detail::ReservationAllocator<std::shared_ptr<InstallRecord>>(owner.metadata)},
                      .next_key        = {},
                      .next_install_at = owner.options.install_run_window};
        });
    }
    if (!handed_off) {
        owner.start_range(*range, {.context = this, .complete = +[](void *raw, Status result) noexcept {
                                       auto &self      = *static_cast<Subtree *>(raw);
                                       self.status     = std::move(result);
                                       self.handed_off = self.status.ok();
                                       self.schedule();
                                   }});
        return;
    }
    prepare_builder();
    if (!loaded_leaf) {
        loaded_leaf = detail::reserved_shared<PageState>(
            builder->options.metadata, *(range ? range->expected : expected)->leaf, builder->options.metadata);
    }
    while (next_base < loaded_leaf->bases.size()) {
        auto &base = loaded_leaf->bases[next_base];
        if (base.resident) {
            ++next_base;
            continue;
        }
        if (!owner.options.loader) {
            complete(Status::unavailable("flush leaf requires an asynchronous loader"));
            return;
        }
        request_load();
        return;
    }
    prepare_builder();
    auto limits        = builder->options.pages;
    limits.max_sources = std::max(limits.max_sources, loaded_leaf->bases.size() + loaded_leaf->overlays.size());
    if (!layout) {
        layout_metrics.emplace(owner.publisher.work_metrics(), TreeStage::PageLayout, tree_metrics_now_ns());
        layout = detail::reserved_unique<detail::SplitWindowCursor>(owner.metadata, loaded_leaf, limits);
    }
    auto result = layout->advance_complete();
    if (!result.ok()) {
        complete(std::move(result));
        return;
    }
    if (!layout->done()) {
        schedule();
        return;
    }
    if (layout_metrics) {
        (void)layout_metrics->finish(TreeOutcome::Success, tree_metrics_now_ns());
    }
    std::optional<TreeMetricsOperation> split_metrics;
    if (!layout->layout().separators.empty()) {
        split_metrics.emplace(owner.publisher.work_metrics(), TreeStage::PageSplit, tree_metrics_now_ns());
    }
    output.pids.clear();
    output.separators.clear();
    result =
        detail::build_flush_leaf_outputs(*builder, pid, range ? range->expected : expected, layout->layout(), &output);
    if (split_metrics) {
        (void)split_metrics->finish(result.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
        if (result.ok()) {
            record_work(owner.publisher.work_metrics(), TreeWorkCounter::SplitOperations, 1, TreeWorkCause::Flush);
            record_work(owner.publisher.work_metrics(), TreeWorkCounter::SplitOutputPages, output.pids.size(),
                        TreeWorkCause::Flush);
        }
    }
    if (!result.ok()) {
        complete(std::move(result));
        return;
    }
    result = coalesce_leaf(limits);
    if (!result.ok()) {
        complete(std::move(result));
        return;
    }
    layout.reset();
    loaded_leaf.reset();
#ifdef CROWDB_TREE_TEST_UTIL
    if (owner.options.after_page_for_tests) {
        owner.options.after_page_for_tests(pid, 0);
    }
#endif
    finish_pages();
}

void FlushBatch::State::Subtree::request_load()
{
    load();
}

void FlushBatch::State::Subtree::load() noexcept
{
    try {
        if ((owner.lifecycle.load(std::memory_order_acquire) & kStopped) != 0) {
            status = Status::unavailable("flush stopped before cold load");
            schedule();
            return;
        }
        if (writing) {
            if (!owner.options.consolidation) {
                status = Status::invalid_argument("flush consolidation options are absent");
                schedule();
                return;
            }
            auto target = final_pages[persist_at];
            auto page   = target.task->lookup(target.pid);
            owner.options.persistence->write(
                {
                    .page         = page,
                    .page_id      = target.pid,
                    .coverage     = owner.report.frontier,
                    .gc_floor     = owner.options.consolidation->pages.gc_floor,
                    .next_page_id = owner.next_pid.load(std::memory_order_relaxed),
                    .context = {.range        = page->leaf ? page->leaf->range : bounds,
                                .inner_fences = page->level != 0 ? std::optional(fences(target.pid)) : std::nullopt},
                    .children_summary = page->level != 0 ? std::optional(child_summary(target.pid)) : std::nullopt
            },
                [this, lease = owner.metadata](uint64_t count, ReservedOverflowIds *out) {
                    auto next = owner.next_pid.load(std::memory_order_relaxed);
                    do {
                        if (count > MappingView::kPageLimit - next) {
                            return Status::resource_exhausted("flush overflow PID space exhausted");
                        }
                    } while (!owner.next_pid.compare_exchange_weak(next, next + count, std::memory_order_relaxed));
                    *out = {.first = next, .count = count, .owner = lease};
                    return Status::Ok();
                },
                [this, target, keep = owner.weak_from_this().lock()](Status result, auto replacement) noexcept {
                    try {
                        if (result.ok() && replacement) {
                            target.task->prepare_builder();
                            result = target.task->builder->replace(target.pid, std::move(replacement));
                        }
                    }
                    catch (const std::bad_alloc &) {
                        result = Status::resource_exhausted();
                    }
                    catch (const std::exception &) {
                        result = Status::internal_error("flush receipt installation failed");
                    }
                    status = std::move(result);
                    ++persist_at;
                    writing = false;
                    schedule();
                });
            return;
        }
        if (expected->level != 0) {
            owner.options.loader->load_inner(expected->inner_backing, [this, keep = owner.weak_from_this().lock()](
                                                                          Status result, auto page) noexcept {
                status = std::move(result);
                inner  = std::move(page);
                if (status.ok() && !inner) {
                    status = Status::corruption("flush loader returned no index");
                }
                schedule();
            });
        }
        else {
            auto loader =
                owner.options.consolidation_loader ? owner.options.consolidation_loader : owner.options.loader;
            loader->load_leaf(loaded_leaf->bases[next_base],
                              [this, keep = owner.weak_from_this().lock()](Status result, auto page) noexcept {
                                  status = std::move(result);
                                  if (status.ok() && !page) {
                                      status = Status::corruption("flush loader returned no leaf");
                                  }
                                  if (status.ok()) {
                                      loaded_leaf->bases[next_base++].resident = std::move(page);
                                  }
                                  schedule();
                              });
        }
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted("flush cold load allocation failed");
        schedule();
    }
    catch (const std::exception &) {
        status = Status::internal_error("flush cold load failed");
        schedule();
    }
}

Status FlushBatch::State::Subtree::coalesce_leaf(const PageMaintenanceLimits &limits)
{
    for (auto output_pid : output.pids) {
        const auto page              = builder->lookup(output_pid);
        const bool clipped           = std::ranges::any_of(page->leaf->overlays, [&](const auto &run) {
            return !run->entries().empty() && (!page->leaf->range.contains(run->entries().front().key()) ||
                                               !page->leaf->range.contains(run->entries().back().key()));
        });
        const bool collect_tombstone = page->leaf->overlays.size() == 1 && !clipped && page->leaf->bases.empty() &&
                                       std::ranges::any_of(page->leaf->overlays, [&](const auto &run) {
                                           if (run->min_slot() > limits.gc_floor) {
                                               return false;
                                           }
                                           return std::ranges::any_of(run->entries(), [&](const auto &entry) {
                                               return page->leaf->range.contains(entry.key()) &&
                                                      entry.slot() <= limits.gc_floor &&
                                                      (entry.flags() & kFlagTombstone) != 0;
                                           });
                                       });
        if (page->leaf->overlays.size() <= 1 && !clipped && !collect_tombstone) {
            continue;
        }
        if (page->leaf->representation_id == UINT64_MAX) {
            return Status::resource_exhausted("flush representation identity exhausted");
        }
        auto compact_limits = limits;
        compact_limits.max_work_entries =
            std::max(compact_limits.max_work_entries, static_cast<size_t>(page->metrics.overlay_entries));
        compact_limits.max_metadata_bytes =
            std::max(compact_limits.max_metadata_bytes, compact_limits.max_work_entries * sizeof(EntryRef));
        uint64_t run = 1;
        for (const auto &input : page->leaf->overlays) {
            if (input->id() >= UINT64_MAX - compact_limits.max_sources) {
                return Status::resource_exhausted("flush run identities exhausted");
            }
            run = std::max(run, input->id() + 1);
        }
        TreeMetricsOperation coalesce_metrics(owner.publisher.work_metrics(), TreeStage::OverlayCoalesce,
                                              tree_metrics_now_ns());
        auto compacted = coalesce_overlays(page->leaf, compact_limits, page->leaf->representation_id + 1, run);
        (void)coalesce_metrics.finish(compacted.status.ok() ? TreeOutcome::Success : TreeOutcome::Error,
                                      tree_metrics_now_ns());
        if (!compacted.status.ok()) {
            return std::move(compacted.status);
        }
        uint64_t output_entries = 0;
        for (const auto &run : compacted.page->overlays) {
            output_entries += run->entries().size();
        }
        record_work(owner.publisher.work_metrics(), TreeWorkCounter::CoalesceOperations, 1, TreeWorkCause::Flush);
        record_work(owner.publisher.work_metrics(), TreeWorkCounter::CoalesceInputRuns, page->leaf->overlays.size(),
                    TreeWorkCause::Flush);
        record_work(owner.publisher.work_metrics(), TreeWorkCounter::CoalesceOutputRuns,
                    compacted.page->overlays.size(), TreeWorkCause::Flush);
        record_work(owner.publisher.work_metrics(), TreeWorkCounter::CoalesceInputEntries,
                    page->metrics.overlay_entries, TreeWorkCause::Flush);
        record_work(owner.publisher.work_metrics(), TreeWorkCounter::CoalesceOutputEntries, output_entries,
                    TreeWorkCause::Flush);
        LeafLayoutStats stats{.kv_count = page->metrics.kv_count, .kv_bytes = page->metrics.kv_bytes};
        auto result = builder->make_leaf(output_pid, page, std::move(compacted.page),
                                         page->metrics.page_size_bytes - compacted.removed_primary_bytes, {}, &stats);
        if (!result.ok()) {
            return result;
        }
    }
    return Status::Ok();
}
} // namespace crowdb::tree
