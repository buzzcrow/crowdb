// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "maintenance_operation_internal.h"
#include "maintenance_publication_internal.h"
#include "maintenance_split_layout.h"

#include <algorithm>

namespace crowdb::tree::detail
{
bool maintenance_needs_coalesce(const PageState &page, size_t max_sources)
{
    if (page.overlays.size() > 1) {
        return true;
    }
    if (page.overlays.size() > max_sources) {
        return false;
    }
    return std::ranges::any_of(page.overlays, [&](const auto &run) {
        return !run || (!run->entries().empty() && (!page.range.contains(run->entries().front().key()) ||
                                                    !page.range.contains(run->entries().back().key())));
    });
}

namespace
{
Status next_run(const PageState &page, size_t max_sources, uint64_t *first)
{
    for (const auto &run : page.overlays) {
        if (!run) {
            return Status::corruption("maintenance overlay has no owner");
        }
        if (run->id() >= UINT64_MAX - max_sources) {
            return Status::resource_exhausted("maintenance run identity exhausted");
        }
        *first = std::max(*first, run->id() + 1);
    }
    return Status::Ok();
}

bool deferred(const Status &status)
{
    return status.code() == Code::kResourceExhausted || status.code() == Code::kUnavailable;
}

TreeOutcome outcome(const Status &status, bool ready = true, bool publication = false)
{
    if (status.ok()) {
        return ready ? TreeOutcome::Success : TreeOutcome::Retry;
    }
    if (publication && status.code() == Code::kUnavailable) {
        return TreeOutcome::Stale;
    }
    return deferred(status) ? TreeOutcome::Retry : TreeOutcome::Error;
}
} // namespace

Status MaintenanceOperation::merge_pair(const std::shared_ptr<const PublishedTreeState> &view,
                                        const std::shared_ptr<const PublishedPage> &left, bool *eligible) const
{
    MergePairPublication classification;
    auto                 status = classify_merge(view, left, &classification);
    *eligible                   = status.ok() && classification.candidate && classification.candidate->ready;
    return status;
}

Status MaintenanceOperation::classify_merge(const std::shared_ptr<const PublishedTreeState> &view,
                                            const std::shared_ptr<const PublishedPage>      &left,
                                            MergePairPublication                            *classification) const
{
    classification->left          = (*candidates)[index].pid;
    classification->expected_left = left;
    if (left->metrics.split_candidate || left->leaf->right_sibling == kInvalidPageId) {
        return Status::Ok();
    }
    const auto right = view->mapping.get(left->leaf->right_sibling);
    if (!right || !right->leaf || right->metrics.split_candidate) {
        return Status::Ok();
    }
    const auto      &limits   = publication.pages;
    constexpr size_t overhead = kFrameHeaderSize + kFrameTrailerSize;
    const bool left_small = left->metrics.page_size_known && left->metrics.page_size_bytes < limits.merge_trigger_bytes;
    const bool right_small =
        right->metrics.page_size_known && right->metrics.page_size_bytes < limits.merge_trigger_bytes;
    if (!left_small && !right_small) {
        return Status::Ok();
    }
    MaintenanceBuilder builder(view, {}, publication);
    MaintenancePath    path{ReservationAllocator<MaintenancePathEntry>(publication.metadata)};
    auto               status = builder.path_to((*candidates)[index].pid, &path);
    if (!status.ok() || path.empty()) {
        return status;
    }
    const auto &parent = path.back();
    if (parent.child + 1 == parent.page->inner->children.size() ||
        parent.page->inner->children[parent.child + 1] != left->leaf->right_sibling) {
        return Status::Ok();
    }
    classification->expected_right  = right;
    classification->expected_parent = parent.page;
    const bool ready =
        left->metrics.page_size_known && left->metrics.kv_known && right->metrics.page_size_known &&
        right->metrics.kv_known && left->metrics.page_size_bytes <= limits.merge_target_bytes &&
        right->metrics.page_size_bytes >= overhead &&
        right->metrics.page_size_bytes - overhead <= limits.merge_target_bytes - left->metrics.page_size_bytes;
    classification->candidate = MergePairProof{.left          = classification->left,
                                               .right         = left->leaf->right_sibling,
                                               .parent        = parent.pid,
                                               .left_content  = left->leaf->content_id,
                                               .right_content = right->leaf->content_id,
                                               .left_route    = left->leaf->route_generation,
                                               .right_route   = right->leaf->route_generation,
                                               .ready         = ready};
    return Status::Ok();
}

MaintenancePublicationResult MaintenanceOperation::plan(const std::shared_ptr<const PublishedTreeState> &view,
                                                        const std::shared_ptr<const PublishedPage>      &page)
{
    const auto pid    = (*candidates)[index].pid;
    auto       record = reserved_shared<InstallRecord>(
        publication.metadata,
        InstallToken{.generation = generation, .batch = invocation, .range = UINT64_MAX - 2, .sequence = ++sequence});
    if (phase == Phase::InnerMerge) {
        auto result = build_inner_merge_publication(view, pid, std::move(record), publication);
        if (result.changed()) {
            action_metrics.emplace(factory->publisher.work_metrics(), TreeStage::PageMerge, tree_metrics_now_ns());
        }
        return result;
    }
    if (phase == Phase::ClassifyMerge) {
        MaintenancePublicationResult result(publication.metadata);
        MergePairPublication         classification;
        result.status = classify_merge(view, page, &classification);
        if (result.status.ok() && classification.candidate != page->merge_outgoing) {
            result.transaction.record     = std::move(record);
            result.transaction.merge_pair = std::move(classification);
        }
        return result;
    }
    if (phase == Phase::Analyze) {
        const auto           started = tree_metrics_now_ns();
        TreeMetricsOperation layout(factory->publisher.work_metrics(), TreeStage::PageLayout, started);
        auto                 result = build_split_step_publication(view, pid, std::move(record), publication);
        (void)layout.finish(outcome(result.status, result.readiness == MaintenanceReadiness::kReady),
                            tree_metrics_now_ns());
        if (result.output_leaf_pids.size() > 1) {
            action_metrics.emplace(factory->publisher.work_metrics(), TreeStage::PageSplit, started);
        }
        return result;
    }
    if (phase == Phase::Coalesce && !maintenance_needs_coalesce(*page->leaf, publication.pages.max_sources)) {
        return {};
    }
    if (phase == Phase::Merge) {
        bool eligible = false;
        auto status   = merge_pair(view, page, &eligible);
        if (!status.ok() || !eligible) {
            MaintenancePublicationResult result;
            result.status = std::move(status);
            return result;
        }
    }
    uint64_t first_run = 1;
    auto     status    = next_run(*page->leaf, publication.pages.max_sources, &first_run);
    if (status.ok() && phase == Phase::Merge) {
        status =
            next_run(*view->mapping.get(page->leaf->right_sibling)->leaf, publication.pages.max_sources, &first_run);
    }
    if (!status.ok()) {
        MaintenancePublicationResult result;
        result.status = std::move(status);
        return result;
    }
    action_metrics.emplace(factory->publisher.work_metrics(),
                           phase == Phase::Coalesce ? TreeStage::OverlayCoalesce : TreeStage::PageMerge,
                           tree_metrics_now_ns());
    return phase == Phase::Coalesce
             ? build_coalesce_publication(view, pid, std::move(record), first_run, publication)
             : build_merge_publication(view, pid, page->leaf->right_sibling, std::move(record), first_run, publication);
}

void MaintenanceOperation::process()
{
    auto view = capture_l1();
    if (view->generation != generation) {
        finish(Status::unavailable("maintenance generation changed"));
        return;
    }
    record_work(factory->publisher.work_metrics(), TreeWorkCounter::CandidateChecks, 1, cause);
    const auto page = view->mapping.get((*candidates)[index].pid);
    if (!page || !page->leaf || page->level != 0) {
        record_work(factory->publisher.work_metrics(), TreeWorkCounter::CandidateSkips, 1, cause);
        advance();
        return;
    }
    if (phase == Phase::InnerMerge &&
        (publication.inner_merge_keys == 0 || view->mapping.get(view->root_page_id)->level < 2)) {
        advance();
        return;
    }
    if ((!target_view && phase == Phase::Analyze && page->metrics.page_size_known && page->metrics.kv_known &&
         !page->metrics.split_candidate && page->metrics.page_size_bytes <= publication.pages.primary_page_bytes) ||
        (phase == Phase::Coalesce && !maintenance_needs_coalesce(*page->leaf, publication.pages.max_sources))) {
        record_work(factory->publisher.work_metrics(), TreeWorkCounter::CandidateSkips, 1, cause);
        advance();
        return;
    }
    if (phase == Phase::Merge) {
        bool eligible = false;
        auto status   = merge_pair(view, page, &eligible);
        if (!status.ok() && !deferred(status)) {
            finish(std::move(status));
            return;
        }
        if (!eligible) {
            record_work(factory->publisher.work_metrics(), TreeWorkCounter::CandidateSkips, 1, cause);
            advance();
            return;
        }
    }
    if (phase != Phase::ClassifyMerge && phase != Phase::InnerMerge) {
        ++steps;
    }
    factory->publisher.set_maintenance_scan_pid((*candidates)[index].pid + 1);
    action_metrics.reset();
    if ((phase == Phase::Analyze || phase == Phase::InnerMerge) && begin_cold(view, page)) {
        return;
    }
    auto result = plan(view, page);
    if (result.status.ok() && result.readiness == MaintenanceReadiness::kNeedsBase && phase == Phase::Analyze &&
        begin_cold(std::move(view), page)) {
        return;
    }
    if (!result.status.ok() || result.readiness != MaintenanceReadiness::kReady || !result.changed()) {
        if (action_metrics) {
            (void)action_metrics->finish(outcome(result.status, result.readiness == MaintenanceReadiness::kReady),
                                         tree_metrics_now_ns());
        }
        if (!result.status.ok() && (target_view || !deferred(result.status))) {
            finish(std::move(result.status));
            return;
        }
        record_work(factory->publisher.work_metrics(), TreeWorkCounter::CandidateSkips, 1, cause);
        advance();
        return;
    }
    if (phase == Phase::ClassifyMerge || phase == Phase::InnerMerge) {
        ++steps;
    }
    submit(std::move(result));
}

void MaintenanceOperation::submit(MaintenancePublicationResult result)
{
    if (target_view) {
        const bool classified =
            result.output_leaf_pids.size() == 1 &&
            std::ranges::any_of(result.transaction.pages, [&](const PageReplacement &change) {
                return change.page_id == target_pid && change.expected && change.replacement &&
                       change.expected->metrics.split_candidate && !change.replacement->metrics.split_candidate &&
                       change.replacement->metrics.page_size_known && change.replacement->metrics.kv_known;
            });
        if (result.output_leaf_pids.size() < 2 && !classified) {
            finish(Status::resource_exhausted("checkpoint split made no structural progress"));
            return;
        }
        result.transaction.expected_l1_seq   = target_view->l1_seq;
        result.transaction.expected_coverage = target_view->coverage;
    }
    inner_merge_pairs   = result.merged_inner_pairs;
    coalesce_input_runs = coalesce_output_runs = coalesce_input_entries = coalesce_output_entries = 0;
    if (phase == Phase::Coalesce) {
        for (const auto &change : result.transaction.pages) {
            if (change.page_id != (*candidates)[index].pid || !change.expected || !change.replacement) {
                continue;
            }
            const auto &before = change.expected->leaf;
            const auto &after  = change.replacement->leaf;
            if (!before || !after) {
                continue;
            }
            coalesce_input_runs  = before->overlays.size();
            coalesce_output_runs = after->overlays.size();
            for (const auto &run : before->overlays) {
                size_t      begin = 0;
                size_t      end   = run->entries().size();
                const auto &lower = before->range.lower;
                const auto &upper = before->range.upper;
                if (lower.has_value()) {
                    begin = run->lower_bound(Slice(lower.value()));
                }
                if (upper.has_value()) {
                    end = run->lower_bound(Slice(upper.value()));
                }
                coalesce_input_entries += end - begin;
            }
            for (const auto &run : after->overlays) {
                coalesce_output_entries += run->entries().size();
            }
        }
    }
    outputs              = std::move(result.output_leaf_pids);
    auto request         = reserved_shared<PublicationRequest>(publication.metadata, publication.metadata);
    request->transaction = std::move(result.transaction);
    auto self            = shared_from_this();
    request->on_done     = [self](PublicationResult published) { self->published(std::move(published)); };
    if (!factory->publisher.try_submit(request)) {
        finish(target_view ? Status::unavailable("checkpoint split publication unavailable") : Status::Ok());
    }
}

void MaintenanceOperation::published(PublicationResult result)
{
    if (action_metrics) {
        (void)action_metrics->finish(outcome(result.status, true, true), tree_metrics_now_ns());
    }
    const auto &work = factory->publisher.work_metrics();
    if (result.status.code() == Code::kUnavailable) {
        record_work(work, TreeWorkCounter::PublicationMismatches, 1, cause);
    }
    if (result.status.ok()) {
        if (phase == Phase::Analyze && outputs.size() > 1) {
            record_work(work, TreeWorkCounter::SplitOperations, 1, cause);
            record_work(work, TreeWorkCounter::SplitOutputPages, outputs.size(), cause);
        }
        else if (phase == Phase::Merge || phase == Phase::InnerMerge) {
            const size_t pairs = inner_merge_pairs + (phase == Phase::Merge ? 1 : 0);
            record_work(work, TreeWorkCounter::MergeOperations, pairs, cause);
            record_work(work, TreeWorkCounter::MergeInputPages, 2 * pairs, cause);
            record_work(work, TreeWorkCounter::MergeOutputPages, pairs, cause);
        }
        else if (phase == Phase::Coalesce) {
            record_work(work, TreeWorkCounter::CoalesceOperations, 1, cause);
            record_work(work, TreeWorkCounter::CoalesceInputRuns, coalesce_input_runs, cause);
            record_work(work, TreeWorkCounter::CoalesceOutputRuns, coalesce_output_runs, cause);
            record_work(work, TreeWorkCounter::CoalesceInputEntries, coalesce_input_entries, cause);
            record_work(work, TreeWorkCounter::CoalesceOutputEntries, coalesce_output_entries, cause);
            if (coalesce_input_entries > coalesce_output_entries) {
                record_work(work, TreeWorkCounter::PayloadReferencesDropped,
                            coalesce_input_entries - coalesce_output_entries, cause);
            }
        }
    }
    if (target_view) {
        target_commit_seq = result.commit_seq;
        target_l1_seq     = result.l1_seq;
        finish(std::move(result.status));
        return;
    }
    auto &candidate = (*candidates)[index];
    if (result.status.code() == Code::kUnavailable && candidate.stale_replans++ < factory->options.max_stale_replans) {
        record_work(factory->publisher.work_metrics(), TreeWorkCounter::Retries, 1, cause);
        schedule();
        return;
    }
    if (!result.status.ok() && !deferred(result.status)) {
        finish(std::move(result.status));
        return;
    }
    if (result.status.ok() && phase == Phase::Analyze) {
        for (size_t i = 1; i < outputs.size() && candidates->size() < factory->options.max_candidates; ++i) {
            candidates->push_back({.pid = outputs[i]});
        }
    }
    advance();
}
} // namespace crowdb::tree::detail
