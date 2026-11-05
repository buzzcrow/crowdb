// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/writeback_metrics.h"
#include "captured_frames_internal.h"
#include "writeback_selection.h"

namespace crowdb::tree
{
void CapturedFrameStream::State::push(uint64_t pid, PageKeyRange range, std::optional<uint32_t> expected_level)
{
    if (stack.size() == limits.max_depth || counters.pages == limits.max_pages) {
        finish(Status::resource_exhausted("captured traversal exceeds its depth/page bound"), true);
        return;
    }
    auto page = view->mapping.get(pid);
    if (!page || pid >= view->next_page_id || (expected_level && page->level != *expected_level)) {
        finish(Status::corruption("captured child is absent or has an invalid level"), true);
        return;
    }
    ++counters.pages;
    stack.push_back({.pid = pid, .page = std::move(page), .range = std::move(range), .fences = {}});
}

void CapturedFrameStream::State::validate(Path &path)
{
    if (path.page->level == 0) {
        if (!path.page->leaf || path.page->inner || path.page->leaf->range.lower != path.range.lower ||
            path.page->leaf->range.upper != path.range.upper) {
            finish(Status::corruption("captured leaf does not match its routing range"), true);
            return;
        }
    }
    else {
        const auto &inner = path.page->inner;
        if (!inner || path.page->leaf || inner->children.empty() ||
            inner->children.size() != inner->separators.size() + 1) {
            finish(Status::corruption("captured index shape is invalid"), true);
            return;
        }
        if (inner->children.size() > limits.encode.max_entries) {
            finish(Status::resource_exhausted("captured index exceeds the child bound"), true);
            return;
        }
        for (size_t i = 0; i < inner->separators.size(); ++i) {
            const auto &key = inner->separators[i];
            if ((path.range.lower && key < *path.range.lower) || (path.range.upper && key > *path.range.upper) ||
                (i != 0 && inner->separators[i - 1] >= key)) {
                finish(Status::corruption("captured index separators violate routing bounds"), true);
                return;
            }
        }
    }
    path.validated = true;
}

bool CapturedFrameStream::State::initialize()
{
    initialized = true;
    if (single_page) {
        stack.push_back({.pid    = single_page->page_id,
                         .page   = single_page->page,
                         .range  = single_page->context.range,
                         .fences = single_page->context.inner_fences.value_or(InnerEncodeFences{})});
        ++counters.pages;
    }
    else if (limits.selected_page) {
        const auto selected = view->mapping.get(*limits.selected_page);
        if (!selected || (selected->level == 0 && !selected->leaf)) {
            finish(Status::unavailable("selected writeback page was replaced"), true);
            return false;
        }
        push(*limits.selected_page, selected->leaf ? selected->leaf->range : PageKeyRange{}, selected->level);
        if (lifecycle.load(std::memory_order_acquire) != 2 || begin_pending) {
            return false;
        }
    }
    else if (view->root_page_id != kInvalidPageId) {
        push(view->root_page_id, {}, {});
        if (lifecycle.load(std::memory_order_acquire) != 2 || begin_pending) {
            return false;
        }
    }
    else if (!view->mapping.empty()) {
        finish(Status::corruption("captured nonempty mapping has no root"), true);
        return false;
    }
    return true;
}

void CapturedFrameStream::State::step()
{
    if (!initialized && !initialize()) {
        return;
    }
    if (stack.empty()) {
        report.complete = true;
        finish(Status::Ok());
        return;
    }
    if (encoder) {
        if (!encoder_ready) {
            prepare_encoder();
        }
        else if (limits.plan_only) {
            plan();
        }
        else {
            emit();
        }
        return;
    }
    auto &path = stack.back();
    if (path.page->level != 0 && !path.page->inner) {
        load_inner();
        return;
    }
    if (!path.validated) {
        validate(path);
        if (lifecycle.load(std::memory_order_acquire) != 2 || begin_pending) {
            return;
        }
    }
    if (limits.selected_page && path.page->level != 0) {
        auto status = writeback_inner_fences(*view, *path.page, limits.encode.max_entries, &path.fences);
        if (!status.ok()) {
            finish(std::move(status), true);
            return;
        }
    }
    if (!single_page && !limits.selected_page && path.page->level != 0 &&
        path.child < path.page->inner->children.size()) {
        const auto &inner = *path.page->inner;
        const auto  index = path.child++;
        auto        range = path.range;
        if (index != 0) {
            range.lower = inner.separators[index - 1];
        }
        if (index < inner.separators.size()) {
            range.upper = inner.separators[index];
        }
        push(inner.children[index], std::move(range), path.page->level - 1);
        if (lifecycle.load(std::memory_order_acquire) == 2) {
            wake();
        }
        return;
    }
    if (!reuse_primary()) {
        prepare();
    }
}

void CapturedFrameStream::State::prepare()
{
    const auto &path = stack.back();
    if (!limits.plan_only && limits.writeback_metrics) {
        auto status = limits.writeback_metrics->begin(path.pid);
        if (!status.ok()) {
            finish(std::move(status), true);
            return;
        }
    }
    CapturedEncodeContext context{
        .range = path.range, .overflow_dependencies = single_page ? single_page->context.overflow_dependencies : view};
    if (path.page->level != 0) {
        context.inner_fences = path.fences;
    }
    auto prepared = prepare_final_page({.page         = path.page,
                                        .page_id      = path.pid,
                                        .coverage     = single_page ? single_page->coverage : view->coverage,
                                        .gc_floor     = single_page ? single_page->gc_floor : view->gc_floor,
                                        .next_page_id = single_page ? single_page->next_page_id : view->next_page_id,
                                        .context      = std::move(context)},
                                       limits.encode);
    if (!prepared.status.ok()) {
        if (prepared.needs_split) {
            report.needs_split = path.pid;
        }
        finish(std::move(prepared.status), true);
        return;
    }
    if (prepared.needs_base && !prepared.encoder) {
        load_base(*prepared.needs_base);
        return;
    }
    if (prepared.needs_inner || !prepared.encoder) {
        finish(Status::corruption("captured encoding did not resolve its page"), true);
        return;
    }
    encoder = std::move(prepared.encoder);
    prepare_encoder();
}

void CapturedFrameStream::State::prepare_encoder()
{
    const auto count = encoder->summary().new_overflow_frames;
    if (!limits.plan_only && count != 0) {
        ReservedOverflowIds ids;
        auto status = reserve_overflow ? reserve_overflow(count, &ids)
                                       : Status::unavailable("snapshot needs an owned overflow identity reservation");
        if (status.ok() && ids.first < overflow_end) {
            status = Status::invalid_argument("snapshot overflow intervals must be disjoint and increasing");
        }
        const auto end = ids.first <= MappingView::kPageLimit && ids.count <= MappingView::kPageLimit - ids.first
                           ? ids.first + ids.count
                           : MappingView::kPageLimit;
        if (status.ok()) {
            status = encoder->bind_overflow(std::move(ids));
        }
        if (!status.ok()) {
            finish(std::move(status), true);
            return;
        }
        overflow_end = end;
    }
    encoder_ready = true;
    wake();
}

void CapturedFrameStream::State::emit()
{
    auto output = encoder->next(buffer->bytes);
    if (!output.status.ok()) {
        finish(std::move(output.status), true);
        return;
    }
    if (!output.frame) {
        finish(Status::corruption("snapshot encoder produced no frame"), true);
        return;
    }
    report.frame = CapturedFrame{.info = std::move(*output.frame), .buffer = buffer, .reused_overflow = {}};
    ++counters.frames;
    if (output.complete) {
        report.frame->reused_overflow = encoder->reused_overflow();
        report.frame->summary         = encoder->summary();
        const auto &encoded           = encoder->counters();
        counters.encoding.primary_frames += encoded.primary_frames;
        counters.encoding.overflow_frames += encoded.overflow_frames;
        counters.encoding.key_copy_bytes += encoded.key_copy_bytes;
        counters.encoding.inline_value_copy_bytes += encoded.inline_value_copy_bytes;
        counters.encoding.overflow_value_copy_bytes += encoded.overflow_value_copy_bytes;
        completed_page(encoder->summary().encoded_entries != 0);
    }
    report.complete = stack.empty();
    finish(Status::Ok());
}

void CapturedFrameStream::State::plan()
{
    report.plan = CapturedFrameResult::Plan{.page_id         = stack.back().pid,
                                            .summary         = encoder->summary(),
                                            .reused_overflow = encoder->reused_overflow(),
                                            .owner           = encoder->dependency_owner()};
    completed_page(encoder->summary().encoded_entries != 0);
    report.complete = stack.empty();
    finish(Status::Ok());
}

void CapturedFrameStream::State::completed_page(bool nonempty_leaf)
{
    auto fences = stack.back().fences;
    if (stack.back().page->level == 0 && nonempty_leaf) {
        fences = {.lower_leaf = stack.back().pid, .upper_leaf = stack.back().pid};
    }
    encoder.reset();
    encoder_ready = false;
    stack.pop_back();
    if (!stack.empty() && fences.lower_leaf != kInvalidPageId) {
        auto &parent = stack.back().fences;
        if (parent.lower_leaf == kInvalidPageId) {
            parent.lower_leaf = fences.lower_leaf;
        }
        parent.upper_leaf = fences.upper_leaf;
    }
}
} // namespace crowdb::tree
