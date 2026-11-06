// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_encode_internal.h"
#include "btree/tree_metrics.h"

#include <algorithm>
#include <new>

namespace crowdb::tree
{
FinalPageEncoder::FinalPageEncoder(std::shared_ptr<detail::FinalPageEncodeState> state) noexcept
    : state_(std::move(state))
{
}

FinalPageEncoder::~FinalPageEncoder() = default;

const FinalPageSummary &FinalPageEncoder::summary() const noexcept
{
    return state_->summary;
}

const PageEncodeCounters &FinalPageEncoder::counters() const noexcept
{
    return state_->counters;
}

std::span<const ReusedOverflowDependency> FinalPageEncoder::reused_overflow() const noexcept
{
    return state_->reused;
}

std::shared_ptr<const void> FinalPageEncoder::dependency_owner() const noexcept
{
    return state_;
}

Status FinalPageEncoder::bind_overflow(ReservedOverflowIds ids)
{
    const auto needed = state_->summary.new_overflow_frames;
    if (!state_->prepared) {
        return Status::unavailable("overflow identities require a complete leaf preparation");
    }
    if (state_->counters.primary_frames != 0 || state_->counters.overflow_frames != 0) {
        return Status::invalid_argument("overflow identities cannot change after encoding starts");
    }
    if (ids.count != needed ||
        (needed != 0 && (!ids.owner || ids.first < state_->next_page_id || ids.first >= MappingView::kPageLimit ||
                         needed > MappingView::kPageLimit - ids.first))) {
        return Status::invalid_argument("overflow interval must be reserved, owned and outside captured PIDs");
    }
    state_->ids = std::move(ids);
    return Status::Ok();
}

PageEncodeStep FinalPageEncoder::next(std::span<uint8_t> output)
{
    if (state_->complete) {
        return {.status = Status::Ok(), .complete = true};
    }
    TreeMetricsOperation metrics(state_->limits.work_metrics, TreeStage::PageEncode, tree_metrics_now_ns());
    try {
        const auto  before = state_->counters;
        auto        result = next_frame(output);
        const auto &after  = state_->counters;
        const auto &work   = state_->limits.work_metrics;
        const auto  cause  = state_->limits.cause;
        record_work(work, TreeWorkCounter::EncodedPrimaryFrames, after.primary_frames - before.primary_frames, cause);
        record_work(work, TreeWorkCounter::EncodedOverflowFrames, after.overflow_frames - before.overflow_frames,
                    cause);
        record_work(work, TreeWorkCounter::EncodingKeyCopyBytes, after.key_copy_bytes - before.key_copy_bytes, cause);
        record_work(work, TreeWorkCounter::EncodingValueCopyBytes,
                    after.inline_value_copy_bytes - before.inline_value_copy_bytes + after.overflow_value_copy_bytes -
                        before.overflow_value_copy_bytes,
                    cause);
        if (result.frame) {
            record_work(work, TreeWorkCounter::EncodedFrameBytes, result.frame->bytes, cause);
        }
        (void)metrics.finish(result.status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
        return result;
    }
    catch (...) {
        (void)metrics.finish(TreeOutcome::Error, tree_metrics_now_ns());
        throw;
    }
}

PageEncodeStep FinalPageEncoder::next_frame(std::span<uint8_t> output)
{
    auto &state = *state_;
    if (!state.prepared) {
        return {.status = Status::unavailable("final leaf preparation is waiting for a physical frame")};
    }
    if (output.size() < state.limits.frame_bytes) {
        return {.status = Status::invalid_argument("final frame output window is too small")};
    }
    if (state.summary.new_overflow_frames != 0 && !state.ids.owner) {
        return {.status = Status::unavailable("final page needs a reserved overflow identity interval")};
    }
    while (state.overflow_entry < state.entries.size() && state.entries[state.overflow_entry].overflow_frames == 0) {
        ++state.overflow_entry;
    }
    if (state.overflow_entry < state.entries.size()) {
        const auto    &entry  = state.entries[state.overflow_entry];
        const uint64_t cap    = overflow_chunk_cap(state.limits.frame_bytes);
        const uint64_t offset = state.overflow_index * cap;
        const auto     bytes  = static_cast<uint32_t>(std::min<uint64_t>(cap, entry.cell.value.size() - offset));
        const uint64_t pid    = state.ids.first + entry.overflow_offset + state.overflow_index;
        const uint64_t next   = state.overflow_index + 1 == entry.overflow_frames ? kInvalidPageId : pid + 1;
        overflow_frame_build(output.data(), state.limits.frame_bytes, pid, next, entry.cell.value.bytes() + offset,
                             bytes);
        ++state.counters.overflow_frames;
        state.counters.overflow_value_copy_bytes += bytes;
        if (++state.overflow_index == entry.overflow_frames) {
            ++state.overflow_entry;
            state.overflow_index = 0;
        }
        return {
            .status = Status::Ok(),
            .frame  = EncodedFrameInfo{.page_id      = pid,
                                       .type         = page_type::kOverflowFrame,
                                       .bytes        = state.limits.frame_bytes,
                                       .dependencies = state.ids.owner}
        };
    }
    detail::encode_primary(state, output.data());
    ++state.counters.primary_frames;
    state.complete = true;
    return {
        .status   = Status::Ok(),
        .frame    = EncodedFrameInfo{.page_id      = state.pid,
                                     .type         = state.page->level == 0 ? page_type::kLeafBase : page_type::kInnerBase,
                                     .bytes        = state.limits.frame_bytes,
                                     .dependencies = state_},
        .complete = true
    };
}

PageEncodePreparation prepare_final_page(const std::shared_ptr<const PublishedTreeState> &captured, uint64_t pid,
                                         const CapturedEncodeContext &context, const PageEncodeLimits &limits)
{
    if (!captured) {
        return {.status      = Status::invalid_argument("page encoding requires a fixed view"),
                .needs_base  = {},
                .needs_inner = {},
                .encoder     = {}};
    }
    const auto page = captured->mapping.get(pid);
    if (page && page->inner) {
        for (const auto child_id : page->inner->children) {
            const auto child = captured->mapping.get(child_id);
            if (!child || child->level + uint64_t{1} != page->level) {
                return {.status =
                            Status::corruption("inner child is absent or has the wrong level in captured mapping"),
                        .needs_base  = {},
                        .needs_inner = {},
                        .encoder     = {}};
            }
        }
        if (context.inner_fences && context.inner_fences->lower_leaf != kInvalidPageId) {
            for (const auto leaf_id : {context.inner_fences->lower_leaf, context.inner_fences->upper_leaf}) {
                const auto leaf = captured->mapping.get(leaf_id);
                if (!leaf || !leaf->leaf || leaf->level != 0 || !leaf->leaf->range.overlaps(context.range)) {
                    return {.status      = Status::invalid_argument("inner fence leaf is absent from captured range"),
                            .needs_base  = {},
                            .needs_inner = {},
                            .encoder     = {}};
                }
            }
        }
    }
    return prepare_final_page({.page         = page,
                               .page_id      = pid,
                               .coverage     = captured->coverage,
                               .gc_floor     = captured->gc_floor,
                               .next_page_id = captured->next_page_id,
                               .context      = context},
                              limits);
}

PageEncodePreparation prepare_final_page(const PageEncodeInput &input, const PageEncodeLimits &limits)
{
    PageEncodePreparation result;
    const auto           &context     = input.context;
    const size_t          lower_bytes = context.range.lower.has_value() ? context.range.lower->size() : 0;
    const size_t          upper_bytes = context.range.upper.has_value() ? context.range.upper->size() : 0;
    if (!input.page || input.page_id >= input.next_page_id || input.next_page_id > MappingView::kPageLimit ||
        (context.range.lower && context.range.upper && *context.range.lower > *context.range.upper) ||
        limits.frame_bytes <= kFrameHeaderSize + kFrameTrailerSize || limits.max_sources == 0 ||
        limits.max_entries == 0 || limits.max_work_entries == 0 || input.gc_floor > input.coverage ||
        lower_bytes > limits.max_metadata_bytes || upper_bytes > limits.max_metadata_bytes - lower_bytes) {
        result.status = Status::invalid_argument("invalid final page encoding bounds");
        return result;
    }
    try {
        auto state          = detail::reserved_shared<detail::FinalPageEncodeState>(limits.metadata, limits);
        state->page         = input.page;
        state->pid          = input.page_id;
        state->next_page_id = input.next_page_id;
        if (!state->page) {
            result.status = Status::invalid_argument("encoded PID is absent from the captured mapping");
            return result;
        }
        result.status = state->page->level == 0
                          ? detail::prepare_leaf(*state, input.coverage, input.gc_floor, context, &result)
                          : detail::prepare_inner(*state, context, &result);
        if (result.status.ok() && !result.needs_base && !result.needs_inner) {
            detail::ReservationAllocator<FinalPageEncoder> allocator(limits.metadata);
            auto                                          *storage = allocator.allocate(1);
            ::new (static_cast<void *>(storage)) FinalPageEncoder(std::move(state));
            result.encoder = detail::ReservedUnique<FinalPageEncoder>(
                storage, detail::ReservationDeleter<FinalPageEncoder>{limits.metadata});
        }
    }
    catch (const std::bad_alloc &) {
        result.status = Status::resource_exhausted();
        result.encoder.reset();
    }
    return result;
}
} // namespace crowdb::tree
