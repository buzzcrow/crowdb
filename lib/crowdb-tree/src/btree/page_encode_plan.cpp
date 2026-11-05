// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_encode_internal.h"

#include <algorithm>

namespace crowdb::tree::detail
{
Status prepare_inner(FinalPageEncodeState &state, const CapturedEncodeContext &context, PageEncodePreparation *result)
{
    constexpr uint64_t kPageOverhead = kFrameHeaderSize + kFrameTrailerSize;
    if (state.page->leaf) {
        return Status::corruption("captured inner page has a leaf representation");
    }
    if (!state.page->inner) {
        if (!state.page->inner_backing || !state.page->inner_backing->readable()) {
            return Status::corruption("cold inner has no captured reloadable backing");
        }
        result->needs_inner = {.page_id = state.pid, .range = context.range, .backing = state.page->inner_backing};
        return Status::Ok();
    }
    const auto &inner = *state.page->inner;
    if (state.page->leaf || inner.children.empty() || inner.children.size() != inner.separators.size() + 1 ||
        inner.children.size() > state.limits.max_entries || inner.children.size() > state.limits.max_work_entries ||
        !context.inner_fences.has_value()) {
        return Status::invalid_argument("final inner shape, child bound or captured fences are invalid");
    }
    if (inner.children.size() > (state.limits.frame_bytes - kPageOverhead) / sizeof(uint64_t)) {
        return Status::resource_exhausted("captured inner page needs structural split before encoding");
    }
    uint64_t used = kPageOverhead + (inner.children.size() * sizeof(uint64_t));
    for (size_t index = 0; index < inner.separators.size(); ++index) {
        const auto &key = inner.separators[index];
        if ((context.range.lower && key < *context.range.lower) ||
            (context.range.upper && key > *context.range.upper) || (index != 0 && inner.separators[index - 1] >= key)) {
            return Status::corruption("inner separators escape captured routing bounds or order");
        }
        if (key.size() > state.limits.frame_bytes - used ||
            kInnerSlotSize > state.limits.frame_bytes - used - key.size()) {
            return Status::resource_exhausted("captured inner page needs structural split before encoding");
        }
        used += key.size() + kInnerSlotSize;
    }
    for (const auto pid : inner.children) {
        if (pid >= state.next_page_id || pid == state.pid) {
            return Status::corruption("final inner child has an invalid page identity");
        }
    }
    const auto fences = context.inner_fences.value();
    if ((fences.lower_leaf == kInvalidPageId) != (fences.upper_leaf == kInvalidPageId)) {
        return Status::invalid_argument("inner encoding fences must be a complete pair");
    }
    if (fences.lower_leaf != kInvalidPageId) {
        for (const auto pid : {fences.lower_leaf, fences.upper_leaf}) {
            if (pid >= state.next_page_id || pid == state.pid) {
                return Status::invalid_argument("final inner fence has an invalid page identity");
            }
        }
    }
    state.prepared                   = true;
    state.summary.inner_fences       = fences;
    state.fences                     = fences;
    state.summary.primary_used_bytes = used;
    state.summary.encoded_entries    = inner.separators.size();
    return Status::Ok();
}
} // namespace crowdb::tree::detail
