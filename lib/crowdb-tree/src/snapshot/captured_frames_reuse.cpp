// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "captured_frames_internal.h"
#include "primary_backing.h"

namespace crowdb::tree
{
bool CapturedFrameStream::State::reuse_primary()
{
    const auto &path     = stack.back();
    const bool  leaf     = path.page->level == 0;
    const auto  gc_floor = single_page ? single_page->gc_floor : view->gc_floor;
    if (leaf ? !can_reuse_primary(*path.page, path.pid, path.range, gc_floor, limits.reuse_backend_id,
                                  limits.encode.frame_bytes)
             : !can_reuse_inner_primary(*path.page, path.pid, path.fences, limits.reuse_backend_id,
                                        limits.encode.frame_bytes)) {
        return false;
    }
    const auto &proof = path.page->primary;
    if (limits.exclusion.overlaps(*proof->frame.backing)) {
        return false;
    }
    const auto &summary = proof->frame.summary;
    if (!summary) {
        return false;
    }
    const auto references = proof->frame.stored_overflow
                              ? std::span<const StoredOverflowReference>(*proof->frame.stored_overflow)
                              : std::span<const StoredOverflowReference>{};
    if (references.size() > limits.encode.max_entries ||
        references.size() > limits.encode.max_metadata_bytes / sizeof(ReusedOverflowDependency)) {
        finish(Status::resource_exhausted("clean primary dependency window exceeds its bound"), true);
        return true;
    }
    auto output     = detail::reserved_shared<PrimaryReuseOutput>(limits.encode.metadata, limits.encode.metadata);
    output->primary = proof;
    output->dependencies.reserve(references.size());
    for (const auto &reference : references) {
        output->dependencies.push_back({.head    = reference.head,
                                        .bytes   = reference.bytes,
                                        .owner   = proof->frame.backing,
                                        .backing = proof->frame.backing->overflow});
    }
    if (limits.plan_only) {
        report.plan = CapturedFrameResult::Plan{.page_id         = path.pid,
                                                .summary         = *summary,
                                                .reused_overflow = output->dependencies,
                                                .owner           = output,
                                                .reused_primary  = true};
    }
    else {
        report.frame = CapturedFrame{
            .info            = {.page_id = path.pid, .type = proof->frame.type, .bytes = proof->frame.frame_bytes},
            .buffer          = {},
            .reused_overflow = output->dependencies,
            .summary         = proof->frame.summary,
            .reuse           = output
        };
        ++counters.frames;
    }
    completed_page(summary->encoded_entries != 0);
    report.complete = stack.empty();
    finish(Status::Ok());
    return true;
}
} // namespace crowdb::tree
