// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "native_import.h"
#include "runtime_range_rebuild.h"
#include "snapshot/published_native.h"
#include "tree_core_access.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
Status measure_native(PublishedNativeCursor &cursor, NativeImportShape *out)
{
    NativeImportShape shape;
    size_t            overflow_frames = 0;
    bool              complete        = false;
    while (!complete) {
        std::optional<NativeFrameView> frame;
        auto                           status = cursor.next(&frame, &complete);
        if (!status.ok()) {
            return status;
        }
        if (!frame) {
            continue;
        }
        if (shape.frames++ == NativeImportLimits{}.max_frames) {
            return Status::resource_exhausted("range native frame limit");
        }
        shape.max_frame_bytes = std::max(shape.max_frame_bytes, frame->bytes.size());
        const auto type       = frame_page_type(frame->bytes.data());
        if (type == page_type::kOverflowFrame) {
            ++overflow_frames;
            continue;
        }
        ++shape.primary_pages;
        shape.max_key_bytes = std::max(shape.max_key_bytes, frame->bytes.size());
        if (type == page_type::kLeafBase) {
            ++shape.leaf_pages;
            const LeafFrameView leaf(frame->bytes.data(), frame->bytes.size());
            const size_t        count = static_cast<size_t>(leaf.count()) + leaf.delta_count();
            shape.max_leaf_entries    = std::max(shape.max_leaf_entries, count);
            for (size_t i = 0; i < count; ++i) {
                const CellView cell(i < leaf.count() ? leaf.cell(static_cast<uint32_t>(i))
                                                     : leaf.delta_cell(static_cast<uint32_t>(i - leaf.count())));
                shape.stored_references += static_cast<size_t>(cell.is_overflow());
            }
        }
    }
    // Each stored reference can visit at most all emitted overflow frames.
    // Streams beyond the import's configured work limit are rejected later by
    // its graph validator; no extra dependency allocation can occur past it.
    const auto dependencies = detail::AllocationBound().add_product(shape.stored_references, overflow_frames).value();
    if (!dependencies) {
        return Status::resource_exhausted("range dependency bound overflow");
    }
    shape.dependency_steps = std::min(*dependencies, NativeImportLimits{}.max_dependency_steps);
    *out                   = shape;
    return cursor.rewind();
}

uint64_t native_leaf_keys(const NativeFrameView &frame, const std::shared_ptr<detail::AllocationReservation> &credit)
{
    if (frame_page_type(frame.bytes.data()) != page_type::kLeafBase) {
        return 0;
    }
    const LeafFrameView           leaf(frame.bytes.data(), frame.bytes.size());
    detail::ReservedVector<Slice> keys{detail::ReservationAllocator<Slice>(credit)};
    keys.reserve(static_cast<size_t>(leaf.count()) + leaf.delta_count());
    for (uint32_t i = 0; i < leaf.count(); ++i) {
        keys.push_back(leaf.key(i));
    }
    for (uint32_t i = 0; i < leaf.delta_count(); ++i) {
        keys.push_back(leaf.delta_key(i));
    }
    std::sort(keys.begin(), keys.end());
    return std::unique(keys.begin(), keys.end()) - keys.begin();
}
} // namespace

Status copy_native_range(Crowdbtree &source, TreeCore &destination, RuntimeRangeImport &staging)
{
    std::shared_ptr<PublishedNativeCursor> cursor;
    auto status = PublishedNativeCursor::create(TreeCoreAccess::get(source)->shared_from_this(), {}, &cursor);
    if (!status.ok()) {
        return status;
    }
    NativeImportShape shape;
    status = measure_native(*cursor, &shape);
    if (!status.ok()) {
        return status;
    }
    shape.max_frame_bytes = std::max<size_t>(shape.max_frame_bytes, destination.config().frame_bytes);
    status                = reserve_range_shape(destination, shape, false, staging);
    if (!status.ok()) {
        return status;
    }
    const auto info               = cursor->info();
    staging.catalog               = detail::reserved_shared<ReadablePageCatalog>(staging.credit, staging.credit);
    staging.catalog->root_page_id = info.root_page_id;
    staging.catalog->coverage     = info.at_slot;
    staging.catalog->generation   = 1;
    staging.catalog->next_page_id = info.next_page_id;
    bool complete                 = false;
    while (!complete) {
        std::optional<NativeFrameView> frame;
        status = cursor->next(&frame, &complete);
        if (!status.ok()) {
            return status;
        }
        if (!frame) {
            continue;
        }
        status = append_native_frame(*staging.catalog, frame->page_id, frame->bytes, destination.config(),
                                     destination.load_limits(), {}, staging.credit);
        if (!status.ok()) {
            return status;
        }
        staging.stats.entries_examined += native_leaf_keys(*frame, staging.credit);
        ++staging.stats.pages_rebuilt;
    }
    if (staging.catalog->frames.size() != info.frame_count) {
        return Status::corruption("range rebuild native stream count mismatch");
    }
    staging.stats.entries_emitted = staging.stats.entries_examined;
    return attach_native_dependencies(*staging.catalog, {}, staging.credit);
}
} // namespace crowdb::tree
