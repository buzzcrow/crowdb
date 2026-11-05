// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_load_leaf.h"

#include "btree/allocation_bound.h"
#include "btree/page_load_internal.h"
#include "crowdb-tree/maptable/compressor.h"

#include <algorithm>
#include <numeric>

namespace crowdb::tree
{
namespace
{
struct LoadedLeafOwner
{
    std::shared_ptr<const LeafBase>   leaf;
    std::shared_ptr<const BackingRef> backing;
};

Status check_leaf_window(std::span<const uint8_t> frame, const PageLoadLimits &limits)
{
    const auto    *data        = frame.data();
    const uint32_t count       = frame_u32(data, fh::kSlotCount);
    const uint32_t deltas      = frame_u32(data, fh::kDeltaCount);
    const uint32_t delta_begin = frame_u32(data, fh::kFreeLo);
    const size_t   body        = frame.size() - kFrameTrailerSize;
    if (count > (body - kFrameHeaderSize) / kLeafSlotSize || delta_begin > body ||
        deltas > (body - delta_begin) / kLeafSlotSize) {
        return Status::corruption("loaded leaf slot directories exceed frame bounds");
    }
    if (count > limits.max_leaf_entries || deltas > limits.max_leaf_entries - count) {
        return Status::resource_exhausted("loaded leaf exceeds entry budget");
    }
    size_t key_bytes = 0;
    for (size_t i = 0; i < static_cast<size_t>(count) + deltas; ++i) {
        const auto offset =
            i < count ? kFrameHeaderSize + (i * kLeafSlotSize) : delta_begin + ((i - count) * kLeafSlotSize);
        const uint32_t size = frame_u32(data + offset, 4);
        if (size > limits.max_leaf_key_bytes - key_bytes) {
            return Status::resource_exhausted("loaded leaf exceeds key byte budget");
        }
        key_bytes += size;
    }
    return Status::Ok();
}

Status check_cells(const LeafFrameView &view, const std::optional<uint64_t> &max_slot)
{
    const size_t count = view.count();
    for (size_t i = 0; i < count + view.delta_count(); ++i) {
        const Slice raw =
            i < count ? view.cell(static_cast<uint32_t>(i)) : view.delta_cell(static_cast<uint32_t>(i - count));
        const CellView cell(raw);
        if (!cell.valid() || (cell.flags() != 0 && cell.flags() != kFlagTombstone && cell.flags() != kFlagOverflow) ||
            (cell.is_tombstone() && raw.size() != kCellHeaderSize) ||
            (cell.is_overflow() &&
             (raw.size() != kOverflowCellSize || cell.overflow_head() == kInvalidPageId || cell.overflow_len() == 0))) {
            return Status::corruption("loaded leaf contains an invalid cell");
        }
        if (max_slot && cell.slot() > *max_slot) {
            return Status::corruption("loaded leaf exceeds its proven slot bound");
        }
    }
    return Status::Ok();
}

Status fold_leaf(LoadBytes &frame, const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    const LeafFrameView view(frame.data(), static_cast<uint32_t>(frame.size()));
    if (view.delta_count() == 0) {
        return Status::Ok();
    }
    std::vector<uint32_t, detail::ReservationAllocator<uint32_t>> order(
        view.delta_count(), detail::ReservationAllocator<uint32_t>(metadata));
    std::iota(order.begin(), order.end(), 0U);
    // LeafChainCursor uses append order for equal-slot delta ties. Including
    // the index in this ordering avoids stable_sort's uncharged scratch space.
    std::sort(order.begin(), order.end(), [&view](uint32_t a, uint32_t b) {
        const int compared = view.delta_key(a).compare(view.delta_key(b));
        return compared == 0 ? a < b : compared < 0;
    });
    size_t unique = 0;
    for (const auto index : order) {
        if (unique != 0 && view.delta_key(order[unique - 1]) == view.delta_key(index)) {
            if (CellView(view.delta_cell(index)).slot() > CellView(view.delta_cell(order[unique - 1])).slot()) {
                order[unique - 1] = index;
            }
        }
        else {
            order[unique++] = index;
        }
    }
    order.resize(unique);
    LoadBytes        normalized(frame.size(), LoadBytes::allocator_type(metadata));
    LeafFrameBuilder builder(normalized.data(), static_cast<uint32_t>(normalized.size()));
    uint32_t         main  = 0;
    size_t           delta = 0;
    while (main < view.count() || delta < order.size()) {
        const bool have_main  = main < view.count();
        const bool have_delta = delta < order.size();
        int        compared   = 0;
        if (have_main && have_delta) {
            compared = view.key(main).compare(view.delta_key(order[delta]));
        }
        const bool take_main  = have_main && (!have_delta || compared <= 0);
        const bool take_delta = have_delta && (!have_main || compared >= 0);
        Slice      key        = take_main ? view.key(main) : view.delta_key(order[delta]);
        Slice      cell       = take_main ? view.cell(main) : view.delta_cell(order[delta]);
        // The main stream wins equal-slot ties, matching LeafChainCursor.
        if (take_main && take_delta && CellView(view.delta_cell(order[delta])).slot() > CellView(cell).slot()) {
            cell = view.delta_cell(order[delta]);
        }
        if (!builder.try_append_sorted(key, cell)) {
            return Status::corruption("loaded leaf records exceed frame capacity");
        }
        main += static_cast<uint32_t>(take_main);
        delta += static_cast<size_t>(take_delta);
    }
    // Keep even slot-zero tombstones: GC belongs to the captured logical view.
    builder.finish(view.self_page_id(), view.right_sibling());
    frame.swap(normalized);
    return Status::Ok();
}
} // namespace

std::optional<size_t> decoded_leaf_allocation_bound(const PageLoadLimits &limits) noexcept
{
    return detail::AllocationBound(detail::shared_allocation_bound<LoadedLeafOwner>())
        .add(detail::shared_allocation_bound<LeafBase>())
        .add(limits.max_frame_bytes)
        .value();
}

Status decode_loaded_leaf(std::span<const uint8_t> blob, const BaseRef &base, const PageLoadLimits &limits,
                          const std::shared_ptr<detail::AllocationReservation> &metadata,
                          std::shared_ptr<const LeafBase>                      *out)
{
    LoadBytes  frame{LoadBytes::allocator_type(metadata)};
    auto       status = decode_backing_frame(blob, *base.backing, limits, metadata, &frame);
    const auto length = static_cast<uint32_t>(frame.size());
    if (!status.ok()) {
        return status;
    }
    if (frame_page_type(frame.data()) != page_type::kLeafBase) {
        return Status::corruption("loaded backing is not a leaf frame");
    }
    status = check_leaf_window(frame, limits);
    if (!status.ok()) {
        return status;
    }
    if (!frame_validate(frame.data(), length) || frame_page_type(frame.data()) != page_type::kLeafBase) {
        return Status::corruption("loaded backing is not a valid leaf frame");
    }
    status = check_cells(LeafFrameView(frame.data(), length), base.max_slot);
    if (!status.ok()) {
        return status;
    }
    status = fold_leaf(frame, metadata);
    if (!status.ok()) {
        return status;
    }
    auto owner     = detail::reserved_shared<LoadedLeafOwner>(metadata);
    owner->leaf    = LeafBase::from_frame_copy_shared(frame.data(), length, metadata);
    owner->backing = base.backing;
    *out           = std::shared_ptr<const LeafBase>(owner, owner->leaf.get());
    return Status::Ok();
}
} // namespace crowdb::tree
