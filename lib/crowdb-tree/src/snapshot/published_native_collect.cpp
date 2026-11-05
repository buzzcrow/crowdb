// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_native_collect.h"

#include "btree/allocation_bound.h"
#include "btree/tree_core.h"

namespace crowdb::tree
{
namespace
{
struct CopiedFrameOwner
{
    CopiedFrameOwner(std::shared_ptr<detail::AllocationReservation> metadata, size_t capacity)
        : bytes(std::move(metadata), capacity, AllocationKind::kFrame)
    {
    }

    AllocationLease bytes;
};

Status measure_frames(PublishedNativeCursor &cursor, size_t max_bytes, size_t *bytes)
{
    detail::AllocationBound bound(detail::shared_allocation_bound<detail::AllocationReservation>());
    bound.add_product(cursor.info().frame_count,
                      (sizeof(NativeFrame) * 2) + detail::shared_allocation_bound<CopiedFrameOwner>());
    if (!bound.value() || *bound.value() > max_bytes) {
        return Status::resource_exhausted("native compatibility result metadata exceeds its capacity limit");
    }
    bool done = false;
    while (!done) {
        std::optional<NativeFrameView> frame;
        auto                           status = cursor.next(&frame, &done);
        if (!status.ok()) {
            return status;
        }
        if (frame) {
            bound.add(frame->bytes.size());
            if (!bound.value() || *bound.value() > max_bytes) {
                return Status::resource_exhausted("native compatibility result exceeds its capacity limit");
            }
        }
    }
    *bytes = *bound.value();
    return cursor.rewind();
}

Status copy_frames(PublishedNativeCursor &cursor, const std::shared_ptr<detail::AllocationReservation> &metadata,
                   std::vector<NativeFrame> *result)
{
    bool done = false;
    while (!done) {
        std::optional<NativeFrameView> frame;
        auto                           status = cursor.next(&frame, &done);
        if (!status.ok()) {
            return status;
        }
        if (!frame) {
            continue;
        }
        if (result->size() == cursor.info().frame_count) {
            return Status::corruption("native compatibility result exceeds its planned count");
        }
        NativeFrame value{.page_id      = frame->page_id,
                          .frame        = std::vector<uint8_t>(frame->bytes.begin(), frame->bytes.end()),
                          .durable_addr = kNoAddr,
                          .durable_plen = 0,
                          .inherited    = false};
        value.allocation_owner = detail::reserved_shared<CopiedFrameOwner>(metadata, metadata, value.frame.capacity());
        result->push_back(std::move(value));
    }
    return result->size() == cursor.info().frame_count
             ? Status::Ok()
             : Status::corruption("native compatibility result omits a planned frame");
}
} // namespace

Status collect_published_frames(const std::shared_ptr<TreeCore> &core, size_t max_bytes, std::vector<NativeFrame> *out,
                                NativeStreamInfo *info)
try {
    if (!core || max_bytes == 0 || out == nullptr || info == nullptr) {
        return Status::invalid_argument("native compatibility collection requires bounded inputs and outputs");
    }
    std::shared_ptr<PublishedNativeCursor> cursor;
    auto                                   status = PublishedNativeCursor::create(core, {}, &cursor);
    if (!status.ok()) {
        return status;
    }
    size_t bytes = 0;
    status       = measure_frames(*cursor, max_bytes, &bytes);
    if (!status.ok()) {
        return status;
    }
    auto credit = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kRouting, bytes);
    if (!credit) {
        return Status::resource_exhausted("native compatibility result reserve unavailable");
    }
    auto                     metadata = detail::AllocationReservation::create(std::move(*credit));
    AllocationLease          container;
    std::vector<NativeFrame> result;
    result.reserve(cursor->info().frame_count);
    container = AllocationLease(metadata, result.capacity() * sizeof(NativeFrame), AllocationKind::kMetadata);
    status    = copy_frames(*cursor, metadata, &result);
    if (!status.ok()) {
        return status;
    }
    metadata->seal();
    *info = cursor->info();
    out->swap(result);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("native compatibility collection allocation failed");
}
} // namespace crowdb::tree
