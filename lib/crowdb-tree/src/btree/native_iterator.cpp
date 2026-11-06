// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "snapshot/published_native.h"

namespace crowdb::tree
{
NativeFrameIterator::NativeFrameIterator(std::shared_ptr<PublishedNativeCursor> runtime)
    : runtime_(std::move(runtime)),
      facade_credit_(runtime_->metadata(), sizeof(NativeFrameIterator), AllocationKind::kMetadata)
{
}

NativeFrameIterator::~NativeFrameIterator()                                          = default;
NativeFrameIterator::NativeFrameIterator(NativeFrameIterator &&) noexcept            = default;
NativeFrameIterator &NativeFrameIterator::operator=(NativeFrameIterator &&) noexcept = default;

Status NativeFrameIterator::next(size_t max_frames, std::vector<NativeFrame> *out, bool *complete)
{
    if (!runtime_ || out == nullptr || complete == nullptr || max_frames == 0) {
        return Status::invalid_argument("native iterator requires a non-empty output batch");
    }
    // The compatibility vector is owned by the caller; engine consumers retain next_owned windows.
    out->clear();
    *complete                  = false;
    constexpr size_t kMaxBatch = 64;
    while (out->size() < std::min(max_frames, kMaxBatch) && !*complete) {
        std::optional<NativeFrameView> frame;
        auto                           status = runtime_->next(&frame, complete);
        if (!status.ok()) {
            return status;
        }
        if (frame) {
            out->push_back({.page_id      = frame->page_id,
                            .frame        = std::vector<uint8_t>(frame->bytes.begin(), frame->bytes.end()),
                            .durable_addr = kNoAddr,
                            .durable_plen = 0,
                            .inherited    = false});
        }
    }
    return Status::Ok();
}

uint64_t NativeFrameIterator::root_page_id() const
{
    return runtime_ ? runtime_->info().root_page_id : kInvalidPageId;
}

uint64_t NativeFrameIterator::at_slot() const
{
    return runtime_ ? runtime_->info().at_slot : 0;
}

uint64_t NativeFrameIterator::next_page_id() const
{
    return runtime_ ? runtime_->info().next_page_id : 0;
}

uint64_t NativeFrameIterator::subtrees_skipped() const
{
    return 0;
}

Status Crowdbtree::open_native_frame_iterator(const KeyRange *filter, std::unique_ptr<NativeFrameIterator> *out)
{
    if (out == nullptr) {
        return Status::invalid_argument("native iterator requires output");
    }
    if (filter != nullptr) {
        auto status = filter->validate();
        if (!status.ok()) {
            return status;
        }
    }
    // Native output preserves a complete recoverable graph. Range consumers filter logical entries.
    std::shared_ptr<PublishedNativeCursor> cursor;
    auto                                   status = PublishedNativeCursor::create(core_, {}, &cursor);
    if (status.ok()) {
        *out = std::unique_ptr<NativeFrameIterator>(new NativeFrameIterator(std::move(cursor)));
    }
    return status;
}
} // namespace crowdb::tree
