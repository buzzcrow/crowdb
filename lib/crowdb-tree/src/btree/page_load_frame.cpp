// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/maptable/compressor.h"
#include "page_load_internal.h"

#include <algorithm>

namespace crowdb::tree
{
Status decode_backing_frame(std::span<const uint8_t> blob, const BackingRef &backing, const PageLoadLimits &limits,
                            const std::shared_ptr<AllocationBudget> &metadata, LoadBytes *out)
{
    if (!backing.readable()) {
        return Status::corruption("backing has neither an extent nor an owned resident frame");
    }
    const bool   resident = backing.storage == BackingStorage::kResidentFrame;
    const size_t length   = resident ? backing.resident_frame.size() : durable_blob_raw_len(blob.data(), blob.size());
    if (length <= kFrameHeaderSize + kFrameTrailerSize || length > limits.max_frame_bytes || length > UINT32_MAX) {
        return Status::corruption("backing frame exceeds declared bounds");
    }
    LoadBytes frame(length, LoadBytes::allocator_type(metadata));
    if (resident) {
        std::copy(backing.resident_frame.begin(), backing.resident_frame.end(), frame.begin());
    }
    else {
        auto status = decode_durable_page(blob.data(), blob.size(), frame.data(), static_cast<uint32_t>(length));
        if (!status.ok()) {
            return status;
        }
    }
    record_work(metadata, TreeWorkCounter::MaterializeCopyBytes, length);
    out->swap(frame);
    return Status::Ok();
}
} // namespace crowdb::tree
