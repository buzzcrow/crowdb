// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_page_store.h"

#include <algorithm>
#include <cstring>

namespace crowdb::tree::detail
{

Status ChunkPageStore::prepare_staged_write(uint64_t offset, size_t length)
{
    if (staged_base_ == nullptr || length == 0) {
        return Status::Ok();
    }
    const uint64_t end = offset + length;
    for (const auto &pack : staged_base_->packs) {
        const uint64_t pack_end = pack.logical_offset + pack.ref.length;
        if (pack_end <= offset || pack.logical_offset >= end || (offset <= pack.logical_offset && end >= pack_end) ||
            staged_.covers(pack.logical_offset, pack.ref.length)) {
            continue;
        }
        std::shared_ptr<const std::vector<uint8_t>> persisted;
        Status                                      status = read_pack(pack.ref, &persisted, {});
        if (!status.ok()) {
            return status;
        }
        std::vector<uint8_t> captured(*persisted);
        staged_.overlay(pack.logical_offset, captured);
        staged_.write(pack.logical_offset, captured);
    }
    return Status::Ok();
}

Status ChunkPageStore::read_staged_range(uint64_t offset, size_t length, std::vector<uint8_t> *out,
                                         ChunkCancellation cancellation) const
{
    if (offset > staged_.size() || length > staged_.size() - offset) {
        return Status::corruption("staged chunk range exceeds logical image");
    }
    out->assign(length, 0);
    if (staged_base_ != nullptr) {
        const uint64_t end = offset + length;
        for (const auto &pack : staged_base_->packs) {
            const uint64_t begin = std::max(offset, pack.logical_offset);
            const uint64_t stop  = std::min(end, pack.logical_offset + pack.ref.length);
            if (begin >= stop || staged_.covers(begin, stop - begin)) {
                continue;
            }
            if (cancellation.cancelled()) {
                return Status::unavailable("chunk staging read cancelled");
            }
            std::shared_ptr<const std::vector<uint8_t>> payload;
            Status                                      status = read_pack(pack.ref, &payload, cancellation);
            if (!status.ok()) {
                return status;
            }
            std::memcpy(out->data() + begin - offset, payload->data() + begin - pack.logical_offset, stop - begin);
        }
    }
    staged_.overlay(offset, *out);
    return cancellation.cancelled() ? Status::unavailable("chunk staging read cancelled") : Status::Ok();
}

const ChunkPagePack *ChunkPageStore::find_reusable_pack(const ChunkManifest &base, uint64_t logical_offset,
                                                        uint32_t length, ChunkCancellation cancellation) const
{
    const ChunkPagePack *found = find_pack_at(base, logical_offset, length);
    if (found == nullptr) {
        return nullptr;
    }
    std::shared_ptr<const std::vector<uint8_t>> persisted;
    Status                                      status = read_pack(found->ref, &persisted, cancellation);
    if (!status.ok() || persisted->size() != length) {
        return nullptr;
    }
    if (!range_was_written(logical_offset, length)) {
        return found;
    }
    std::vector<uint8_t> overlaid(*persisted);
    staged_.overlay(logical_offset, overlaid);
    return overlaid == *persisted ? found : nullptr;
}

} // namespace crowdb::tree::detail
