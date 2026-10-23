// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/disk.h"
#include "engine/aligned_writer.h"

#include <cerrno>
#include <cstring>
#include <utility>

namespace crowdb::diskio
{

void AlignedWriter::merge_boundaries(size_t index, Request *request, MergeBuffer buffer, size_t boundary)
{
    const bool partial_head = request->phys_offset != buffer.offset;
    const bool partial_tail = static_cast<uint64_t>(request->phys_offset) + request->size !=
                              static_cast<uint64_t>(buffer.offset) + buffer.size;
    for (; boundary < 2; ++boundary) {
        const bool needed = boundary == 0 ? partial_head || (partial_tail && buffer.size == buffer.block_size)
                                          : partial_tail && buffer.size > buffer.block_size;
        if (!needed) {
            continue;
        }
        const size_t offset   = boundary == 0 ? 0 : buffer.size - buffer.block_size;
        const auto   position = buffer.offset + static_cast<off_t>(offset);
        auto        &cache    = shards_[index]->partial_blocks;
        auto         cached =
            cache.find(CacheKey{.disk_id = request->disk->id(), .block_offset = static_cast<uint64_t>(position)});
        if (cached != cache.end() && cached->second.disk.lock().get() != request->disk.get()) {
            cache.erase(cached);
            cached = cache.end();
        }
        if (cached != cache.end()) {
            std::memcpy(buffer.bytes.get() + offset, cached->second.bytes.data(), buffer.block_size);
            continue;
        }
        auto        *destination = buffer.bytes.get() + offset;
        const size_t block_size  = buffer.block_size;
        request->disk->engine()->submit_read(
            request->disk.get(), position, destination, block_size, static_cast<uint64_t>(position),
            [this, index, request, buffer = std::move(buffer), boundary, block_size](int result) mutable {
                if (std::cmp_not_equal(result, block_size)) {
                    finish(index, request, result < 0 ? result : -EIO);
                    return;
                }
                merge_boundaries(index, request, std::move(buffer), boundary + 1);
            });
        return;
    }
    std::memcpy(buffer.bytes.get() + (request->phys_offset - buffer.offset), request->data, request->size);
    submit_buffer(index, request, std::move(buffer));
}

void AlignedWriter::submit_buffer(size_t index, Request *request, MergeBuffer buffer)
{
    auto        *bytes  = buffer.bytes.get();
    const off_t  offset = buffer.offset;
    const size_t size   = buffer.size;
    request->disk->engine()->submit_write(
        request->disk.get(), offset, bytes, size, [this, index, request, buffer = std::move(buffer)](int result) {
            auto &shard = *shards_[index];
            if (std::cmp_equal(result, buffer.size)) {
                update_cache(shard, *request, buffer.bytes.get(), buffer.offset, buffer.size, buffer.block_size);
                finish(index, request, static_cast<int>(request->size));
                return;
            }
            invalidate_cache(shard, *request, buffer.offset, buffer.size, buffer.block_size);
            finish(index, request, result < 0 ? result : -EIO);
        });
}

} // namespace crowdb::diskio
