// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "engine/aligned_writer.h"

#include "disk/disk.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <thread>
#include <utility>

namespace crowdb::diskio
{

namespace
{
bool is_power_of_two(size_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

std::shared_ptr<uint8_t> allocate_aligned(size_t alignment, size_t size)
{
    void *memory = nullptr;
    if (::posix_memalign(&memory, alignment, size) != 0) {
        return {};
    }
    return {static_cast<uint8_t *>(memory), [](uint8_t *ptr) { ::free(ptr); }};
}
} // namespace

AlignedWriter::Shard::Shard() : pending(1024)
{
}

AlignedWriter::AlignedWriter()
{
    for (auto &shard : shards_) {
        shard = std::make_unique<Shard>();
    }
}

AlignedWriter::~AlignedWriter()
{
    stop();
}

void AlignedWriter::stop()
{
    stopping_.store(true, std::memory_order_release);
    for (auto &shard : shards_) {
        try_start(static_cast<size_t>(&shard - shards_.data()));
    }
    for (const auto &shard : shards_) {
        while (shard->active.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
}

size_t AlignedWriter::CacheKeyHash::operator()(const CacheKey &key) const
{
    uint64_t hash = key.disk_id.high ^ (key.disk_id.low * 0x9E3779B97F4A7C15ULL);
    hash ^= key.block_offset + 0x9E3779B97F4A7C15ULL + (hash << 6) + (hash >> 2);
    return static_cast<size_t>(hash);
}

size_t AlignedWriter::shard_index(const Request &request) const
{
    return CacheKeyHash{}(CacheKey{.disk_id = request.disk->id(), .block_offset = request.ordering_phys_offset}) %
           SHARD_COUNT;
}

void AlignedWriter::submit(std::shared_ptr<Disk> disk, off_t phys_offset, const uint8_t *data, size_t size,
                           std::function<void(int)> on_complete)
{
    submit_ordered(std::move(disk), phys_offset, data, size, static_cast<uint64_t>(phys_offset),
                   std::move(on_complete));
}

void AlignedWriter::submit_ordered(std::shared_ptr<Disk> disk, off_t phys_offset, const uint8_t *data, size_t size,
                                   uint64_t ordering_phys_offset, std::function<void(int)> on_complete)
{
    if (stopping_.load(std::memory_order_acquire)) {
        if (on_complete) {
            on_complete(-ECANCELED);
        }
        return;
    }
    if (disk == nullptr || disk->fd() < 0 || phys_offset < 0 || (size != 0 && data == nullptr)) {
        if (on_complete) {
            on_complete(-EINVAL);
        }
        return;
    }
    if (disk->block_size() == 1 || size == 0) {
        Disk     *disk_ptr = disk.get();
        IoEngine *engine   = disk_ptr->engine();
        engine->submit_write(disk_ptr, phys_offset, data, size,
                             [disk = std::move(disk), callback = std::move(on_complete)](int result) mutable {
                                 if (callback) {
                                     callback(result);
                                 }
                             });
        return;
    }
    auto  *request = new Request{.disk                 = std::move(disk),
                                 .phys_offset          = phys_offset,
                                 .data                 = data,
                                 .size                 = size,
                                 .ordering_phys_offset = ordering_phys_offset,
                                 .on_complete          = std::move(on_complete)};
    size_t index   = shard_index(*request);
    if (!shards_[index]->pending.try_push(request)) {
        if (request->on_complete) {
            request->on_complete(-EAGAIN);
        }
        delete request;
        return;
    }
    try_start(index);
}

void AlignedWriter::try_start(size_t index)
{
    if (!shards_[index]->active.exchange(true, std::memory_order_acq_rel)) {
        process_next(index);
    }
}

void AlignedWriter::process_next(size_t index)
{
    auto    &shard   = *shards_[index];
    Request *request = nullptr;
    if (stopping_.load(std::memory_order_acquire)) {
        while (shard.pending.drain(&request, 1) == 1) {
            if (request->on_complete) {
                request->on_complete(-ECANCELED);
            }
            delete request;
        }
        shard.active.store(false, std::memory_order_release);
        return;
    }
    if (shard.pending.drain(&request, 1) == 1) {
        process(index, request);
        return;
    }
    shard.active.store(false, std::memory_order_release);
    if (shard.pending.has_pending()) {
        try_start(index);
    }
}

void AlignedWriter::process(size_t index, Request *request)
{
    size_t block_size = request->disk->block_size();
    if (block_size == 0) {
        finish(index, request, -EINVAL);
        return;
    }
    if (block_size == 1 || request->size == 0) {
        request->disk->engine()->submit_write(request->disk.get(), request->phys_offset, request->data, request->size,
                                              [this, index, request](int result) { finish(index, request, result); });
        return;
    }
    if (!is_power_of_two(block_size) || block_size < sizeof(void *) || block_size > MAX_BLOCK_BYTES) {
        finish(index, request, -EINVAL);
        return;
    }
    size_t offset_in_block = static_cast<size_t>(request->phys_offset) % block_size;
    if (request->size > std::numeric_limits<size_t>::max() - offset_in_block) {
        finish(index, request, -EOVERFLOW);
        return;
    }
    size_t covered = offset_in_block + request->size;
    if (covered > std::numeric_limits<size_t>::max() - (block_size - 1)) {
        finish(index, request, -EOVERFLOW);
        return;
    }
    size_t aligned_size    = ((covered + block_size - 1) / block_size) * block_size;
    off_t  aligned_offset  = request->phys_offset - static_cast<off_t>(offset_in_block);
    bool   pointer_aligned = reinterpret_cast<uintptr_t>(request->data) % block_size == 0;
    if (offset_in_block == 0 && request->size % block_size == 0 && pointer_aligned) {
        request->disk->engine()->submit_write(request->disk.get(), request->phys_offset, request->data, request->size,
                                              [this, index, request, block_size](int result) {
                                                  if (std::cmp_equal(result, request->size)) {
                                                      invalidate_cache(*shards_[index], *request, request->phys_offset,
                                                                       request->size, block_size);
                                                  }
                                                  finish(index, request, result);
                                              });
        return;
    }

    auto buffer = allocate_aligned(block_size, aligned_size);
    if (!buffer) {
        finish(index, request, -ENOMEM);
        return;
    }
    merge_boundaries(
        index, request,
        MergeBuffer{
            .bytes = std::move(buffer), .offset = aligned_offset, .size = aligned_size, .block_size = block_size},
        0);
}

void AlignedWriter::update_cache(Shard &shard, const Request &request, const uint8_t *buffer, off_t aligned_offset,
                                 size_t aligned_size, size_t block_size)
{
    uint64_t logical_end = static_cast<uint64_t>(request.phys_offset) + request.size;
    invalidate_cache(shard, request, aligned_offset, aligned_size, block_size);
    if (logical_end % block_size == 0) {
        return;
    }
    if (shard.partial_blocks.size() >= MAX_CACHED_BLOCKS_PER_SHARD) {
        shard.partial_blocks.erase(shard.partial_blocks.begin());
    }
    uint64_t tail_offset = static_cast<uint64_t>(aligned_offset) + aligned_size - block_size;
    shard.partial_blocks[CacheKey{.disk_id = request.disk->id(), .block_offset = tail_offset}] = CachedBlock{
        .disk = request.disk, .bytes = std::vector<uint8_t>(buffer + aligned_size - block_size, buffer + aligned_size)};
}

void AlignedWriter::invalidate_cache(Shard &shard, const Request &request, off_t aligned_offset, size_t aligned_size,
                                     size_t block_size)
{
    auto     block_offset = static_cast<uint64_t>(aligned_offset);
    uint64_t end          = block_offset + aligned_size;
    while (block_offset < end) {
        shard.partial_blocks.erase(CacheKey{.disk_id = request.disk->id(), .block_offset = block_offset});
        block_offset += block_size;
    }
}

void AlignedWriter::finish(size_t index, Request *request, int result)
{
    if (request->on_complete) {
        request->on_complete(result);
    }
    delete request;
    process_next(index);
}

} // namespace crowdb::diskio
