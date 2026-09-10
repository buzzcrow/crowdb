// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "engine/aligned_writer.h"

#include "crowdb-common/log.h"
#include "disk/disk.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <thread>

namespace crowdb::diskio
{

namespace
{
alignas(4096) const std::array<uint8_t, AlignedWriter::MAX_ZERO_BYTES> ZERO_BYTES{};
constexpr uint64_t GENERATION_RECORD_MAGIC = 0x43524F5744424746ULL;

struct GenerationRecord
{
    uint64_t magic;
    uint64_t disk_high;
    uint64_t disk_low;
    uint64_t phys_offset;
    uint64_t allocation_ts;
    uint64_t checksum;
};

uint64_t generation_checksum(const GenerationRecord &record)
{
    return record.magic ^ record.disk_high ^ record.disk_low ^ record.phys_offset ^ record.allocation_ts;
}

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
    return std::shared_ptr<uint8_t>(static_cast<uint8_t *>(memory), [](uint8_t *ptr) { ::free(ptr); });
}
} // namespace

AlignedWriter::Shard::Shard() : pending(1024)
{
}

AlignedWriter::AlignedWriter(std::string generation_journal_path)
{
    for (auto &shard : shards_) {
        shard = std::make_unique<Shard>();
    }
    generation_journal_required_ = !generation_journal_path.empty();
    if (generation_journal_required_) {
        generation_journal_fd_ = ::open(generation_journal_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0640);
        if (generation_journal_fd_ < 0) {
            CRB_LOG_ERROR("failed to open allocation generation journal: path={} errno={}", generation_journal_path,
                          errno);
        }
        else {
            load_generations();
        }
    }
}

AlignedWriter::~AlignedWriter()
{
    stop();
    if (generation_journal_fd_ >= 0) {
        ::close(generation_journal_fd_);
    }
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

size_t AlignedWriter::AllocationKeyHash::operator()(const AllocationKey &key) const
{
    uint64_t hash = key.disk_id.high ^ (key.disk_id.low * 0x9E3779B97F4A7C15ULL);
    hash ^= key.phys_offset + 0x9E3779B97F4A7C15ULL + (hash << 6) + (hash >> 2);
    return static_cast<size_t>(hash);
}

size_t AlignedWriter::shard_index(const Request &request) const
{
    uint64_t offset =
        request.allocation_ts == 0 ? static_cast<uint64_t>(request.phys_offset) : request.allocation_phys_offset;
    return AllocationKeyHash{}(AllocationKey{request.disk->id(), offset}) % SHARD_COUNT;
}

void AlignedWriter::submit(std::shared_ptr<Disk> disk, off_t phys_offset, const uint8_t *data, size_t size,
                           std::function<void(int)> on_complete)
{
    submit_fenced(std::move(disk), phys_offset, data, size, 0, static_cast<uint64_t>(phys_offset),
                  std::move(on_complete));
}

void AlignedWriter::submit_fenced(std::shared_ptr<Disk> disk, off_t phys_offset, const uint8_t *data, size_t size,
                                  uint64_t allocation_ts, uint64_t allocation_phys_offset,
                                  std::function<void(int)> on_complete)
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
    if (allocation_ts == 0 && (disk->block_size() == 1 || size == 0)) {
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
    auto  *request = new Request{std::move(disk),        phys_offset,           data, size, allocation_ts,
                                 allocation_phys_offset, std::move(on_complete)};
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
    int generation_result = admit_generation(index, *request);
    if (generation_result != 0) {
        finish(index, request, generation_result);
        return;
    }
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
    if (!is_power_of_two(block_size) || block_size < sizeof(void *) || block_size > MAX_ZERO_BYTES) {
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
                                                  if (result == static_cast<int>(request->size)) {
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
    uint8_t *bytes = buffer.get();
    for (size_t offset = 0; offset < aligned_size; offset += block_size) {
        std::memcpy(bytes + offset, ZERO_BYTES.data(), block_size);
    }

    auto    &cache = shards_[index]->partial_blocks;
    CacheKey head_key{request->disk->id(), static_cast<uint64_t>(aligned_offset)};
    auto     head = cache.find(head_key);
    if (head != cache.end() && head->second.disk.lock().get() != request->disk.get()) {
        cache.erase(head);
        head = cache.end();
    }
    if (head != cache.end()) {
        std::memcpy(bytes, head->second.bytes.data(), block_size);
    }
    if (aligned_size > block_size) {
        CacheKey tail_key{request->disk->id(), static_cast<uint64_t>(aligned_offset) + aligned_size - block_size};
        auto     tail = cache.find(tail_key);
        if (tail != cache.end()) {
            if (tail != cache.end() && tail->second.disk.lock().get() != request->disk.get()) {
                cache.erase(tail);
                tail = cache.end();
            }
            std::memcpy(bytes + aligned_size - block_size, tail->second.bytes.data(), block_size);
        }
    }
    std::memcpy(bytes + offset_in_block, request->data, request->size);

    if (offset_in_block == 0 || head != cache.end()) {
        submit_buffer(index, request, std::move(buffer), aligned_offset, aligned_size, block_size);
        return;
    }

    CRB_LOG_WARN("unaligned disk write cache miss; recovering block: disk_high={} disk_low={} offset={} block_size={}",
                 request->disk->id().high, request->disk->id().low, request->phys_offset, block_size);
    request->disk->engine()->submit_read(
        request->disk.get(), aligned_offset, bytes, block_size, static_cast<uint64_t>(aligned_offset),
        [this, index, request, buffer = std::move(buffer), aligned_offset, aligned_size, block_size](int result) {
            if (result != static_cast<int>(block_size)) {
                finish(index, request, result < 0 ? result : -EIO);
                return;
            }
            std::memcpy(buffer.get() + (request->phys_offset - aligned_offset), request->data, request->size);
            submit_buffer(index, request, buffer, aligned_offset, aligned_size, block_size);
        });
}

int AlignedWriter::admit_generation(size_t index, const Request &request)
{
    if (request.allocation_ts == 0) {
        return 0;
    }
    AllocationKey key{request.disk->id(), request.allocation_phys_offset};
    auto         &generations = shards_[index]->allocation_generations;
    auto          current     = generations.find(key);
    if (current != generations.end() && request.allocation_ts < current->second) {
        return -ESTALE;
    }
    if (current != generations.end() && request.allocation_ts == current->second) {
        return 0;
    }
    if (!append_generation(key, request.allocation_ts)) {
        return -EIO;
    }
    generations[key] = request.allocation_ts;
    return 0;
}

bool AlignedWriter::append_generation(const AllocationKey &key, uint64_t allocation_ts)
{
    if (!generation_journal_required_) {
        return true;
    }
    if (generation_journal_fd_ < 0) {
        return false;
    }
    GenerationRecord record{GENERATION_RECORD_MAGIC, key.disk_id.high, key.disk_id.low,
                            key.phys_offset,         allocation_ts,    0};
    record.checksum  = generation_checksum(record);
    uint64_t offset  = generation_journal_offset_.fetch_add(sizeof(record), std::memory_order_acq_rel);
    ssize_t  written = ::pwrite(generation_journal_fd_, &record, sizeof(record), static_cast<off_t>(offset));
    if (written != static_cast<ssize_t>(sizeof(record)) || ::fdatasync(generation_journal_fd_) != 0) {
        CRB_LOG_ERROR("failed to persist allocation generation: disk_high={} disk_low={} offset={} generation={} "
                      "errno={}",
                      key.disk_id.high, key.disk_id.low, key.phys_offset, allocation_ts, errno);
        return false;
    }
    return true;
}

void AlignedWriter::load_generations()
{
    off_t end = ::lseek(generation_journal_fd_, 0, SEEK_END);
    if (end < 0) {
        CRB_LOG_ERROR("failed to size allocation generation journal: errno={}", errno);
        ::close(generation_journal_fd_);
        generation_journal_fd_ = -1;
        return;
    }
    generation_journal_offset_.store(static_cast<uint64_t>(end), std::memory_order_release);
    for (off_t offset = 0; offset + static_cast<off_t>(sizeof(GenerationRecord)) <= end;
         offset += static_cast<off_t>(sizeof(GenerationRecord))) {
        GenerationRecord record{};
        if (::pread(generation_journal_fd_, &record, sizeof(record), offset) != static_cast<ssize_t>(sizeof(record)) ||
            record.magic != GENERATION_RECORD_MAGIC || record.checksum != generation_checksum(record)) {
            CRB_LOG_WARN("skipping invalid allocation generation record: offset={}", offset);
            continue;
        }
        AllocationKey key{
            {record.disk_high, record.disk_low},
            record.phys_offset
        };
        size_t index       = AllocationKeyHash{}(key) % SHARD_COUNT;
        auto  &generations = shards_[index]->allocation_generations;
        auto   current     = generations.find(key);
        if (current == generations.end() || record.allocation_ts > current->second) {
            generations[key] = record.allocation_ts;
        }
    }
}

void AlignedWriter::submit_buffer(size_t index, Request *request, std::shared_ptr<uint8_t> buffer, off_t aligned_offset,
                                  size_t aligned_size, size_t block_size)
{
    uint8_t *bytes = buffer.get();
    request->disk->engine()->submit_write(
        request->disk.get(), aligned_offset, bytes, aligned_size,
        [this, index, request, buffer = std::move(buffer), aligned_offset, aligned_size, block_size](int result) {
            auto &shard = *shards_[index];
            if (result == static_cast<int>(aligned_size)) {
                update_cache(shard, *request, buffer.get(), aligned_offset, aligned_size, block_size);
                finish(index, request, static_cast<int>(request->size));
                return;
            }
            invalidate_cache(shard, *request, aligned_offset, aligned_size, block_size);
            finish(index, request, result < 0 ? result : -EIO);
        });
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
    shard.partial_blocks[CacheKey{request.disk->id(), tail_offset}] =
        CachedBlock{request.disk, std::vector<uint8_t>(buffer + aligned_size - block_size, buffer + aligned_size)};
}

void AlignedWriter::invalidate_cache(Shard &shard, const Request &request, off_t aligned_offset, size_t aligned_size,
                                     size_t block_size)
{
    uint64_t block_offset = static_cast<uint64_t>(aligned_offset);
    uint64_t end          = block_offset + aligned_size;
    while (block_offset < end) {
        shard.partial_blocks.erase(CacheKey{request.disk->id(), block_offset});
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
