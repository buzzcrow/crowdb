// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Server-owned byte-write alignment and partial-block cache.
#pragma once

#include "crowdb-common/mpsc_queue.h"
#include "disk/types.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace crowdb::diskio
{

class Disk;

class AlignedWriter
{
  public:
    static constexpr size_t MAX_ZERO_BYTES = 2 * 1024 * 1024;

    explicit AlignedWriter(std::string generation_journal_path = {});
    ~AlignedWriter();

    AlignedWriter(const AlignedWriter &)            = delete;
    AlignedWriter &operator=(const AlignedWriter &) = delete;

    void submit(std::shared_ptr<Disk> disk, off_t phys_offset, const uint8_t *data, size_t size,
                std::function<void(int)> on_complete);
    void submit_fenced(std::shared_ptr<Disk> disk, off_t phys_offset, const uint8_t *data, size_t size,
                       uint64_t allocation_ts, uint64_t allocation_phys_offset, std::function<void(int)> on_complete);
    void stop();

  private:
    struct Request
    {
        std::shared_ptr<Disk>    disk;
        off_t                    phys_offset;
        const uint8_t           *data;
        size_t                   size;
        uint64_t                 allocation_ts;
        uint64_t                 allocation_phys_offset;
        std::function<void(int)> on_complete;
    };

    struct CacheKey
    {
        DiskId   disk_id;
        uint64_t block_offset;

        bool operator==(const CacheKey &other) const
        {
            return disk_id == other.disk_id && block_offset == other.block_offset;
        }
    };

    struct CacheKeyHash
    {
        size_t operator()(const CacheKey &key) const;
    };

    struct CachedBlock
    {
        std::weak_ptr<Disk>  disk;
        std::vector<uint8_t> bytes;
    };

    struct AllocationKey
    {
        DiskId   disk_id;
        uint64_t phys_offset;

        bool operator==(const AllocationKey &other) const
        {
            return disk_id == other.disk_id && phys_offset == other.phys_offset;
        }
    };

    struct AllocationKeyHash
    {
        size_t operator()(const AllocationKey &key) const;
    };

    struct Shard
    {
        Shard();

        crowdb::common::MpscQueue<Request *>                           pending;
        std::atomic<bool>                                              active{false};
        std::unordered_map<CacheKey, CachedBlock, CacheKeyHash>        partial_blocks;
        std::unordered_map<AllocationKey, uint64_t, AllocationKeyHash> allocation_generations;
    };

    static constexpr size_t                         SHARD_COUNT                 = 64;
    static constexpr size_t                         MAX_CACHED_BLOCKS_PER_SHARD = 64;
    std::array<std::unique_ptr<Shard>, SHARD_COUNT> shards_;
    std::atomic<bool>                               stopping_{false};
    int                                             generation_journal_fd_{-1};
    bool                                            generation_journal_required_{false};
    std::atomic<uint64_t>                           generation_journal_offset_{0};

    size_t shard_index(const Request &request) const;
    void   try_start(size_t shard_index);
    void   process_next(size_t shard_index);
    void   process(size_t shard_index, Request *request);
    int    admit_generation(size_t shard_index, const Request &request);
    bool   append_generation(const AllocationKey &key, uint64_t allocation_ts);
    void   load_generations();
    void   submit_buffer(size_t shard_index, Request *request, std::shared_ptr<uint8_t> buffer, off_t aligned_offset,
                         size_t aligned_size, size_t block_size);
    void   finish(size_t shard_index, Request *request, int result);
    void   update_cache(Shard &shard, const Request &request, const uint8_t *buffer, off_t aligned_offset,
                        size_t aligned_size, size_t block_size);
    void   invalidate_cache(Shard &shard, const Request &request, off_t aligned_offset, size_t aligned_size,
                            size_t block_size);
};

} // namespace crowdb::diskio
