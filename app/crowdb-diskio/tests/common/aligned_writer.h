// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "disk/disk.h"
#include "engine/aligned_writer.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace crowdb::diskio::test
{

class TestRecordingEngine final : public crowdb::diskio::IoEngine
{
  public:
    explicit TestRecordingEngine(size_t capacity, bool defer_writes = false)
        : storage(capacity, 0),
          defer_writes(defer_writes)
    {
    }

    void submit_write(crowdb::diskio::Disk * /*disk*/, off_t offset, const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete) override
    {
        write_offsets.push_back(offset);
        write_sizes.push_back(size);
        if (offset < 0 || static_cast<size_t>(offset) + size > storage.size()) {
            on_complete(-1);
            return;
        }
        std::memcpy(storage.data() + offset, data, size);
        if (defer_writes) {
            pending_writes.emplace_back(size, std::move(on_complete));
        }
        else {
            on_complete(static_cast<int>(size));
        }
    }

    void submit_read(crowdb::diskio::Disk * /*disk*/, off_t offset, uint8_t *buf, size_t size,
                     uint64_t /*test_pattern_offset*/, std::function<void(int)> on_complete) override
    {
        read_count++;
        if (failed_read_offset == offset) {
            on_complete(failed_read_result);
            return;
        }
        if (offset < 0 || static_cast<size_t>(offset) + size > storage.size()) {
            on_complete(-1);
            return;
        }
        if (defer_reads) {
            pending_reads.emplace_back([this, offset, buf, size, callback = std::move(on_complete)] {
                std::memcpy(buf, storage.data() + offset, size);
                callback(static_cast<int>(size));
            });
            return;
        }
        std::memcpy(buf, storage.data() + offset, size);
        on_complete(static_cast<int>(size));
    }

    void submit_fsync(crowdb::diskio::Disk * /*disk*/, std::function<void(int)> on_complete) override
    {
        on_complete(0);
    }

    void complete_one_write()
    {
        ASSERT_FALSE(pending_writes.empty());
        auto [size, callback] = std::move(pending_writes.front());
        pending_writes.pop_front();
        callback(static_cast<int>(size));
    }

    void complete_one_read()
    {
        ASSERT_FALSE(pending_reads.empty());
        auto callback = std::move(pending_reads.front());
        pending_reads.pop_front();
        callback();
    }

    std::vector<uint8_t>                                    storage;
    bool                                                    defer_writes;
    bool                                                    defer_reads{false};
    std::deque<std::function<void()>>                       pending_reads;
    std::optional<off_t>                                    failed_read_offset;
    int                                                     failed_read_result{-EIO};
    size_t                                                  read_count{0};
    std::vector<off_t>                                      write_offsets;
    std::vector<size_t>                                     write_sizes;
    std::deque<std::pair<size_t, std::function<void(int)>>> pending_writes;
};

class TestDisk final : public crowdb::diskio::Disk
{
  public:
    TestDisk(TestRecordingEngine *engine, size_t block_size) : engine_(engine), block_size_(block_size)
    {
    }

    [[nodiscard]] crowdb::diskio::DiskType type() const override
    {
        return crowdb::diskio::DiskType::Mem;
    }

    [[nodiscard]] int fd() const override
    {
        return 1;
    }

    [[nodiscard]] bool is_o_direct() const override
    {
        return block_size_ > 1;
    }

    [[nodiscard]] size_t block_size() const override
    {
        return block_size_;
    }

    crowdb::diskio::IoEngine *engine() override
    {
        return engine_;
    }

    [[nodiscard]] crowdb::diskio::DiskId id() const override
    {
        return {.high = 7, .low = 9};
    }

    crowdb::diskio::Zone *find_zone(uint32_t /*zone_index*/) override
    {
        return nullptr;
    }

  private:
    TestRecordingEngine *engine_;
    size_t               block_size_;
};

} // namespace crowdb::diskio::test
