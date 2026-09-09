// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/disk.h"
#include "engine/aligned_writer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <thread>
#include <utility>
#include <vector>

namespace
{

class RecordingEngine final : public crowdb::diskio::IoEngine
{
  public:
    explicit RecordingEngine(size_t capacity, bool defer_writes = false)
        : storage(capacity, 0),
          defer_writes(defer_writes)
    {
    }

    void submit_write(crowdb::diskio::Disk *, off_t offset, const uint8_t *data, size_t size,
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

    void submit_read(crowdb::diskio::Disk *, off_t offset, uint8_t *buf, size_t size, uint64_t,
                     std::function<void(int)> on_complete) override
    {
        read_count++;
        if (offset < 0 || static_cast<size_t>(offset) + size > storage.size()) {
            on_complete(-1);
            return;
        }
        std::memcpy(buf, storage.data() + offset, size);
        on_complete(static_cast<int>(size));
    }

    void submit_fsync(crowdb::diskio::Disk *, std::function<void(int)> on_complete) override
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

    std::vector<uint8_t>                                    storage;
    bool                                                    defer_writes;
    size_t                                                  read_count{0};
    std::vector<off_t>                                      write_offsets;
    std::vector<size_t>                                     write_sizes;
    std::deque<std::pair<size_t, std::function<void(int)>>> pending_writes;
};

class TestDisk final : public crowdb::diskio::Disk
{
  public:
    TestDisk(RecordingEngine *engine, size_t block_size) : engine_(engine), block_size_(block_size)
    {
    }

    crowdb::diskio::DiskType type() const override
    {
        return crowdb::diskio::DiskType::Mem;
    }

    int fd() const override
    {
        return 1;
    }

    bool is_o_direct() const override
    {
        return block_size_ > 1;
    }

    size_t block_size() const override
    {
        return block_size_;
    }

    crowdb::diskio::IoEngine *engine() override
    {
        return engine_;
    }

    crowdb::diskio::DiskId id() const override
    {
        return {7, 9};
    }

    crowdb::diskio::Zone *find_zone(uint32_t) override
    {
        return nullptr;
    }

  private:
    RecordingEngine *engine_;
    size_t           block_size_;
};

TEST(AlignedWriter, BlockSizeOneUsesNoOpPath)
{
    RecordingEngine               engine(4096);
    auto                          disk = std::make_shared<TestDisk>(&engine, 1);
    crowdb::diskio::AlignedWriter writer;
    std::vector<uint8_t>          data{1, 2, 3};
    int                           result = -1;

    writer.submit(disk, 17, data.data(), data.size(), [&](int value) { result = value; });

    EXPECT_EQ(result, 3);
    ASSERT_EQ(engine.write_sizes.size(), 1);
    EXPECT_EQ(engine.write_offsets[0], 17);
    EXPECT_EQ(engine.write_sizes[0], 3);
}

TEST(AlignedWriter, SequentialPartialWritesReuseCachedBlock)
{
    RecordingEngine               engine(8192);
    auto                          disk = std::make_shared<TestDisk>(&engine, 4096);
    crowdb::diskio::AlignedWriter writer;
    std::vector<uint8_t>          first(1024, 0x11);
    std::vector<uint8_t>          second(1024, 0x22);
    int                           first_result  = -1;
    int                           second_result = -1;

    writer.submit(disk, 0, first.data(), first.size(), [&](int value) { first_result = value; });
    writer.submit(disk, 1024, second.data(), second.size(), [&](int value) { second_result = value; });

    EXPECT_EQ(first_result, 1024);
    EXPECT_EQ(second_result, 1024);
    EXPECT_EQ(engine.read_count, 0);
    ASSERT_EQ(engine.write_sizes.size(), 2);
    EXPECT_EQ(engine.write_sizes[0], 4096);
    EXPECT_EQ(engine.write_sizes[1], 4096);
    EXPECT_TRUE(std::all_of(engine.storage.begin(), engine.storage.begin() + 1024,
                            [](uint8_t value) { return value == 0x11; }));
    EXPECT_TRUE(std::all_of(engine.storage.begin() + 1024, engine.storage.begin() + 2048,
                            [](uint8_t value) { return value == 0x22; }));
}

TEST(AlignedWriter, CacheMissReadsAndMergesExistingBlock)
{
    RecordingEngine engine(8192);
    std::fill(engine.storage.begin(), engine.storage.begin() + 4096, 0x7a);
    auto                          disk = std::make_shared<TestDisk>(&engine, 4096);
    crowdb::diskio::AlignedWriter writer;
    std::vector<uint8_t>          data(1024, 0x33);
    int                           result = -1;

    writer.submit(disk, 1024, data.data(), data.size(), [&](int value) { result = value; });

    EXPECT_EQ(result, 1024);
    EXPECT_EQ(engine.read_count, 1);
    EXPECT_TRUE(std::all_of(engine.storage.begin(), engine.storage.begin() + 1024,
                            [](uint8_t value) { return value == 0x7a; }));
    EXPECT_TRUE(std::all_of(engine.storage.begin() + 1024, engine.storage.begin() + 2048,
                            [](uint8_t value) { return value == 0x33; }));
    EXPECT_TRUE(std::all_of(engine.storage.begin() + 2048, engine.storage.begin() + 4096,
                            [](uint8_t value) { return value == 0x7a; }));
}

TEST(AlignedWriter, FullAlignedWriteInvalidatesPartialCache)
{
    RecordingEngine               engine(8192);
    auto                          disk = std::make_shared<TestDisk>(&engine, 4096);
    crowdb::diskio::AlignedWriter writer;
    std::vector<uint8_t>          partial(1024, 0x11);
    std::vector<uint8_t>          continuation(1024, 0x22);
    void                         *allocation = nullptr;
    ASSERT_EQ(::posix_memalign(&allocation, 4096, 4096), 0);
    auto aligned = std::unique_ptr<uint8_t, decltype(&::free)>(static_cast<uint8_t *>(allocation), &::free);
    std::memset(aligned.get(), 0x44, 4096);

    writer.submit(disk, 0, partial.data(), partial.size(), [](int result) { ASSERT_EQ(result, 1024); });
    writer.submit(disk, 0, aligned.get(), 4096, [](int result) { ASSERT_EQ(result, 4096); });
    writer.submit(disk, 1024, continuation.data(), continuation.size(), [](int result) { ASSERT_EQ(result, 1024); });

    EXPECT_EQ(engine.read_count, 1);
    EXPECT_TRUE(std::all_of(engine.storage.begin(), engine.storage.begin() + 1024,
                            [](uint8_t value) { return value == 0x44; }));
    EXPECT_TRUE(std::all_of(engine.storage.begin() + 1024, engine.storage.begin() + 2048,
                            [](uint8_t value) { return value == 0x22; }));
    EXPECT_TRUE(std::all_of(engine.storage.begin() + 2048, engine.storage.begin() + 4096,
                            [](uint8_t value) { return value == 0x44; }));
}

TEST(AlignedWriter, FullRangeWriteInvalidatesInteriorPartialCache)
{
    RecordingEngine               engine(16384);
    auto                          disk = std::make_shared<TestDisk>(&engine, 4096);
    crowdb::diskio::AlignedWriter writer;
    std::vector<uint8_t>          partial(1024, 0x11);
    std::vector<uint8_t>          continuation(1024, 0x22);
    void                         *allocation = nullptr;
    ASSERT_EQ(::posix_memalign(&allocation, 4096, 12288), 0);
    auto aligned = std::unique_ptr<uint8_t, decltype(&::free)>(static_cast<uint8_t *>(allocation), &::free);
    std::memset(aligned.get(), 0x44, 12288);

    writer.submit(disk, 4096, partial.data(), partial.size(), [](int result) { ASSERT_EQ(result, 1024); });
    writer.submit(disk, 0, aligned.get(), 12288, [](int result) { ASSERT_EQ(result, 12288); });
    writer.submit(disk, 5120, continuation.data(), continuation.size(), [](int result) { ASSERT_EQ(result, 1024); });

    EXPECT_EQ(engine.read_count, 1);
    EXPECT_TRUE(std::all_of(engine.storage.begin() + 4096, engine.storage.begin() + 5120,
                            [](uint8_t value) { return value == 0x44; }));
    EXPECT_TRUE(std::all_of(engine.storage.begin() + 5120, engine.storage.begin() + 6144,
                            [](uint8_t value) { return value == 0x22; }));
    EXPECT_TRUE(std::all_of(engine.storage.begin() + 6144, engine.storage.begin() + 8192,
                            [](uint8_t value) { return value == 0x44; }));
}

TEST(AlignedWriter, DestructionWaitsForActiveCompletion)
{
    RecordingEngine      engine(8192, true);
    auto                 disk   = std::make_shared<TestDisk>(&engine, 4096);
    auto                 writer = std::make_unique<crowdb::diskio::AlignedWriter>();
    std::vector<uint8_t> data(1024, 0x11);
    std::atomic<bool>    destroyed{false};
    writer->submit(disk, 0, data.data(), data.size(), [](int result) { ASSERT_EQ(result, 1024); });

    std::thread destroyer([writer = std::move(writer), &destroyed]() mutable {
        writer.reset();
        destroyed.store(true, std::memory_order_release);
    });
    std::this_thread::yield();
    EXPECT_FALSE(destroyed.load(std::memory_order_acquire));
    engine.complete_one_write();
    destroyer.join();
    EXPECT_TRUE(destroyed.load(std::memory_order_acquire));
}

TEST(AlignedWriter, SameBlockWritesCompleteInSubmissionOrder)
{
    RecordingEngine               engine(8192, true);
    auto                          disk = std::make_shared<TestDisk>(&engine, 4096);
    crowdb::diskio::AlignedWriter writer;
    std::vector<uint8_t>          first(1024, 0x11);
    std::vector<uint8_t>          second(1024, 0x22);
    std::vector<int>              completions;

    writer.submit(disk, 0, first.data(), first.size(), [&](int) { completions.push_back(1); });
    writer.submit(disk, 1024, second.data(), second.size(), [&](int) { completions.push_back(2); });
    ASSERT_EQ(engine.pending_writes.size(), 1);

    engine.complete_one_write();
    ASSERT_EQ(engine.pending_writes.size(), 1);
    ASSERT_EQ(completions, std::vector<int>{1});
    engine.complete_one_write();

    EXPECT_EQ(completions, (std::vector<int>{1, 2}));
}

} // namespace
