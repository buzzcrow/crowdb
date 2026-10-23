// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/aligned_writer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

namespace
{
using crowdb::diskio::AlignedWriter;
using crowdb::diskio::test::TestDisk;
using crowdb::diskio::test::TestRecordingEngine;

TEST(AlignedWriter, ByteAddressedWritesKeepTheirSegmentCompletionOrder)
{
    TestRecordingEngine  engine(4096, true);
    auto                 disk = std::make_shared<TestDisk>(&engine, 1);
    AlignedWriter        writer;
    std::vector<uint8_t> data(32, 7);
    std::vector<int>     completed;
    writer.submit_ordered(disk, 0, data.data(), data.size(), 0, [&](int) { completed.push_back(1); });
    writer.submit_ordered(disk, 32, data.data(), data.size(), 0, [&](int) { completed.push_back(2); });
    EXPECT_EQ(engine.pending_writes.size(), 1);
    engine.complete_one_write();
    EXPECT_EQ(completed, (std::vector<int>{1}));
    while (!engine.pending_writes.empty()) {
        engine.complete_one_write();
    }
    EXPECT_EQ(completed, (std::vector<int>{1, 2}));
}

TEST(AlignedWriter, EmptyOrderedBarrierWaitsForWritesWithoutIssuingMediaIo)
{
    TestRecordingEngine  engine(4096, true);
    auto                 disk = std::make_shared<TestDisk>(&engine, 1);
    AlignedWriter        writer;
    std::vector<uint8_t> data(32, 7);
    std::vector<int>     completed;
    writer.submit_ordered(disk, 0, data.data(), data.size(), 0, [&](int) { completed.push_back(1); });
    writer.submit_ordered(disk, 0, nullptr, 0, 0, [&](int result) {
        EXPECT_EQ(result, 0);
        completed.push_back(2);
    });
    EXPECT_EQ(engine.pending_writes.size(), 1);
    EXPECT_TRUE(completed.empty());
    engine.complete_one_write();
    while (!engine.pending_writes.empty()) {
        engine.complete_one_write();
    }
    EXPECT_EQ(completed, (std::vector<int>{1, 2}));
    EXPECT_EQ(engine.write_sizes, (std::vector<size_t>{32}));
}

TEST(AlignedWriter, ExpiredQueuedWritesCannotRunAfterThePriorCompletion)
{
    for (const size_t block_size : {size_t{1}, size_t{4096}}) {
        TestRecordingEngine  engine(8192, true);
        auto                 disk = std::make_shared<TestDisk>(&engine, block_size);
        AlignedWriter        writer;
        std::vector<uint8_t> data(32, 7);
        std::vector<int>     completed;
        writer.submit_ordered(disk, 0, data.data(), data.size(), 0, [&](int result) { completed.push_back(result); });
        writer.submit_ordered(
            disk, 32, data.data(), data.size(), 0, [&](int result) { completed.push_back(result); },
            std::chrono::steady_clock::time_point::min());
        EXPECT_TRUE(completed.empty());
        EXPECT_EQ(engine.pending_writes.size(), 1);
        engine.complete_one_write();
        EXPECT_EQ(completed, (std::vector<int>{32, -ETIMEDOUT}));
        EXPECT_TRUE(engine.pending_writes.empty());
        EXPECT_EQ(engine.write_sizes.size(), 1);
        while (!engine.pending_writes.empty()) {
            engine.complete_one_write();
        }
    }
}

TEST(AlignedWriter, ExpiredBoundaryReadCannotStartAMediaWrite)
{
    TestRecordingEngine engine(8192);
    engine.defer_reads        = true;
    auto                 disk = std::make_shared<TestDisk>(&engine, 4096);
    AlignedWriter        writer;
    std::vector<uint8_t> data(32, 7);
    int                  completed = 0;
    writer.submit_ordered(
        disk, 0, data.data(), data.size(), 0, [&](int result) { completed = result; },
        std::chrono::steady_clock::now() + std::chrono::milliseconds(50));
    EXPECT_EQ(engine.pending_reads.size(), 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    while (!engine.pending_reads.empty()) {
        engine.complete_one_read();
    }
    EXPECT_EQ(completed, -ETIMEDOUT);
    EXPECT_TRUE(engine.write_sizes.empty());
}
} // namespace
