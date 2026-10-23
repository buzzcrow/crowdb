// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/aligned_writer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

namespace
{
using crowdb::diskio::AlignedWriter;
using crowdb::diskio::test::TestDisk;
using crowdb::diskio::test::TestRecordingEngine;

TEST(AlignedWriter, DelayedDuplicatePreservesLaterBytesAfterCacheReset)
{
    TestRecordingEngine  engine(8192);
    auto                 disk = std::make_shared<TestDisk>(&engine, 4096);
    std::vector<uint8_t> first(13, 0x11);
    std::vector<uint8_t> second(13, 0x22);
    {
        AlignedWriter writer;
        writer.submit_ordered(disk, 0, first.data(), first.size(), 0, [](int result) { ASSERT_EQ(result, 13); });
        writer.submit_ordered(disk, 13, second.data(), second.size(), 0, [](int result) { ASSERT_EQ(result, 13); });
    }
    AlignedWriter reopened;
    reopened.submit_ordered(disk, 0, first.data(), first.size(), 0, [](int result) { ASSERT_EQ(result, 13); });
    EXPECT_TRUE(std::equal(first.begin(), first.end(), engine.storage.begin()));
    EXPECT_TRUE(std::equal(second.begin(), second.end(), engine.storage.begin() + 13));
}

TEST(AlignedWriter, MissingHeadAndTailPreserveBothBoundaryBlocks)
{
    TestRecordingEngine engine(3 * 4096);
    std::fill(engine.storage.begin(), engine.storage.end(), 0x7a);
    auto                 disk = std::make_shared<TestDisk>(&engine, 4096);
    AlignedWriter        writer;
    std::vector<uint8_t> data(8192, 0x33);
    writer.submit_ordered(disk, 5, data.data(), data.size(), 0, [](int result) { ASSERT_EQ(result, 8192); });
    EXPECT_EQ(engine.read_count, 2);
    EXPECT_TRUE(
        std::all_of(engine.storage.begin(), engine.storage.begin() + 5, [](uint8_t value) { return value == 0x7a; }));
    EXPECT_TRUE(std::equal(data.begin(), data.end(), engine.storage.begin() + 5));
    EXPECT_TRUE(
        std::all_of(engine.storage.begin() + 8197, engine.storage.end(), [](uint8_t value) { return value == 0x7a; }));
}

TEST(AlignedWriter, CachedHeadDoesNotAuthorizeZeroingAnUncachedTail)
{
    TestRecordingEngine engine(8192);
    std::fill(engine.storage.begin(), engine.storage.end(), 0x7a);
    auto                 disk = std::make_shared<TestDisk>(&engine, 4096);
    AlignedWriter        writer;
    std::vector<uint8_t> first(13, 0x11);
    std::vector<uint8_t> second(4096, 0x22);
    writer.submit_ordered(disk, 0, first.data(), first.size(), 0, [](int result) { ASSERT_EQ(result, 13); });
    writer.submit_ordered(disk, 13, second.data(), second.size(), 0, [](int result) { ASSERT_EQ(result, 4096); });
    EXPECT_EQ(engine.read_count, 2);
    EXPECT_TRUE(std::equal(first.begin(), first.end(), engine.storage.begin()));
    EXPECT_TRUE(std::equal(second.begin(), second.end(), engine.storage.begin() + 13));
    EXPECT_TRUE(
        std::all_of(engine.storage.begin() + 4109, engine.storage.end(), [](uint8_t value) { return value == 0x7a; }));
}

TEST(AlignedWriter, FailedOrShortBoundaryReadCannotWriteUnknownBytes)
{
    for (const int read_result : {-EIO, 2048}) {
        TestRecordingEngine engine(3 * 4096);
        std::fill(engine.storage.begin(), engine.storage.end(), 0x7a);
        engine.failed_read_offset     = 8192;
        engine.failed_read_result     = read_result;
        const auto           retained = engine.storage;
        auto                 disk     = std::make_shared<TestDisk>(&engine, 4096);
        AlignedWriter        writer;
        std::vector<uint8_t> data(8192, 0x33);
        int                  result = 0;
        writer.submit_ordered(disk, 5, data.data(), data.size(), 0, [&](int completed) { result = completed; });
        EXPECT_EQ(result, -EIO);
        EXPECT_EQ(engine.read_count, 2);
        EXPECT_TRUE(engine.write_sizes.empty());
        EXPECT_EQ(engine.storage, retained);
    }
}
} // namespace
