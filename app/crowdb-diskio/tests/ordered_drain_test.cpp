// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/aligned_writer.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>

namespace crowdb::diskio
{
TEST(OrderedDiskDrain, ZeroByteBarrierWaitsForActiveWriteAndExpiredLateWriteCannotFollow)
{
    test::TestRecordingEngine engine(8192, true);
    auto                      disk = std::make_shared<test::TestDisk>(&engine, 1);
    AlignedWriter             writer;
    std::vector<uint8_t>      data(32, 9);
    std::vector<int>          completions;
    writer.submit_ordered(disk, 0, data.data(), data.size(), 0, [&](int result) { completions.push_back(result); });
    writer.submit_ordered(disk, 0, nullptr, 0, 0, [&](int result) { completions.push_back(result); });
    EXPECT_TRUE(completions.empty());
    EXPECT_EQ(engine.pending_writes.size(), 1U);
    writer.submit_ordered(
        disk, 32, data.data(), data.size(), 0, [&](int result) { completions.push_back(result); },
        std::chrono::steady_clock::now() - std::chrono::seconds(1));
    engine.complete_one_write();
    EXPECT_EQ(completions, (std::vector<int>{32, 0, -ETIMEDOUT}));
    EXPECT_EQ(engine.write_sizes.size(), 1U);
    EXPECT_TRUE(engine.pending_writes.empty());
}
} // namespace crowdb::diskio
