// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/backend/page_store.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <vector>

using namespace crowdb::tree;

TEST(MemoryBacking, SparseGrowthAllocatesWrittenPagesWithoutMaterializingTheAddressSpace)
{
    MemPageStore                 store;
    const std::array<uint8_t, 8> prefix{1, 2, 3, 4, 5, 6, 7, 8};
    const uint8_t                suffix = 0xab;
    constexpr uint64_t           offset = 200ULL * 1024 * 1024;
    ASSERT_TRUE(store.write_at(0, prefix.data(), prefix.size()).ok());
    ASSERT_TRUE(store.write_at(offset, &suffix, 1).ok());
    EXPECT_EQ(store.size(), offset + 1);
    EXPECT_LT(store.resident_bytes(), 1024 * 1024);
    std::array<uint8_t, 8> read{};
    ASSERT_TRUE(store.read_at(0, read.data(), read.size()).ok());
    EXPECT_EQ(read, prefix);
    read.fill(0xff);
    ASSERT_TRUE(store.read_at(offset / 2, read.data(), read.size()).ok());
    EXPECT_EQ(read, (std::array<uint8_t, 8>{}));
    uint8_t last = 0;
    ASSERT_TRUE(store.read_at(offset, &last, 1).ok());
    EXPECT_EQ(last, suffix);
}

TEST(MemoryBacking, CrossPageWritesAndOverwritesPreserveBothEnds)
{
    MemPageStore         store;
    constexpr uint64_t   offset = (64 * 1024) - 3;
    std::vector<uint8_t> input((3 * 64 * 1024) + 7);
    for (size_t index = 0; index < input.size(); ++index) {
        input[index] = static_cast<uint8_t>(index % 251);
    }
    ASSERT_TRUE(store.write_at(offset, input.data(), input.size()).ok());
    const std::array<uint8_t, 9> patch{91, 92, 93, 94, 95, 96, 97, 98, 99};
    ASSERT_TRUE(store.write_at((128 * 1024) - 4, patch.data(), patch.size()).ok());
    std::copy(patch.begin(), patch.end(), input.begin() + ((64 * 1024) - 1));
    std::vector<uint8_t> output(input.size());
    ASSERT_TRUE(store.read_at(offset, output.data(), output.size()).ok());
    EXPECT_EQ(output, input);
    uint8_t hole = 0xff;
    ASSERT_TRUE(store.read_at(offset - 1, &hole, 1).ok());
    EXPECT_EQ(hole, 0);
}

TEST(MemoryBacking, LargeSparseAddressesAndRangeOverflowRemainBounded)
{
    MemPageStore   store;
    constexpr auto end   = std::numeric_limits<uint64_t>::max();
    const uint8_t  value = 0xcd;
    ASSERT_TRUE(store.write_at(end - 1, &value, 1).ok());
    EXPECT_EQ(store.size(), end);
    EXPECT_LT(store.resident_bytes(), 1024 * 1024);
    uint8_t read = 0;
    ASSERT_TRUE(store.read_at(end - 1, &read, 1).ok());
    EXPECT_EQ(read, value);
    EXPECT_EQ(store.write_at(end, &value, 1).code(), Code::kInvalidArgument);
    EXPECT_EQ(store.write_at(end - 1, &value, 2).code(), Code::kInvalidArgument);
    EXPECT_FALSE(store.read_at(end - 1, &read, 2).ok());
    EXPECT_EQ(store.size(), end);
}

TEST(MemoryBacking, EmptyAndInvalidTransfersDoNotGrowTheBacking)
{
    MemPageStore store;
    EXPECT_TRUE(store.write_at(100, nullptr, 0).ok());
    EXPECT_TRUE(store.read_at(0, nullptr, 0).ok());
    EXPECT_EQ(store.write_at(0, nullptr, 1).code(), Code::kInvalidArgument);
    EXPECT_EQ(store.size(), 0);
    EXPECT_EQ(store.resident_bytes(), 0);
}
