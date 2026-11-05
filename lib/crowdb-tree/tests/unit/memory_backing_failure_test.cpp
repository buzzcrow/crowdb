// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/local/memory_backing_test.h"
#include "crowdb-tree/backend/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <vector>

using namespace crowdb::tree;

TEST(MemoryBacking, FailedGrowthReleasesEveryPreparedPageAndPreservesStoredBytes)
{
    MemPageStore                 store;
    const std::array<uint8_t, 3> prefix{1, 2, 3};
    ASSERT_TRUE(store.write_at(0, prefix.data(), prefix.size()).ok());
    const auto           before = store.resident_bytes();
    std::vector<uint8_t> payload(3 * 64 * 1024, 0x55);
    set_memory_backing_failure_after_for_tests(1);
    const auto failed = store.write_at(64 * 1024, payload.data(), payload.size());
    set_memory_backing_failure_after_for_tests(std::nullopt);
    ASSERT_EQ(failed.code(), Code::kResourceExhausted);
    EXPECT_EQ(store.resident_bytes(), before);
    EXPECT_EQ(store.size(), prefix.size());
    std::array<uint8_t, 3> first{};
    ASSERT_TRUE(store.read_at(0, first.data(), first.size()).ok());
    EXPECT_EQ(first, prefix);
    ASSERT_TRUE(store.write_at(64 * 1024, payload.data(), payload.size()).ok());
    std::vector<uint8_t> actual(payload.size());
    ASSERT_TRUE(store.read_at(64 * 1024, actual.data(), actual.size()).ok());
    EXPECT_EQ(actual, payload);
    ASSERT_TRUE(store.read_at(0, first.data(), first.size()).ok());
    EXPECT_EQ(first, prefix);
}
