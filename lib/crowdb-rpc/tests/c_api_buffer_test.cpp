// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api.h"

#include <gtest/gtest.h>

#include <cstring>

TEST(CApiBufferTest, ClonedHandlesReleaseIndependently)
{
    const uint8_t bytes[]  = {4, 8, 16};
    auto          original = crowdb_rpc_buffer_create(bytes, sizeof(bytes));
    ASSERT_NE(original, nullptr);
    auto clone = crowdb_rpc_buffer_ref(original);
    ASSERT_NE(clone, nullptr);
    ASSERT_NE(original, clone);
    EXPECT_EQ(crowdb_rpc_buffer_data(original), crowdb_rpc_buffer_data(clone));
    crowdb_rpc_buffer_release(original);
    EXPECT_EQ(crowdb_rpc_buffer_len(clone), sizeof(bytes));
    EXPECT_EQ(std::memcmp(crowdb_rpc_buffer_data(clone), bytes, sizeof(bytes)), 0);
    crowdb_rpc_buffer_release(clone);
}

TEST(CApiBufferTest, ExternalOwnerReleasesOnlyAfterLastIndependentHandle)
{
    uint32_t      releases = 0;
    const uint8_t bytes[]  = {3, 5, 7};
    auto          original = crowdb_rpc_buffer_create_external(
        bytes, sizeof(bytes), [](void *context) { ++*static_cast<uint32_t *>(context); }, &releases);
    ASSERT_NE(original, nullptr);
    auto clone = crowdb_rpc_buffer_ref(original);
    ASSERT_NE(original, clone);
    crowdb_rpc_buffer_release(clone);
    EXPECT_EQ(releases, 0U);
    EXPECT_EQ(std::memcmp(crowdb_rpc_buffer_data(original), bytes, sizeof(bytes)), 0);
    crowdb_rpc_buffer_release(original);
    EXPECT_EQ(releases, 1U);
}
