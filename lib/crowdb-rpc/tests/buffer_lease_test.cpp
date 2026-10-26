// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"

#include <gtest/gtest.h>

#include <array>
#include <cstring>

using namespace crowdb::rpc;

TEST(BufferLeaseTest, NestedViewsRetainCanonicalInitializedSpanAfterPoolDestruction)
{
    crowdb_rpc_buffer_t handle   = nullptr;
    uint64_t            identity = 0;
    {
        RegisteredBufferPool pool(65536, 2, 17);
        auto                *root = pool.alloc(128);
        ASSERT_NE(root, nullptr);
        std::array<uint8_t, 32> bytes{};
        bytes.fill(0x5a);
        root->write(bytes.data(), bytes.size());
        auto *view   = root->slice(8, 17);
        auto *nested = view->slice(3, 6);
        ASSERT_NE(nested, nullptr);
        handle   = new crowdb_rpc_buffer_s{nested};
        identity = crowdb_rpc_buffer_allocation_id(handle);
        EXPECT_NE(identity, 0U);
        EXPECT_EQ(crowdb_rpc_buffer_capacity(handle), 6U);
        EXPECT_EQ(crowdb_rpc_buffer_allocation_capacity(handle), 1024U);
        EXPECT_EQ(crowdb_rpc_buffer_allocation_offset(handle), 11U);
        view->release();
        root->release();
    }
    auto *allocation = crowdb_rpc_buffer_allocation_ref(handle);
    ASSERT_NE(allocation, nullptr);
    EXPECT_EQ(crowdb_rpc_buffer_len(allocation), 32U);
    EXPECT_EQ(crowdb_rpc_buffer_allocation_id(allocation), identity);
    EXPECT_EQ(crowdb_rpc_buffer_allocation_offset(allocation), 0U);
    crowdb_rpc_buffer_release(handle);
    for (uint32_t index = 0; index < 32; ++index) {
        EXPECT_EQ(crowdb_rpc_buffer_data(allocation)[index], 0x5a);
    }
    crowdb_rpc_buffer_release(allocation);
}

TEST(BufferLeaseTest, UnknownExternalCapacityIsNeverGuessedFromItsViewLength)
{
    std::array<uint8_t, 10> bytes{};
    uint32_t                released = 0;
    auto                   *handle   = crowdb_rpc_buffer_create_external(
        bytes.data(), bytes.size(), [](void *context) { ++*static_cast<uint32_t *>(context); }, &released);
    ASSERT_NE(handle, nullptr);
    EXPECT_EQ(crowdb_rpc_buffer_allocation_capacity(handle), 0U);
    EXPECT_EQ(crowdb_rpc_buffer_allocation_id(handle), 0U);
    EXPECT_EQ(crowdb_rpc_buffer_allocation_ref(handle), nullptr);
    crowdb_rpc_buffer_release(handle);
    EXPECT_EQ(released, 1U);
    EXPECT_EQ(crowdb_rpc_buffer_allocation_capacity(nullptr), 0U);
    EXPECT_EQ(crowdb_rpc_buffer_allocation_ref(nullptr), nullptr);
}
