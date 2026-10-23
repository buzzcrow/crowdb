// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_rdma_device.h"
#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_RPC_TEST_UTIL)
#    include <gtest/gtest.h>

using namespace crowdb::rpc;

TEST(RdmaBufferPoolTest, OwnedBorrowKeepsExactNestedOffsetAndRejectsOtherArena)
{
    auto device = TestRdmaDevice::open();
    if (device == nullptr) {
        GTEST_SKIP() << "no accessible HCA";
    }
    RdmaBufferPool pool(device->pd, 4096, 4, device), other(device->pd, 4096, 4, device);
    auto          *root = pool.alloc(128);
    ASSERT_NE(root, nullptr);
    auto *base = root->data;
    for (uint32_t index = 0; index < 128; ++index) {
        base[index] = static_cast<uint8_t>(index);
    }
    root->len         = 128;
    root->view_offset = 17;
    auto *first       = root->slice(4, 40);
    ASSERT_NE(first, nullptr);
    auto *nested = first->slice(3, 13);
    ASSERT_NE(nested, nullptr);
    auto *borrowed = pool.borrow_owned_range(nested, nested->payload_data(), 13);
    ASSERT_NE(borrowed, nullptr);
    EXPECT_EQ(borrowed->payload_data(), base + 24);
    EXPECT_EQ(borrowed->payload_data()[12], 36);
    EXPECT_EQ(other.borrow_owned_range(nested, nested->payload_data(), 13), nullptr);
    EXPECT_EQ(pool.borrow_owned_range(nested, base + 1020, 8), nullptr);
    root->release();
    first->release();
    nested->release();
    EXPECT_FALSE(pool.quiescent());
    EXPECT_EQ(borrowed->payload_data()[0], 24);
    borrowed->release();
    EXPECT_TRUE(pool.quiescent());
}

TEST(RdmaBufferPoolTest, OwnedBorrowRetainsDetachedRegistrationAfterFacadeDestruction)
{
    auto device = TestRdmaDevice::open();
    if (device == nullptr) {
        GTEST_SKIP() << "no accessible HCA";
    }
    auto  pool   = std::make_unique<RdmaBufferPool>(device->pd, 4096, 4, device);
    auto *source = pool->alloc(128);
    ASSERT_NE(source, nullptr);
    source->write("detached payload", 16);
    ASSERT_TRUE(source->pool->detach(source));
    EXPECT_EQ(source->pool, nullptr);
    auto *borrowed = pool->borrow_owned_range(source, source->payload_data(), 16);
    ASSERT_NE(borrowed, nullptr);
    pool.reset();
    source->release();
    EXPECT_EQ(std::memcmp(borrowed->payload_data(), "detached payload", 16), 0);
    EXPECT_NE(borrowed->registration_handle, 0U);
    borrowed->release();
}

TEST(RdmaBufferPoolTest, ExternalViewCanRetainTheRegisteredAllocationWithoutACopy)
{
    auto device = TestRdmaDevice::open();
    if (device == nullptr) {
        GTEST_SKIP() << "no accessible HCA";
    }
    RdmaBufferPool pool(device->pd, 4096, 4, device);
    auto          *source = pool.alloc(128);
    ASSERT_NE(source, nullptr);
    source->write("external payload", 16);
    // The external descriptor's enclosing owner pins the native source,
    // as the HTTP framed-body adapter does across the Rust/C ABI boundary.
    Buffer external;
    external.data = source->payload_data();
    external.len = external.capacity = 16;
    auto *borrowed                   = pool.borrow_owned_range(&external, external.data, external.len);
    ASSERT_NE(borrowed, nullptr);
    EXPECT_EQ(borrowed->payload_data(), source->payload_data());
    source->release();
    EXPECT_EQ(std::memcmp(borrowed->payload_data(), "external payload", 16), 0);
    borrowed->release();
    EXPECT_TRUE(pool.quiescent());
}
#endif
