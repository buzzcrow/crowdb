// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_internal.h"
#include "crowdb-tree/backend/backend_release_service.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "crowdb-tree/backend_release_c_api.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestWake
{
    std::atomic<size_t> wakes{0};
    std::atomic<size_t> drops{0};

    static void wake(void *p)
    {
        static_cast<TestWake *>(p)->wakes.fetch_add(1);
    }

    static void drop(void *p)
    {
        static_cast<TestWake *>(p)->drops.fetch_add(1);
    }
};

TEST(BackendReleaseCAbi, HostHandleOutlivesTreeAndDropsContextOnlyAfterDisposal)
{
    tree_test::TempDir path("block_release_abi_");
    ct_options         options{};
    options.path              = path.path.c_str();
    options.backend           = CT_BACKEND_BLOCK;
    options.iu_size           = 4096;
    options.frame_bytes       = 4096;
    options.block_size        = 1 << 20;
    options.buffer_pool_bytes = 1 << 20;
    ct_tree *tree             = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    TestWake                    wake;
    ct_backend_release_service *service = nullptr;
    ASSERT_EQ(ct_tree_backend_release_service(tree, TestWake::wake, TestWake::drop, &wake, &service), 0);
    TestWake                    refused;
    ct_backend_release_service *duplicate = nullptr;
    EXPECT_NE(ct_tree_backend_release_service(tree, TestWake::wake, TestWake::drop, &refused, &duplicate), 0);
    EXPECT_EQ(refused.drops.load(), 0U);
    EXPECT_EQ(duplicate, nullptr);
    std::thread callback([tree] { ct_close(tree); });
    callback.join();
    EXPECT_EQ(wake.drops.load(), 0U);
    size_t work    = 0;
    bool   drained = false;
    while (work++ < 1024) {
        const auto step = ct_backend_release_service_pump(service, 1);
        EXPECT_LE(step.visited, 1U);
        if (step.drained != 0) {
            drained = true;
            break;
        }
    }
    EXPECT_TRUE(drained);
    ct_backend_release_service_free(service);
    EXPECT_EQ(wake.drops.load(), 1U);
    EXPECT_GT(wake.wakes.load(), 0U);
}

TEST(BackendReleaseCAbi, UnsupportedBackendDoesNotConsumeWakeContext)
{
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    TestWake                    wake;
    ct_backend_release_service *service = nullptr;
    EXPECT_EQ(ct_tree_backend_release_service(tree, TestWake::wake, TestWake::drop, &wake, &service),
              static_cast<ct_status>(Code::kNotFound));
    EXPECT_EQ(service, nullptr);
    EXPECT_EQ(wake.drops.load(), 0U);
    ct_close(tree);
}
} // namespace
} // namespace crowdb::tree
