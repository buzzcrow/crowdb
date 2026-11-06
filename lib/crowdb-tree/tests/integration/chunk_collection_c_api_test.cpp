// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_c_api_internal.h"
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "common/test_chunk_collector.h"
#include "crowdb-tree/btree/tree.h"
#include "crowdb-tree/chunk_runtime_c_api.h"
#include "load_budget_test_support.h"

#include <latch>

namespace crowdb::tree
{
namespace
{
struct TestCollectionCAbi
{
    test_read::LoadCredits                        credits;
    std::shared_ptr<detail::TestCollectorCatalog> catalog   = std::make_shared<detail::TestCollectorCatalog>();
    std::shared_ptr<detail::TestFixedTransport>   transport = std::make_shared<detail::TestFixedTransport>(catalog);
    ct_runtime                                   *runtime   = nullptr;
    ct_page_store                                *store     = nullptr;
    ct_tree                                      *tree      = nullptr;

    ~TestCollectionCAbi()
    {
        if (tree != nullptr) {
            ct_close(tree);
        }
        if (store != nullptr) {
            ct_page_store_free(store);
        }
        if (runtime != nullptr) {
            ct_runtime_free(runtime);
        }
        if (catalog->queue) {
            catalog->queue->stop_admission();
            (void)catalog->queue->pump(32);
            EXPECT_TRUE(catalog->queue->drained());
        }
    }

    ct_status open()
    {
        auto status = detail::ChunkPinReleaseQueue::create(
            credits.credits.reservation(), 32, std::make_shared<int>(1), [](void * /*unused*/) noexcept {}, nullptr,
            &catalog->queue);
        if (!status.ok()) {
            return static_cast<ct_status>(status.code());
        }
        auto result = ct_runtime_create(nullptr, &runtime);
        if (result != 0) {
            return result;
        }
        ct_root_catalog                roots{.catalog = catalog, .transport = transport};
        ct_chunk_transport             chunks{.transport = transport};
        const ct_chunk_runtime_options options{.version         = 1,
                                               .struct_bytes    = sizeof(ct_chunk_runtime_options),
                                               .tree_id         = 17,
                                               .owner_epoch     = 1,
                                               .open_generation = 0,
                                               .iu_size         = 4096,
                                               .mirror_copies   = 2,
                                               .max_chunk_bytes = 256 * 1024 * 1024};
        result = ct_chunk_runtime_page_store_open(runtime, &options, &roots, &chunks, &store);
        if (result != 0) {
            return result;
        }
        ct_options config{};
        config.page_store  = store;
        config.frame_bytes = 4096;
        return ct_open_with_runtime(&config, runtime, nullptr, &tree);
    }
};
} // namespace

TEST(ChunkCollectionCAbi, TypedReportSurvivesTreeDrainAndRejectsWrongVersion)
{
    TestCollectionCAbi test;
    ASSERT_EQ(test.open(), 0);
    test.catalog->record = detail::test_collector_stage();
    ct_future *future    = nullptr;
    ASSERT_EQ(ct_chunk_collect_async(test.tree, &future), 0);
    std::unique_ptr<ct_future, decltype(&ct_future_free)> handle(future, &ct_future_free);
    ct_chunk_collection_result_v1                         result{};
    int32_t                                               done = 0;
    EXPECT_EQ(ct_future_poll_chunk_collection(handle.get(), &done, &result),
              static_cast<ct_status>(Code::kInvalidArgument));
    TreeCoreAccess::get(*c_api_tree(test.tree))->close_and_drain();
    result.struct_size = sizeof(result);
    result.version     = 1;
    ASSERT_EQ(ct_future_poll_chunk_collection(handle.get(), &done, &result), 0);
    ASSERT_EQ(done, 1);
    EXPECT_EQ(handle.release(), future);
    EXPECT_EQ(result.terminal_status, 0);
    EXPECT_EQ(result.stages_removed, 1U);
    EXPECT_EQ(result.logical_capacity_bytes, 4096U);
    EXPECT_EQ(test.transport->deletions, 1U);
}

TEST(ChunkCollectionCAbi, DroppedFutureRetainsAcceptedDeleteThroughClose)
{
    TestCollectionCAbi test;
    ASSERT_EQ(test.open(), 0);
    test.catalog->record = detail::test_collector_stage();
    std::latch entered(1);
    std::latch release(1);
    test.transport->before_drain = [&] {
        entered.count_down();
        release.wait();
    };
    ct_future *future = nullptr;
    ASSERT_EQ(ct_chunk_collect_async(test.tree, &future), 0);
    entered.wait();
    ct_future_free(future);
    release.count_down();
    ct_close(test.tree);
    test.tree = nullptr;
    EXPECT_EQ(test.transport->deletions, 1U);
    EXPECT_FALSE(test.catalog->record);
}
} // namespace crowdb::tree
