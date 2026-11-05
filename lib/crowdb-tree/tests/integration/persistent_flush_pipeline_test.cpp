// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/backend/page_store.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <semaphore>

namespace crowdb::tree
{
#ifdef CROWDB_TREE_TEST_UTIL
namespace
{
struct TestPipelineStore : MemPageStore
{
    TestPipelineStore() : MemPageStore(4096)
    {
    }

    std::atomic<size_t>   writes{0};
    std::atomic<bool>     notify_write{false};
    std::binary_semaphore written{0};

    Status write_typed_at(PagePurpose purpose, uint64_t address, const uint8_t *data, size_t bytes) override
    {
        const auto status = MemPageStore::write_typed_at(purpose, address, data, bytes);
        if (status.ok() && purpose == PagePurpose::kBtreePage) {
            writes.fetch_add(1, std::memory_order_relaxed);
            if (notify_write.exchange(false, std::memory_order_acq_rel)) {
                written.release();
            }
        }
        return status;
    }
};
} // namespace

TEST(PersistentFlushPipeline, FinalPageWritesWhileAnUnrelatedSubtreeStillWaitsAtHandoff)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers = 4;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    auto store                         = std::make_shared<TestPipelineStore>();
    config.page_store                  = store.get();
    config.backend_owner               = store;
    config.frame_bytes                 = 4096;
    config.leaf_split_bytes            = 400;
    config.max_inline_value            = 32;
    config.inner_max_keys              = 4;
    config.inner_merge_keys            = 1;
    config.execution.flush_parallelism = 4;
    std::shared_ptr<TreeCore> core;
    const auto                opened = TreeCore::open(config, &core);
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    Batch initial;
    for (size_t i = 0; i < 200; ++i) {
        const auto digits = std::to_string(i);
        initial.ops.push_back(
            {.key = std::string(4 - digits.size(), '0') + digits, .kind = OpKind::kPut, .value = std::string(32, 'v')});
    }
    ASSERT_TRUE(core->apply(1, initial).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    const auto before = core->capture();
    ASSERT_GE(before->mapping.get(before->root_page_id)->level, 2U);
    uint64_t blocked_pid = kInvalidPageId;
    for (uint64_t pid = 0; pid < before->next_page_id; ++pid) {
        const auto page = before->mapping.get(pid);
        if (page && page->leaf && page->leaf->range.contains("0199")) {
            blocked_pid = pid;
        }
    }
    ASSERT_NE(blocked_pid, kInvalidPageId);
    std::binary_semaphore entered{0};
    std::function<void()> resume;
    core->set_flush_handoff_wait_for_tests([&](uint64_t pid, std::function<void()> ready) {
        if (pid == blocked_pid) {
            resume = std::move(ready);
            entered.release();
            return true;
        }
        return false;
    });
    ASSERT_TRUE(core->apply(2,
                            Batch{
                                .ops = {{.key = "0000", .kind = OpKind::kPut, .value = std::string(32, 'a')},
                                        {.key = "0199", .kind = OpKind::kPut, .value = std::string(32, 'b')}}
    })
                    .ok());
    const auto writes = store->writes.load();
    store->notify_write.store(true, std::memory_order_release);
    auto       checkpoint = std::async(std::launch::async, [&] { return core->checkpoint(); });
    const auto blocked    = entered.try_acquire_for(std::chrono::seconds(10));
    const auto written    = store->written.try_acquire_for(std::chrono::seconds(10));
    const auto waiting    = checkpoint.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout;
    if (resume) {
        resume();
        resume = {};
    }
    const auto status = checkpoint.get();
    core->binding()->drain();
    core->set_flush_handoff_wait_for_tests({});
    EXPECT_TRUE(blocked);
    EXPECT_TRUE(written);
    EXPECT_TRUE(waiting);
    EXPECT_GT(store->writes.load(), writes);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->durable_coverage(), 2U);
    GetView value;
    ASSERT_TRUE(core->get("0000", &value).ok());
    EXPECT_EQ(value.value().to_string(), std::string(32, 'a'));
    ASSERT_TRUE(core->get("0199", &value).ok());
    EXPECT_EQ(value.value().to_string(), std::string(32, 'b'));
}
#endif
} // namespace crowdb::tree
