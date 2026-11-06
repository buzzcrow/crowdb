// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/page_store.h"
#include "snapshot/primary_backing.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <semaphore>

namespace crowdb::tree
{
#ifdef CROWDB_TREE_TEST_UTIL
namespace
{
struct TestPipelineStore : MemPageStore, AsyncPageStore
{
    TestPipelineStore() : MemPageStore(4096)
    {
    }

    std::atomic<size_t>   writes{0};
    std::atomic<bool>     notify_write{false};
    std::binary_semaphore written{0};
    std::atomic<uint64_t> blocked_address{UINT64_MAX};
    std::atomic<bool>     pause_read{false};
    std::atomic<bool>     read_blocked{false};
    std::atomic<bool>     notify_other_read{false};
    std::binary_semaphore read_entered{0};
    std::binary_semaphore other_read{0};

    struct Read
    {
        PageAddr        address = 0;
        void           *data    = nullptr;
        size_t          bytes   = 0;
        AsyncCompletion complete;
    } held_read;

    uint64_t submit_read(PageAddr address, void *data, size_t bytes, AsyncCompletion complete) override
    {
        if (address == blocked_address.load() && pause_read.exchange(false)) {
            held_read = {.address = address, .data = data, .bytes = bytes, .complete = std::move(complete)};
            read_blocked.store(true);
            read_entered.release();
            return 0;
        }
        if (read_blocked.load() && notify_other_read.exchange(false)) {
            other_read.release();
        }
        complete.complete(MemPageStore::read_at(address, static_cast<uint8_t *>(data), bytes));
        return 0;
    }

    void resume_read()
    {
        if (read_blocked.exchange(false)) {
            auto read = std::move(held_read);
            read.complete.complete(MemPageStore::read_at(read.address, static_cast<uint8_t *>(read.data), read.bytes));
        }
    }

    uint64_t submit_write(PageAddr address, const void *data, size_t bytes, AsyncCompletion complete) override
    {
        return submit_typed_write(PagePurpose::kBtreePage, address, data, bytes, std::move(complete));
    }

    uint64_t submit_typed_write(PagePurpose purpose, PageAddr address, const void *data, size_t bytes,
                                AsyncCompletion complete) override
    {
        complete.complete(write_typed_at(purpose, address, static_cast<const uint8_t *>(data), bytes));
        return 0;
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        complete.complete(Status::Ok());
        return Status::Ok();
    }

    void cancel(uint64_t /*operation*/) override
    {
    }

    bool write_completion_is_reloadable() const noexcept override
    {
        return true;
    }

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

TEST(PersistentFlushPipeline, AColdReadDoesNotBlockOtherReadsOrReadyPageWrites)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 4;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    auto store                         = std::make_shared<TestPipelineStore>();
    config.page_store                  = store.get();
    config.async_page_store            = store.get();
    config.backend_owner               = store;
    config.frame_bytes                 = 4096;
    config.leaf_split_bytes            = 400;
    config.max_inline_value            = 32;
    config.inner_max_keys              = 4;
    config.inner_merge_keys            = 1;
    config.execution.flush_parallelism = 4;
    std::shared_ptr<TreeCore> core;
    {
        const auto status = TreeCore::open(config, &core);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    Batch initial;
    for (size_t i = 0; i < 200; ++i) {
        const auto digits = std::to_string(i);
        initial.ops.push_back(
            {.key = std::string(4 - digits.size(), '0') + digits, .kind = OpKind::kPut, .value = std::string(32, 'v')});
    }
    ASSERT_TRUE(core->apply(1, initial).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core.reset();
    {
        const auto status = TreeCore::open(config, &core);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    const auto view = core->capture();
    ASSERT_GE(view->mapping.get(view->root_page_id)->level, 2U);
    std::atomic<bool>     arm_read{true};
    std::atomic<bool>     pause_handoff{true};
    std::binary_semaphore handoff_entered{0};
    std::function<void()> resume_handoff;
    core->set_flush_handoff_wait_for_tests([&](uint64_t pid, std::function<void()> ready) {
        const auto page = view->mapping.get(pid);
        if (page && page->primary && page->primary->frame.backing && arm_read.exchange(false)) {
            store->blocked_address.store(page->primary->frame.backing->address);
            store->pause_read.store(true);
            return false;
        }
        if (pause_handoff.exchange(false)) {
            resume_handoff = std::move(ready);
            handoff_entered.release();
            return true;
        }
        return false;
    });
    const Batch changes{
        .ops = {{.key = "0000", .kind = OpKind::kPut, .value = std::string(32, 'a')},
                {.key = "0199", .kind = OpKind::kPut, .value = std::string(32, 'b')}}
    };
    ASSERT_TRUE(core->apply(2, changes).ok());
    core->binding()->drain();
    store->notify_other_read.store(true);
    store->notify_write.store(true);
    auto       checkpoint    = std::async(std::launch::async, [&] { return core->checkpoint(); });
    const bool blocked       = store->read_entered.try_acquire_for(std::chrono::seconds(5));
    const bool handoff_waits = handoff_entered.try_acquire_for(std::chrono::seconds(5));
    if (handoff_waits) {
        resume_handoff();
        resume_handoff = {};
    }
    const bool another_read = store->other_read.try_acquire_for(std::chrono::seconds(2));
    const bool written      = store->written.try_acquire_for(std::chrono::seconds(2));
    const bool waiting      = checkpoint.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout;
    store->resume_read();
    const auto status = checkpoint.get();
    core->binding()->drain();
    core->set_flush_handoff_wait_for_tests({});
    EXPECT_TRUE(blocked);
    EXPECT_TRUE(handoff_waits);
    EXPECT_TRUE(another_read);
    EXPECT_TRUE(written);
    EXPECT_TRUE(waiting);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->durable_coverage(), 2U);
    core.reset();
    {
        const auto status = TreeCore::open(config, &core);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    for (const auto *key : {"0000", "0199"}) {
        GetView value;
        ASSERT_TRUE(core->get(key, &value).ok());
        EXPECT_EQ(value.value().to_string(), std::string(32, std::string_view(key) == "0000" ? 'a' : 'b'));
    }
}
#endif
} // namespace crowdb::tree
