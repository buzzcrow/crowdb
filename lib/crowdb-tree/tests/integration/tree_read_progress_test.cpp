// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "local_recovery_test_support.h"

#include <chrono>
#include <future>
#include <semaphore>

namespace crowdb::tree
{
namespace
{
struct TestReadCpuGate
{
    std::shared_ptr<TreeRuntimeBinding> binding;
    std::binary_semaphore               entered{0};
    std::atomic<bool>                   released{false};

    explicit TestReadCpuGate(std::shared_ptr<TreeRuntimeBinding> owner) : binding(std::move(owner))
    {
        EXPECT_TRUE(binding->cpu()->try_submit(binding->lane(), &run, this).ok());
        entered.acquire();
        auto queued = Status::Ok();
        for (size_t i = 0; i <= binding->options().cpu_queue_limit; ++i) {
            queued = binding->cpu()->try_submit(binding->lane(), +[](void *) noexcept {}, nullptr);
            if (!queued.ok()) {
                break;
            }
        }
        EXPECT_EQ(queued.code(), Code::kResourceExhausted);
    }

    ~TestReadCpuGate()
    {
        release();
        binding->drain();
    }

    static void run(void *context) noexcept
    {
        auto &self = *static_cast<TestReadCpuGate *>(context);
        self.entered.release();
        while (!self.released.load(std::memory_order_acquire)) {
            self.released.wait(false);
        }
    }

    void release()
    {
        released.store(true, std::memory_order_release);
        released.notify_all();
    }
};

struct TestProgressReadStore final : AsyncPageStore
{
    test_write::TestRecoveryStore store;
    std::binary_semaphore         entered{0};
    bool                          notify = false;

    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override
    {
        return true;
    }

    uint64_t submit_read(PageAddr at, void *out, size_t bytes, AsyncCompletion done) override
    {
        const auto id = store.submit_read(at, out, bytes, std::move(done));
        if (notify) {
            entered.release();
        }
        return id;
    }

    uint64_t submit_write(PageAddr at, const void *data, size_t bytes, AsyncCompletion done) override
    {
        return store.submit_write(at, data, bytes, std::move(done));
    }

    Status submit_fsync(AsyncCompletion done) override
    {
        return store.submit_fsync(std::move(done));
    }

    void cancel(uint64_t /*op_id*/) override
    {
    }
};

TEST(TreeReadProgress, AdmittedAsyncReadWaitsForFullOrdinaryCpuQueue)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.runtime_binding.cpu_queue_limit = 8;
    auto store                             = std::make_shared<MemPageStore>(4096);
    config.page_store                      = store.get();
    config.backend_owner                   = store;
    config.frame_bytes                     = 4096;
    config.leaf_split_bytes                = 3000;
    std::shared_ptr<TreeCore> core;
    const auto                opened = TreeCore::open(config, &core);
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    core->binding()->drain();
    std::promise<Status> done;
    auto                 completed = done.get_future();
    TestReadCpuGate      gate(core->binding());
    core->get_async("key", [&](Status status, GetView value) {
        if (status.ok()) {
            EXPECT_EQ(value.value().to_string(), "value");
        }
        done.set_value(std::move(status));
    });
    const auto pending = completed.wait_for(std::chrono::milliseconds(20));
    gate.release();
    const auto status = completed.get();
    EXPECT_EQ(pending, std::future_status::timeout);
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TEST(TreeReadProgress, ColdDecodeUsesReservedProgressWhenOrdinaryCpuQueueFills)
{
    auto store          = std::make_shared<TestProgressReadStore>();
    store->store.device = std::make_shared<MemPageStore>(4096);
    Config             config;
    TreeRuntimeOptions host;
    host.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.runtime_binding.cpu_queue_limit = 8;
    config.page_store                      = store->store.device.get();
    config.async_page_store                = store.get();
    config.backend_owner                   = store;
    config.frame_bytes                     = 4096;
    config.leaf_split_bytes                = 3000;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    core->binding()->drain();
    store->store.defer_at = store->store.reads + 1;
    store->notify         = true;
    std::promise<Status> done;
    auto                 completed = done.get_future();
    core->get_async("key", [&](Status status, GetView value) {
        if (status.ok()) {
            EXPECT_EQ(value.value().to_string(), "value");
        }
        done.set_value(std::move(status));
    });
    store->entered.acquire();
    TestReadCpuGate gate(core->binding());
    store->store.finish();
    const auto pending = completed.wait_for(std::chrono::milliseconds(20));
    gate.release();
    const auto status = completed.get();
    EXPECT_EQ(pending, std::future_status::timeout);
    ASSERT_TRUE(status.ok()) << status.to_string();
}
} // namespace
} // namespace crowdb::tree
