// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/scheduled_page_store.h"
#include "btree/tree_metrics.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
struct TestCompletion
{
    std::atomic<size_t> calls{0};
    Status              status;

    AsyncCompletion callback(std::shared_ptr<const void> owner = {})
    {
        return {.context = this,
                .complete_fn =
                    [](void *context, Status result) {
                        auto *self   = static_cast<TestCompletion *>(context);
                        self->status = std::move(result);
                        self->calls.fetch_add(1, std::memory_order_release);
                        self->calls.notify_all();
                    },
                .owner = std::move(owner)};
    }

    void wait()
    {
        while (calls.load(std::memory_order_acquire) == 0) {
            calls.wait(0, std::memory_order_acquire);
        }
    }
};

struct TestBlockingStore : MemPageStore
{
    mutable std::atomic<bool> entered{false};
    mutable std::atomic<bool> released{false};

    Status read_at(uint64_t address, uint8_t *data, size_t length) const override
    {
        entered.store(true, std::memory_order_release);
        entered.notify_all();
        while (!released.load(std::memory_order_acquire)) {
            released.wait(false, std::memory_order_acquire);
        }
        return MemPageStore::read_at(address, data, length);
    }

    void wait() const
    {
        while (!entered.load(std::memory_order_acquire)) {
            entered.wait(false, std::memory_order_acquire);
        }
    }

    void release() const
    {
        released.store(true, std::memory_order_release);
        released.notify_all();
    }
};

struct TestScheduledStore
{
    std::shared_ptr<TreeRuntime>        runtime;
    std::shared_ptr<TreeRuntimeBinding> binding;
    std::shared_ptr<AsyncPageStore>     scheduled;

    explicit TestScheduledStore(size_t io = 16, size_t queue = 64)
    {
        TreeRuntimeOptions options;
        options.workers             = 1;
        options.backend_workers     = 1;
        options.backend_queue_limit = queue;
        EXPECT_TRUE(TreeRuntime::create(options, &runtime).ok());
        TreeBindingOptions tree;
        tree.limits.io_operations = io;
        tree.backend_queue_limit  = queue;
        EXPECT_TRUE(TreeRuntimeBinding::create(runtime, tree, &binding).ok());
    }

    Status create(PageStore *sync, AsyncPageStore *async = nullptr)
    {
        return ScheduledPageStore::create(sync, async, {}, binding, binding->lifecycle_budget(),
                                          binding->backend_lane(), &scheduled);
    }

    ~TestScheduledStore()
    {
        binding->drain();
    }
};

TEST(ScheduledPageStore, BlockingReadDoesNotOccupyTheOnlyTreeCpuWorker)
{
    TestBlockingStore  device;
    TestScheduledStore test;
    ASSERT_TRUE(test.create(&device).ok());
    const std::array<uint8_t, 4> source{1, 2, 3, 4};
    ASSERT_TRUE(device.write_at(0, source.data(), source.size()).ok());
    std::array<uint8_t, 4> output{};
    TestCompletion         read;
    test.scheduled->submit_read(0, output.data(), output.size(), read.callback());
    device.wait();
    TestCompletion cpu;
    const auto     status = test.binding->cpu()->try_submit(
        test.binding->lane(),
        [](void *context) noexcept { static_cast<TestCompletion *>(context)->callback().complete(Status::Ok()); },
        &cpu);
    EXPECT_TRUE(status.ok());
    if (status.ok()) {
        cpu.wait();
    }
    EXPECT_EQ(read.calls.load(), 0U);
    device.release();
    read.wait();
    EXPECT_TRUE(read.status.ok());
    EXPECT_EQ(output, source);
}

TEST(ScheduledPageStore, FullCpuQueueUsesThePrepaidBoundedIoWindow)
{
    TestBlockingStore  device;
    TestScheduledStore test(8, 1);
    ASSERT_TRUE(test.create(&device).ok());
    std::array<uint8_t, 4> bytes{};
    ASSERT_TRUE(device.write_at(0, bytes.data(), bytes.size()).ok());
    std::array<TestCompletion, 8> accepted;
    TestCompletion                rejected;
    test.scheduled->submit_read(0, bytes.data(), bytes.size(), accepted[0].callback());
    device.wait();
    for (size_t index = 1; index < accepted.size(); ++index) {
        test.scheduled->submit_read(0, bytes.data(), bytes.size(), accepted[index].callback());
        EXPECT_EQ(accepted[index].calls.load(), 0U);
    }
    test.scheduled->submit_read(0, bytes.data(), bytes.size(), rejected.callback());
    EXPECT_EQ(rejected.calls.load(), 1U);
    EXPECT_EQ(rejected.status.code(), Code::kResourceExhausted);
    device.release();
    for (auto &completion : accepted) {
        completion.wait();
        EXPECT_TRUE(completion.status.ok());
        EXPECT_EQ(completion.calls.load(), 1U);
    }
}

struct TestUncertainStore : AsyncPageStore
{
    AsyncCompletion retained;
    bool            throws = false;

    uint64_t submit_read(PageAddr /*address*/, void * /*buffer*/, size_t /*length*/, AsyncCompletion complete) override
    {
        retained = std::move(complete);
        if (throws) {
            throw std::runtime_error("uncertain submission");
        }
        return 1;
    }

    uint64_t submit_write(PageAddr /*address*/, const void * /*buffer*/, size_t /*length*/,
                          AsyncCompletion complete) override
    {
        return submit_read(0, nullptr, 0, std::move(complete));
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        submit_read(0, nullptr, 0, std::move(complete));
        return Status::Ok();
    }

    void cancel(uint64_t /*operation*/) override
    {
    }
};

TEST(ScheduledPageStore, UncertainThrowKeepsBufferUntilLateTransportDisposalAndCompletesOnce)
{
    TestUncertainStore device;
    device.throws = true;
    TestScheduledStore test;
    ASSERT_TRUE(test.create(nullptr, &device).ok());
    auto                      owner = std::make_shared<std::array<uint8_t, 4>>();
    std::weak_ptr<const void> weak  = owner;
    TestCompletion            done;
    test.scheduled->submit_read(0, owner->data(), owner->size(), done.callback(owner));
    owner.reset();
    done.wait();
    test.binding->drain();
    EXPECT_EQ(done.status.code(), Code::kIoError);
    EXPECT_FALSE(weak.expired());
    device.retained.complete(Status::Ok());
    EXPECT_EQ(done.calls.load(), 1U);
    device.retained = {};
    EXPECT_TRUE(weak.expired());
}

TEST(ScheduledPageStore, DroppedAcceptedCallbackReturnsTerminalAndInFlightAdmission)
{
    TestUncertainStore device;
    TestScheduledStore test(1);
    ASSERT_TRUE(test.create(nullptr, &device).ok());
    std::array<uint8_t, 4> bytes{};
    TestCompletion         first;
    TestCompletion         rejected;
    TestCompletion         retry;
    test.scheduled->submit_read(0, bytes.data(), bytes.size(), first.callback());
    test.binding->drain();
    test.scheduled->submit_read(0, bytes.data(), bytes.size(), rejected.callback());
    EXPECT_EQ(rejected.status.code(), Code::kResourceExhausted);
    device.retained = {};
    first.wait();
    EXPECT_EQ(first.status.code(), Code::kUnavailable);
    test.scheduled->submit_read(0, bytes.data(), bytes.size(), retry.callback());
    test.binding->drain();
    device.retained.complete(Status::Ok());
    device.retained = {};
    retry.wait();
    EXPECT_TRUE(retry.status.ok());
}

TEST(ScheduledPageStore, CountersIncludeMetadataAndCountOnlyFirstRealTerminal)
{
    MemPageStore       device;
    TestScheduledStore test;
    ASSERT_TRUE(test.create(&device).ok());
    std::array<uint8_t, 8> bytes{};
    TestCompletion         write;
    auto                   callback = write.callback();
    callback.cause                  = TreeWorkCause::Checkpoint;
    test.scheduled->submit_typed_write(PagePurpose::kPageIndex, 0, bytes.data(), bytes.size(), callback);
    write.wait();
    ASSERT_TRUE(write.status.ok());
    TestCompletion read;
    callback       = read.callback();
    callback.cause = TreeWorkCause::Snapshot;
    test.scheduled->submit_read(0, bytes.data(), bytes.size(), callback);
    read.wait();
    ASSERT_TRUE(read.status.ok());
    TestCompletion missing;
    callback       = missing.callback();
    callback.cause = TreeWorkCause::Read;
    test.scheduled->submit_read(100, bytes.data(), bytes.size(), callback);
    missing.wait();
    EXPECT_FALSE(missing.status.ok());
    test.binding->drain();
    const auto counters = test.binding->work_metrics()->counters.snapshot();
    auto       value    = [&](TreeWorkCounter counter, TreeWorkCause cause) {
        return counters[static_cast<size_t>(cause)][static_cast<size_t>(counter)];
    };
    EXPECT_EQ(value(TreeWorkCounter::WriteOperations, TreeWorkCause::Checkpoint), 1U);
    EXPECT_EQ(value(TreeWorkCounter::WriteBytes, TreeWorkCause::Checkpoint), 8U);
    EXPECT_EQ(value(TreeWorkCounter::ReadOperations, TreeWorkCause::Snapshot), 1U);
    EXPECT_EQ(value(TreeWorkCounter::ReadBytes, TreeWorkCause::Snapshot), 8U);
    EXPECT_EQ(value(TreeWorkCounter::ReadOperations, TreeWorkCause::Read), 1U);
    EXPECT_EQ(value(TreeWorkCounter::ReadErrors, TreeWorkCause::Read), 1U);
    EXPECT_EQ(value(TreeWorkCounter::ReadBytes, TreeWorkCause::Read), 0U);
}

TEST(ScheduledPageStore, LateSuccessAfterTransportThrowDoesNotInventSuccessfulIoBytes)
{
    TestUncertainStore device;
    device.throws = true;
    TestScheduledStore test;
    ASSERT_TRUE(test.create(nullptr, &device).ok());
    std::array<uint8_t, 4> bytes{};
    TestCompletion         done;
    auto                   callback = done.callback();
    callback.cause                  = TreeWorkCause::Read;
    test.scheduled->submit_read(0, bytes.data(), bytes.size(), callback);
    done.wait();
    test.binding->drain();
    device.retained.complete(Status::Ok());
    device.retained.complete(Status::Ok());
    device.retained     = {};
    const auto counters = test.binding->work_metrics()->counters.snapshot()[static_cast<size_t>(TreeWorkCause::Read)];
    EXPECT_EQ(done.calls.load(), 1U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::ReadOperations)], 1U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::ReadErrors)], 1U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::ReadBytes)], 0U);
    EXPECT_EQ(counters[static_cast<size_t>(TreeWorkCounter::StaleCallbacks)], 2U);
}
} // namespace
} // namespace crowdb::tree
