// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/backing_io.h"
#include "backend/local/scheduled_page_store.h"
#include "common/test_chunk_root.h"

#include <future>
#include <semaphore>

namespace crowdb::tree
{
namespace
{
struct TestChunkWrites
{
    std::shared_ptr<TreeRuntime>        runtime;
    std::shared_ptr<TreeRuntimeBinding> binding;
    std::shared_ptr<AsyncPageStore>     store;
    MemPageStore                        device{4096};

    explicit TestChunkWrites(size_t workers)
    {
        TreeRuntimeOptions options;
        options.workers         = 1;
        options.backend_workers = workers;
        EXPECT_TRUE(TreeRuntime::create(options, &runtime).ok());
        EXPECT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
        EXPECT_TRUE(ScheduledPageStore::create(&device, nullptr, {}, binding, binding->lifecycle_budget(),
                                               binding->backend_lane(), &store)
                        .ok());
    }

    ~TestChunkWrites()
    {
        binding->drain();
    }

    std::future<Status> submit(test_chunk::TestRootBackend &backend, std::span<const uint8_t> input,
                               std::shared_ptr<ChunkPackBacking> *pack)
    {
        detail::ChunkStageId stage;
        detail::ChunkId      chunk;
        EXPECT_TRUE(backend.ids->next(PagePurpose::kBtreePage, &stage, &chunk).ok());
        EXPECT_TRUE(prepare_chunk_backing(backend.domain, stage, chunk, static_cast<uint32_t>(input.size()),
                                          PagePurpose::kBtreePage, backend.credits.credits.reservation(), pack)
                        .ok());
        auto promise = std::make_shared<std::promise<Status>>();
        auto result  = promise->get_future();
        submit_chunk_write(*store, *pack, input, backend.credits.credits.reservation(),
                           {.context = promise.get(),
                            .complete_fn =
                                [](void *raw, Status status) {
                                    static_cast<std::promise<Status> *>(raw)->set_value(std::move(status));
                                },
                            .owner = promise});
        return result;
    }
};
} // namespace

TEST(ChunkWriteQueue, SlowPipeDoesNotBlockTheOtherPipe)
{
    test_chunk::TestRootBackend a;
    test_chunk::TestRootBackend b;
    TestChunkWrites             writes(2);
    std::binary_semaphore       entered{0};
    std::binary_semaphore       resume{0};
    a.transport->before_allocate = [&] {
        entered.release();
        resume.acquire();
    };
    const std::array<uint8_t, 32>     input{};
    std::shared_ptr<ChunkPackBacking> first;
    std::shared_ptr<ChunkPackBacking> second;
    auto                              blocked = writes.submit(a, input, &first);
    const bool                        started = entered.try_acquire_for(std::chrono::seconds(5));
    auto                              other   = writes.submit(b, input, &second);
    const bool                        ready   = other.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    const bool waiting = blocked.wait_for(std::chrono::seconds(0)) == std::future_status::timeout;
    resume.release();
    EXPECT_TRUE(started);
    EXPECT_TRUE(ready);
    EXPECT_TRUE(waiting);
    EXPECT_TRUE(other.get().ok());
    EXPECT_TRUE(blocked.get().ok());
    writes.binding->drain();
    std::array<uint8_t, 32> output{};
    ASSERT_TRUE(read_chunk_backing(*second, output, b.credits.credits.reservation()).ok());
    EXPECT_EQ(output, input);
}

TEST(ChunkWriteQueue, QueuedPagesShareOneAllocationAndKeepExactIndependentLocations)
{
    test_chunk::TestRootBackend backend;
    TestChunkWrites             writes(1);
    std::binary_semaphore       entered{0};
    std::binary_semaphore       resume{0};
    std::atomic<bool>           first{true};
    backend.transport->before_allocate = [&] {
        if (first.exchange(false)) {
            entered.release();
            resume.acquire();
        }
    };
    std::array<std::array<uint8_t, 128>, 5>          inputs{};
    std::array<std::shared_ptr<ChunkPackBacking>, 5> packs;
    std::array<std::future<Status>, 5>               results;
    for (size_t i = 0; i < inputs.size(); ++i) {
        inputs[i].fill(static_cast<uint8_t>(i + 1));
    }
    results[0]         = writes.submit(backend, inputs[0], packs.data());
    const bool started = entered.try_acquire_for(std::chrono::seconds(5));
    for (size_t i = 1; i < inputs.size(); ++i) {
        results[i] = writes.submit(backend, inputs[i], &packs[i]);
    }
    writes.store.reset();
    resume.release();
    EXPECT_TRUE(started);
    for (auto &result : results) {
        EXPECT_TRUE(result.get().ok());
    }
    writes.binding->drain();
    EXPECT_EQ(backend.catalog->records.size(), 3U);
    EXPECT_EQ(packs[2]->pack.stage, packs[3]->pack.stage);
    EXPECT_EQ(packs[2]->pack.stage, packs[4]->pack.stage);
    EXPECT_EQ(packs[2]->pin, packs[4]->pin);
    EXPECT_LT(packs[2]->pack.offset, packs[3]->pack.offset);
    EXPECT_LT(packs[3]->pack.offset, packs[4]->pack.offset);
    for (size_t i = 0; i < packs.size(); ++i) {
        std::array<uint8_t, 128> output{};
        ASSERT_TRUE(read_chunk_backing(*packs[i], output, backend.credits.credits.reservation()).ok());
        EXPECT_EQ(output, inputs[i]);
    }
    auto kept = packs[4];
    packs     = {};
    (void)backend.catalog->queue->pump(16);
    EXPECT_EQ(backend.catalog->queue->snapshot().registered, 1U);
    std::array<uint8_t, 128> output{};
    ASSERT_TRUE(read_chunk_backing(*kept, output, backend.credits.credits.reservation()).ok());
    EXPECT_EQ(output, inputs[4]);
}
} // namespace crowdb::tree
