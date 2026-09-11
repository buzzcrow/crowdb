// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "backend/chunk/chunk_page_store.h"

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <vector>

using namespace crowdb::tree;
using namespace crowdb::tree::detail;

namespace
{

void publish_bytes(ChunkPageStore *store, const std::vector<uint8_t> &bytes)
{
    if (!store->write_at(8192, bytes.data(), bytes.size()).ok() || !store->sync().ok() ||
        !store->write_at(0, bytes.data(), 1).ok() || !store->sync().ok()) {
        std::abort();
    }
}

struct Completion
{
    std::atomic<bool> done{false};
};

void completed(void *context, Status status)
{
    if (!status.ok()) {
        std::abort();
    }
    auto *completion = static_cast<Completion *>(context);
    completion->done.store(true, std::memory_order_release);
    completion->done.notify_one();
}

void BM_ChunkColdRead(benchmark::State &state)
{
    auto                 catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto                 transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore       writer({.tree_id = 1, .owner_epoch = 1}, catalog, transport);
    std::vector<uint8_t> bytes(4U * 1024U * 1024U, 7);
    publish_bytes(&writer, bytes);
    std::vector<uint8_t> out(64U * 1024U);
    for (auto _ : state) {
        ChunkPageStore reader({.tree_id = 1, .owner_epoch = 1, .layout_validity_ms = 0}, catalog, transport);
        benchmark::DoNotOptimize(reader.read_at(8192, out.data(), out.size()));
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(out.size()));
}

void BM_ChunkCoalescedRead(benchmark::State &state)
{
    auto                 catalog   = std::make_shared<MemoryRootCatalog>(1);
    auto                 transport = std::make_shared<MemoryChunkTransport>();
    ChunkPageStore       store({.tree_id = 2, .owner_epoch = 1}, catalog, transport);
    std::vector<uint8_t> bytes(4U * 1024U * 1024U, 7);
    publish_bytes(&store, bytes);
    std::vector<uint8_t> out(64U * 1024U);
    for (auto _ : state) {
        Completion completion;
        store.submit_read(8192, out.data(), out.size(), {.context = &completion, .complete_fn = &completed});
        completion.done.wait(false, std::memory_order_acquire);
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(out.size()));
    state.counters["coalesced"] = static_cast<double>(store.stats().coalesced_reads);
}

void BM_ChunkSnapshot(benchmark::State &state)
{
    std::vector<uint8_t> bytes(4U * 1024U * 1024U, 7);
    uint64_t             tree_id = 100;
    for (auto _ : state) {
        state.PauseTiming();
        auto catalog   = std::make_shared<MemoryRootCatalog>(1);
        auto transport = std::make_shared<MemoryChunkTransport>();
        auto store = std::make_unique<ChunkPageStore>(ChunkPageStore::Config{.tree_id = tree_id++, .owner_epoch = 1},
                                                      catalog, transport);
        store->write_at(8192, bytes.data(), bytes.size());
        store->sync();
        store->write_at(0, bytes.data(), 1);
        state.ResumeTiming();
        benchmark::DoNotOptimize(store->sync());
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(bytes.size()));
}

} // namespace

BENCHMARK(BM_ChunkColdRead);
BENCHMARK(BM_ChunkCoalescedRead);
BENCHMARK(BM_ChunkSnapshot);
