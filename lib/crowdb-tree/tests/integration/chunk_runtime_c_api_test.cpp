// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_c_api_internal.h"
#include "c_api_internal.h"
#include "common/test_chunk_root.h"
#include "crowdb-tree/chunk_runtime_c_api.h"
#include "runtime/tree_runtime_c_api.h"

namespace crowdb::tree
{
namespace
{
ct_chunk_runtime_options runtime_options()
{
    return {.version         = 1,
            .struct_bytes    = sizeof(ct_chunk_runtime_options),
            .tree_id         = 19,
            .owner_epoch     = 1,
            .open_generation = 0,
            .iu_size         = 4096,
            .mirror_copies   = 2,
            .max_chunk_bytes = 256 * 1024 * 1024};
}
} // namespace

TEST(ChunkRuntimeCAbi, VersionedFactoryChargesAndReleasesItsActualOwners)
{
    test_chunk::TestRootBackend backend;
    ct_runtime                 *raw_runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &raw_runtime), 0);
    std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> runtime(raw_runtime, &ct_runtime_free);
    Config                                                  config;
    ASSERT_TRUE(configure_tree_runtime(runtime.get(), nullptr, &config).ok());
    const auto         baseline = config.runtime->snapshot().allocated.routing_bytes;
    ct_root_catalog    catalog{.catalog = backend.catalog, .transport = backend.transport};
    ct_chunk_transport transport{.transport = backend.transport};
    auto               options   = runtime_options();
    ct_page_store     *raw_store = nullptr;
    --options.struct_bytes;
    EXPECT_EQ(ct_chunk_runtime_page_store_open(runtime.get(), &options, &catalog, &transport, &raw_store),
              static_cast<ct_status>(Code::kInvalidArgument));
    EXPECT_EQ(raw_store, nullptr);
    EXPECT_EQ(config.runtime->snapshot().allocated.routing_bytes, baseline);
    ++options.struct_bytes;
    ASSERT_EQ(ct_chunk_runtime_page_store_open(runtime.get(), &options, &catalog, &transport, &raw_store), 0);
    EXPECT_GT(config.runtime->snapshot().allocated.routing_bytes, baseline);
    ct_page_store_free(raw_store);
    EXPECT_EQ(config.runtime->snapshot().allocated.routing_bytes, baseline);
}

TEST(ChunkRuntimeCAbi, PublicTreeUsesSameHostAndCommitsWalWithTheRoot)
{
    test_chunk::TestRootBackend backend;
    backend.catalog->current_generation = 0;
    ct_runtime *raw_runtime             = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &raw_runtime), 0);
    std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> runtime(raw_runtime, &ct_runtime_free);
    ct_root_catalog    catalog{.catalog = backend.catalog, .transport = backend.transport};
    ct_chunk_transport transport{.transport = backend.transport};
    const auto         chunk_options = runtime_options();
    ct_page_store     *raw_store     = nullptr;
    ASSERT_EQ(ct_chunk_runtime_page_store_open(runtime.get(), &chunk_options, &catalog, &transport, &raw_store), 0);
    std::unique_ptr<ct_page_store, decltype(&ct_page_store_free)> store(raw_store, &ct_page_store_free);
    ct_options                                                    options{};
    options.page_store       = store.get();
    options.frame_bytes      = 4096;
    options.max_inline_value = 64;
    ct_runtime *raw_other    = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &raw_other), 0);
    std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> other(raw_other, &ct_runtime_free);
    ct_tree                                                *raw_tree = nullptr;
    EXPECT_EQ(ct_open_with_runtime(&options, other.get(), nullptr, &raw_tree),
              static_cast<ct_status>(Code::kInvalidArgument));
    ASSERT_EQ(ct_open_with_runtime(&options, runtime.get(), nullptr, &raw_tree), 0);
    std::unique_ptr<ct_tree, decltype(&ct_close)> tree(raw_tree, &ct_close);
    const uint8_t                                 key = 'k';
    const std::string                             value(20000, 'v');
    ASSERT_EQ(ct_apply_put(tree.get(), 1, &key, 1, reinterpret_cast<const uint8_t *>(value.data()), value.size()), 0);
    ASSERT_EQ(ct_chunk_page_store_set_wal_replay_offset(store.get(), 4096), 0);
    uint64_t coverage = 0;
    ASSERT_EQ(ct_snapshot(tree.get(), &coverage), 0);
    EXPECT_EQ(coverage, 1U);
    uint64_t wal  = 0;
    uint64_t root = 0;
    ASSERT_EQ(ct_chunk_page_store_get_wal_replay_offset(store.get(), &wal), 0);
    ASSERT_EQ(ct_chunk_page_store_get_manifest_generation(store.get(), &root), 0);
    EXPECT_EQ(wal, 4096U);
    EXPECT_EQ(root, 1U);
    const auto writes   = backend.transport->writes;
    const auto begins   = backend.catalog->begin_calls;
    const auto prepares = backend.catalog->prepare_calls;
    ASSERT_EQ(ct_snapshot(tree.get(), &coverage), 0);
    ASSERT_EQ(ct_chunk_page_store_get_manifest_generation(store.get(), &root), 0);
    EXPECT_EQ(root, 1U);
    EXPECT_EQ(backend.transport->writes, writes);
    EXPECT_EQ(backend.catalog->begin_calls, begins);
    EXPECT_EQ(backend.catalog->prepare_calls, prepares);
    tree.reset();
    raw_tree = nullptr;
    EXPECT_EQ(ct_open_with_runtime(&options, runtime.get(), nullptr, &raw_tree),
              static_cast<ct_status>(Code::kUnavailable));
    EXPECT_EQ(backend.catalog->writer_releases, 0U);
    EXPECT_GT(backend.catalog->queue->pump(64).completed, 0U);
    EXPECT_EQ(backend.catalog->writer_releases, 1U);
}
} // namespace crowdb::tree
