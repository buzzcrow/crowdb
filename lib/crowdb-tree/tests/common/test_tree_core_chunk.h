// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "backend/chunk/chunk_backend_state.h"
#include "btree/tree_core_access.h"
#include "common/test_chunk_root.h"

namespace crowdb::tree::test_chunk
{
struct TestCoreChunk
{
    test_chunk::TestRootBackend                    backend;
    Config                                         config;
    std::shared_ptr<detail::ChunkRuntimePageStore> store;
    std::unique_ptr<Crowdbtree>                    tree;
    bool                                           attempted_open = false;

    explicit TestCoreChunk(size_t pin_capacity = 256) : backend(pin_capacity)
    {
        TreeRuntimeOptions host;
        host.workers         = 1;
        host.backend_workers = 1;
        EXPECT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        backend.catalog->current_generation = 0;
        configure_store(0);
        config.frame_bytes                          = 4096;
        config.leaf_split_bytes                     = 3000;
        config.max_inline_value                     = 64;
        config.runtime_binding.limits.io_operations = 1;
    }

    ~TestCoreChunk()
    {
        tree.reset();
        config.backend_owner.reset();
        store.reset();
        config.runtime.reset();
    }

    void configure_store(uint64_t generation)
    {
        store = std::make_shared<detail::ChunkRuntimePageStore>(
            detail::ChunkRuntimeOptions{.tree            = 19,
                                        .owner_epoch     = 1,
                                        .open_generation = generation,
                                        .iu_size         = 4096,
                                        .mirrors         = 2,
                                        .max_chunk_bytes = 256 * 1024 * 1024},
            backend.catalog, backend.transport, backend.credits.credits.reservation());
        config.page_store    = store.get();
        config.backend_owner = store;
    }

    Status open(uint64_t generation = 0)
    {
        if (attempted_open) {
            backend.catalog            = backend.catalog->next_owner();
            backend.transport->catalog = backend.catalog;
            configure_store(generation);
        }
        attempted_open = true;
        return Crowdbtree::open(config, &tree);
    }

    [[nodiscard]] Status put(uint64_t slot, const std::string &value) const
    {
        return tree->apply(slot, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = value}}});
    }
};
} // namespace crowdb::tree::test_chunk
