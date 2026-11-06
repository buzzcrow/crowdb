// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
TEST(ChunkLegacyRuntimeOpen, RejectsTheMigrationOnlyStoreWithAndWithoutAsyncInjection)
{
    for (const bool asynchronous : {false, true}) {
        SCOPED_TRACE(asynchronous);
        auto           catalog   = std::make_shared<MemoryRootCatalog>(1);
        auto           transport = std::make_shared<MemoryChunkTransport>();
        ChunkPageStore store({.tree_id = 29, .owner_epoch = 1, .pack_bytes = 4096, .iu_size = 1}, catalog, transport);
        ASSERT_TRUE(store.open_status().ok());
        crowdb::tree::Config config;
        ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.page_store       = &store;
        config.async_page_store = asynchronous ? &store : nullptr;
        config.frame_bytes      = 4096;
        std::unique_ptr<Crowdbtree> tree;
        const auto                  status = Crowdbtree::open(config, &tree);
        EXPECT_EQ(status.code(), Code::kNotSupported) << status.to_string();
        EXPECT_NE(status.to_string().find("migrate_legacy_chunk"), std::string::npos);
        EXPECT_FALSE(tree);
        EXPECT_EQ(store.stats().generations_published, 0U);
    }
}
} // namespace crowdb::tree::detail
