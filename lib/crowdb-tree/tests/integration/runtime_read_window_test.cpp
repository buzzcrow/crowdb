// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/backend/page_store.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(RuntimeReadWindow, DefaultFrameGeometryReadsColdInnerAndLeafWindowsWithinItsOriginalQuota)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    const auto store                            = std::make_shared<MemPageStore>(4096);
    config.page_store                           = store.get();
    config.backend_owner                        = store;
    config.runtime_binding.limits.io_operations = 1;
    std::shared_ptr<TreeCore> core;
    auto                      status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(80, 'v');
    for (size_t part = 0; part < 12; ++part) {
        Batch batch;
        for (size_t i = part * 100; i < (part + 1) * 100; ++i) {
            batch.ops.push_back({.key = "key" + std::to_string(10000 + i), .kind = OpKind::kPut, .value = value});
        }
        status = core->apply(part + 1, batch);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    status = core->checkpoint();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_GT(core->capture()->stats.inner_pages, 0U);
    core.reset();
    status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView found;
    status = core->get("key11199", &found);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(found.found());
    EXPECT_EQ(found.value().to_string(), value);
    size_t      count = 0;
    std::string cursor;
    bool        truncated = true;
    while (truncated) {
        CoreScanOptions options;
        options.start           = cursor;
        options.has_start_bound = !cursor.empty();
        options.limit           = 128;
        std::vector<scan_entry> entries;
        status = core->scan(options, &entries, &truncated);
        ASSERT_TRUE(status.ok()) << status.to_string();
        for (const auto &entry : entries) {
            EXPECT_EQ(entry.key, "key" + std::to_string(10000 + count));
            EXPECT_EQ(entry.value, value);
            ++count;
        }
        if (!entries.empty()) {
            cursor = entries.back().key;
        }
        ASSERT_TRUE(!truncated || !entries.empty());
    }
    EXPECT_EQ(count, 1200U);
    EXPECT_EQ(core->durable_coverage(), 12U);
}
} // namespace crowdb::tree
