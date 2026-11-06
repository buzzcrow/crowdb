// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "btree/tree_core_read_internal.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"
#include "snapshot/published_snapshot_entries.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
TEST(BorrowedReadBudget, InlineWritebackAndNewUnknownOverlaysAdmitConcurrentReads)
{
    MemPageStore store(1);
    Config       config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.page_store                           = &store;
    config.frame_bytes                          = 65536;
    config.runtime_binding.limits.routing_bytes = 128 * 1024 * 1024;
    // I/O credit is consumed only when a backing read is submitted.
    config.runtime_binding.limits.io_operations = 64;
    Crowdbtree        tree(config);
    const std::string value(16384, 'v');
    ASSERT_TRUE(tree.apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = value}}}).ok());
    ASSERT_TRUE(tree.flush().ok());
    auto &core = *TreeCoreAccess::get(tree);
    ASSERT_TRUE(core.checkpoint().ok());
    ASSERT_TRUE(tree.apply(2, Batch{.ops = {{.key = "b", .kind = OpKind::kPut, .value = value}}}).ok());
    ASSERT_TRUE(tree.flush().ok());
    core.binding()->drain();
    auto captured = std::make_shared<PublishedTreeState>(*core.capture());
    // A different, still-unmeasured leaf can coexist with the written base.
    captured->stats.kv_unknown_pages = 1;
    ASSERT_GT(captured->mapping.copy_bounds().max_leaf_bases, 0U);
    ASSERT_GT(captured->stats.kv_unknown_pages, 0U);
    std::array<core_read_detail::ReadPreparation, 16> accepted;
    size_t                                            readers = 0;
    for (auto &read : accepted) {
        const auto status = core_read_detail::prepare(core, captured, core.read_limits(), &read);
        ASSERT_TRUE(status.ok()) << "readers=" << readers << ' ' << status.to_string();
        ++readers;
    }
}

TEST(BorrowedReadBudget, InlineOverlayValuesDoNotReserveAnOverflowAssemblyWindow)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.runtime_binding.limits.routing_bytes = 16 * 1024 * 1024;
    Crowdbtree        tree(config);
    const std::string first(1024 * 1024, 'a');
    const std::string second(1024 * 1024, 'b');
    auto              status = tree.apply(1, Batch{
                                                 .ops = {{.key = "a", .kind = OpKind::kPut, .value = first},
                                                         {.key = "b", .kind = OpKind::kPut, .value = second}}
    });
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = tree.flush();
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto &core = *TreeCoreAccess::get(tree);
    core.binding()->drain();
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    status = core.capture_snapshot(&snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    status = PublishedSnapshotEntries::create(snapshot, &cursor);
    ASSERT_TRUE(status.ok()) << status.to_string();
    bool   complete = false;
    size_t entries  = 0;
    while (!complete) {
        std::optional<PublishedScanEntry> entry;
        status = cursor->next(&entry, &complete);
        ASSERT_TRUE(status.ok()) << status.to_string();
        if (entry) {
            EXPECT_EQ(entry->cell.value.to_string(), entries == 0 ? first : second);
            ++entries;
        }
    }
    EXPECT_EQ(entries, 2U);

    GetView found;
    status = snapshot->get(Slice("b"), &found);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(found.found());
    EXPECT_EQ(found.value().to_string(), second);
    ScanPackedBuf   packed;
    size_t          count     = 0;
    bool            truncated = false;
    CoreScanOptions options;
    options.limit       = 2;
    options.byte_budget = 3 * 1024 * 1024;
    status              = snapshot->scan(options, &packed, &count, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(count, 2U);
    EXPECT_FALSE(truncated);
    status = core.get(Slice("a"), &found);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(found.found());
    EXPECT_EQ(found.value().to_string(), first);
}
} // namespace crowdb::tree
