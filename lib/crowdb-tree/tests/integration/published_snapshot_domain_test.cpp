// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/btree/range_rebuild.h"
#include "crowdb-tree/btree/scan_packed.h"
#include "snapshot/published_snapshot_entries.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestSnapshotDomain
{
    std::shared_ptr<MemPageStore> store = std::make_shared<MemPageStore>(1);
    Config                        config;
    std::unique_ptr<Crowdbtree>   tree;

    Status open()
    {
        auto status = TreeRuntime::create({}, &config.runtime);
        if (!status.ok()) {
            return status;
        }
        config.page_store    = store.get();
        config.backend_owner = store;
        status               = Crowdbtree::open(config, &tree);
        if (!status.ok()) {
            return status;
        }
        for (const auto *key : {"", "b", "m", "z"}) {
            status = tree->put(key, "value");
            if (!status.ok()) {
                return status;
            }
        }
        status = tree->snapshot();
        if (!status.ok()) {
            return status;
        }
        tree.reset();
        config.key_range = KeyRange::bounded("a", "m");
        return Crowdbtree::open(config, &tree);
    }
};

TEST(PublishedSnapshotDomain, DetachedGetScanAndEntriesKeepTheCapturedLogicalRange)
{
    TestSnapshotDomain test;
    auto               status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    ASSERT_TRUE(TreeCoreAccess::get(*test.tree)->capture_snapshot(&snapshot).ok());
    auto public_view = test.tree->snapshot_view();
    test.tree.reset();
    test.config.key_range = KeyRange::unbounded();
    GetView value;
    ASSERT_TRUE(snapshot->get("b", &value).ok());
    EXPECT_EQ(value.value().to_string(), "value");
    ASSERT_TRUE(snapshot->get("z", &value).ok());
    EXPECT_FALSE(value.found());
    EXPECT_FALSE(public_view->get("z", nullptr, nullptr));
    ASSERT_EQ(public_view->entries().size(), 1U);
    EXPECT_EQ(public_view->entries()[0].key, "b");
    for (const bool reverse : {false, true}) {
        ScanPackedBuf   packed;
        size_t          count     = 0;
        bool            truncated = false;
        CoreScanOptions options;
        options.reverse   = reverse;
        const auto result = snapshot->scan(options, &packed, &count, &truncated);
        ASSERT_TRUE(result.ok()) << result.to_string();
        EXPECT_EQ(count, 1U);
        EXPECT_FALSE(truncated);
    }
    std::shared_ptr<PublishedSnapshotEntries> entries;
    ASSERT_TRUE(PublishedSnapshotEntries::create(snapshot, &entries).ok());
    bool                     complete = false;
    std::vector<std::string> keys;
    while (!complete) {
        std::optional<PublishedScanEntry> entry;
        ASSERT_TRUE(entries->next(&entry, &complete).ok());
        if (entry) {
            keys.push_back(Slice(entry->key).to_string());
        }
    }
    EXPECT_EQ(keys, (std::vector<std::string>{"b"}));
}

TEST(PublishedSnapshotDomain, UnboundedRebuildCannotWidenABoundedRecoveredSource)
{
    TestSnapshotDomain test;
    auto               status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    MemPageStore target(1);
    Config       config;
    config.page_store = &target;
    std::unique_ptr<Crowdbtree> destination;
    status = rebuild_range(*test.tree, KeyRange::unbounded(), config, &destination);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(destination->get("b", nullptr, nullptr));
    EXPECT_FALSE(destination->get("z", nullptr, nullptr));
    EXPECT_EQ(destination->snapshot_view()->entries().size(), 1U);
}
} // namespace
} // namespace crowdb::tree
