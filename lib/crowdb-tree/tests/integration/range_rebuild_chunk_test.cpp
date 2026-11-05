// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_tree_core_chunk.h"
#include "crowdb-tree/btree/range_rebuild.h"
#include "runtime_tree_access.h"
#include "snapshot/primary_backing.h"

#include <gtest/gtest.h>

#include <map>

namespace crowdb::tree
{
namespace
{
using test_chunk::TestCoreChunk;

std::map<std::string, std::string> contents(Crowdbtree &tree)
{
    std::vector<scan_entry> entries;
    bool                    truncated = false;
    const auto              status    = tree.scan({}, {}, {}, 0, 0, false, 0, &entries, &truncated);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(truncated);
    std::map<std::string, std::string> result;
    for (auto &entry : entries) {
        result.emplace(std::move(entry.key), std::move(entry.value));
    }
    return result;
}

void fill(TestCoreChunk &test)
{
    ASSERT_TRUE(test.open().ok());
    for (size_t index = 0; index < 80; ++index) {
        ASSERT_TRUE(test.tree->put("k" + std::to_string(1000 + index), std::string(128, 'v')).ok());
    }
    ASSERT_TRUE(test.tree->snapshot().ok());
}

Status rebuild(TestCoreChunk &source, TestCoreChunk &child, const KeyRange &range, RangeRebuildStats *stats = nullptr)
{
    auto status          = rebuild_range(*source.tree, range, child.config, &child.tree, stats);
    child.attempted_open = true;
    return status;
}

void next_owner(TestCoreChunk &child)
{
    child.tree.reset();
    child.backend.catalog            = child.backend.catalog->next_owner();
    child.backend.transport->catalog = child.backend.catalog;
    child.configure_store(child.backend.catalog->current_generation);
}

TEST(RangeRebuildChunk, ChildPublishesIndependentPaidBackingAndKeepsParentUnchanged)
{
    TestCoreChunk source;
    TestCoreChunk child;
    fill(source);
    RangeRebuildStats stats;
    ASSERT_TRUE(rebuild(source, child, KeyRange::unbounded(), &stats).ok());
    EXPECT_EQ(stats.pages_reused, 0U);
    EXPECT_GT(stats.pages_rebuilt, 0U);
    EXPECT_EQ(contents(*child.tree), contents(*source.tree));
    const auto old_source  = source.backend.catalog->committed_manifest;
    auto       source_root = RuntimeTreeAccess_for_tests::root(*source.tree);
    auto       child_root  = RuntimeTreeAccess_for_tests::root(*child.tree);
    ASSERT_TRUE(source_root->primary);
    ASSERT_TRUE(child_root->primary);
    EXPECT_NE(source_root->primary->frame.backing, child_root->primary->frame.backing);
    ASSERT_TRUE(child.tree->put("new-key", "child-only").ok());
    const auto checkpoint_status = child.tree->snapshot();
    ASSERT_TRUE(checkpoint_status.ok()) << checkpoint_status.to_string();
    EXPECT_EQ(source.backend.catalog->committed_manifest, old_source);
    EXPECT_FALSE(contents(*source.tree).contains("new-key"));
    EXPECT_EQ(contents(*child.tree).at("new-key"), "child-only");
}

TEST(RangeRebuildChunk, ImportedGenerationCanCheckpointLaterWritesAndReopenExactCoverage)
{
    TestCoreChunk source;
    TestCoreChunk child;
    ASSERT_TRUE(source.open().ok());
    ASSERT_TRUE(source.tree->put("b", "base").ok());
    ASSERT_TRUE(source.tree->flush().ok());
    auto status = rebuild(source, child, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(child.tree->durable_snapshot_last_applied_slot(), 1U);
    EXPECT_EQ(child.tree->durable_snapshot_seq(), 1U);
    for (uint64_t slot = 2; slot <= 3; ++slot) {
        ASSERT_TRUE(
            child.tree->apply(slot, Batch{.ops = {{.key = "c", .kind = OpKind::kPut, .value = std::to_string(slot)}}})
                .ok());
        status = child.tree->snapshot();
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(child.tree->durable_snapshot_last_applied_slot(), slot);
        EXPECT_EQ(child.tree->durable_snapshot_seq(), slot);
    }
    const auto generation = child.backend.catalog->current_generation;
    child.tree.reset();
    status = child.open(generation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(child.tree->durable_snapshot_last_applied_slot(), 3U);
    EXPECT_EQ(contents(*child.tree), (std::map<std::string, std::string>{
                                         {"b", "base"},
                                         {"c", "3"   }
    }));
}

TEST(RangeRebuildChunk, RetryReplacesAnAlreadyCommittedChildThroughItsAuthority)
{
    TestCoreChunk source;
    TestCoreChunk child;
    ASSERT_TRUE(source.open().ok());
    ASSERT_TRUE(source.tree->put("a", "first").ok());
    ASSERT_TRUE(source.tree->snapshot().ok());
    ASSERT_TRUE(rebuild(source, child, KeyRange::unbounded()).ok());
    const auto generation = child.backend.catalog->current_generation;
    next_owner(child);
    ASSERT_TRUE(source.tree->put("b", "second").ok());
    ASSERT_TRUE(source.tree->snapshot().ok());
    ASSERT_TRUE(rebuild(source, child, KeyRange::unbounded()).ok());
    EXPECT_GT(child.backend.catalog->current_generation, generation);
    EXPECT_EQ(contents(*child.tree), (std::map<std::string, std::string>{
                                         {"a", "first" },
                                         {"b", "second"}
    }));
}

TEST(RangeRebuildChunk, BoundedChildHasOnlyItsReachableGraphAndReopens)
{
    TestCoreChunk source;
    TestCoreChunk child;
    fill(source);
    const auto range = KeyRange::bounded("k1020", "k1060");
    ASSERT_TRUE(rebuild(source, child, range).ok());
    child.config.key_range = range;
    const auto view        = RuntimeTreeAccess_for_tests::capture(*child.tree);
    size_t     count       = 0;
    view->mapping.visit(&count, [](void *context, uint64_t, const std::shared_ptr<const PublishedPage> &) {
        ++*static_cast<size_t *>(context);
    });
    EXPECT_EQ(count, view->stats.leaf_pages + view->stats.inner_pages);
    EXPECT_EQ(contents(*child.tree).size(), 40U);
    uint64_t bytes    = UINT64_MAX;
    bool     complete = false;
    ASSERT_TRUE(child.tree->materialize_ownership(&bytes, &complete).ok());
    EXPECT_TRUE(complete);
    EXPECT_EQ(bytes, 0U);
    child.tree.reset();
    ASSERT_TRUE(child.open(child.backend.catalog->current_generation).ok());
    EXPECT_EQ(contents(*child.tree).size(), 40U);
    EXPECT_FALSE(contents(*child.tree).contains("k1019"));
    EXPECT_FALSE(contents(*child.tree).contains("k1060"));
    ASSERT_TRUE(child.tree->put("k1030x", "after-reopen").ok());
    ASSERT_TRUE(child.tree->snapshot().ok());
    child.tree.reset();
    ASSERT_TRUE(child.open(child.backend.catalog->current_generation).ok());
    EXPECT_EQ(contents(*child.tree).at("k1030x"), "after-reopen");
}

TEST(RangeRebuildChunk, UncheckpointedPublishedSourceIsCopiedAndDurableInChild)
{
    TestCoreChunk source;
    TestCoreChunk child;
    ASSERT_TRUE(source.open().ok());
    ASSERT_TRUE(source.tree->put("key", "old").ok());
    ASSERT_TRUE(source.tree->snapshot().ok());
    const auto durable = source.tree->durable_snapshot_last_applied_slot();
    ASSERT_TRUE(source.tree->put("key", "new").ok());
    ASSERT_TRUE(source.tree->flush().ok());
    ASSERT_TRUE(rebuild(source, child, KeyRange::unbounded()).ok());
    EXPECT_EQ(source.tree->durable_snapshot_last_applied_slot(), durable);
    EXPECT_EQ(contents(*child.tree).at("key"), "new");
    child.tree.reset();
    ASSERT_TRUE(child.open(child.backend.catalog->current_generation).ok());
    EXPECT_EQ(contents(*child.tree).at("key"), "new");
}

TEST(RangeRebuildChunk, FailedReplacementKeepsOldRootRecoverable)
{
    TestCoreChunk source;
    TestCoreChunk child;
    ASSERT_TRUE(source.open().ok());
    ASSERT_TRUE(source.tree->put("key", "old").ok());
    ASSERT_TRUE(source.tree->snapshot().ok());
    ASSERT_TRUE(rebuild(source, child, KeyRange::unbounded()).ok());
    const auto generation = child.backend.catalog->current_generation;
    next_owner(child);
    ASSERT_TRUE(source.tree->put("key", "new").ok());
    ASSERT_TRUE(source.tree->snapshot().ok());
    child.backend.catalog->fail_commit = true;
    EXPECT_FALSE(rebuild(source, child, KeyRange::unbounded()).ok());
    EXPECT_FALSE(child.tree);
    EXPECT_EQ(child.backend.catalog->current_generation, generation);
    child.backend.catalog->fail_commit = false;
    ASSERT_TRUE(child.open(generation).ok());
    EXPECT_EQ(contents(*child.tree).at("key"), "old");
}
} // namespace
} // namespace crowdb::tree
