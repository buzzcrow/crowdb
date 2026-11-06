// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"

#include <gtest/gtest.h>

#include <sstream>

namespace crowdb::tree
{
namespace
{
class TreeCoreSourceStats : public testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions host;
        host.workers = 2;
        ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        config.frame_bytes       = 4096;
        config.leaf_split_bytes  = 3000;
        config.buffer_pool_bytes = 4 * 1024 * 1024;
        tree                     = std::make_unique<Crowdbtree>(config);
        core                     = TreeCoreAccess::get(*tree);
    }

    static void expect_current(TreeCore &core)
    {
        SourceMemoryStats expected{.known = true};
        for (const auto &table : core.capture()->sources->tables) {
            expected += table->source_stats();
        }
        EXPECT_EQ(core.source_stats(), expected);
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(TreeCoreSourceStats, FlushAndGenerationReplacementKeepOldOwnersOutOfCurrentSummary)
{
    EXPECT_TRUE(core->source_stats().known);
    EXPECT_EQ(core->source_stats().tables, 1U);
    EXPECT_EQ(core->source_stats().records, 0U);
    ASSERT_TRUE(tree->put("a", "one").ok());
    ASSERT_TRUE(tree->put("b", "two").ok());
    ASSERT_TRUE(tree->put("a", "new").ok());
    expect_current(*core);
    const auto previous = core->capture();
    ASSERT_TRUE(tree->flush().ok());
    expect_current(*core);
    EXPECT_EQ(core->source_stats().records, 0U);
    EXPECT_EQ(previous->sources->tables.front()->count(), 2U);
    ASSERT_TRUE(tree->put("c", "three").ok());
    const auto before_clear = core->capture();
    ASSERT_TRUE(tree->clear().ok());
    EXPECT_NE(core->capture()->sources->memory, before_clear->sources->memory);
    expect_current(*core);
    EXPECT_EQ(core->source_stats().records, 0U);
    EXPECT_EQ(before_clear->sources->memory->snapshot().records, 1U);
    ASSERT_TRUE(tree->put("d", "new-generation").ok());
    EXPECT_EQ(core->source_stats().records, 1U);
    EXPECT_EQ(before_clear->sources->memory->snapshot().records, 1U);
}

TEST_F(TreeCoreSourceStats, InheritedFrozenSummaryTracksOnlyItsFixedParentPrefix)
{
    Crowdbtree child(config);
    auto      *child_core = TreeCoreAccess::get(child);
    ASSERT_TRUE(tree->put("a", "parent-a").ok());
    ASSERT_TRUE(tree->put("b", "parent-b").ok());
    auto status = child.install_split_memtable_overlay(*tree, 2);
    ASSERT_TRUE(status.ok()) << status.to_string();
    expect_current(*child_core);
    EXPECT_EQ(child_core->source_stats().records, 2U);
    ASSERT_TRUE(child.put("a", "child-a").ok());
    ASSERT_TRUE(tree->put("c", "parent-c").ok());
    EXPECT_EQ(child_core->source_stats().records, 3U);
    uint64_t generation = 0;
    uint64_t frontier   = 0;
    ASSERT_TRUE(tree->begin_split_memtable_view(&generation, &frontier).ok());
    status = tree->publish_split_memtable_view(generation, 2, child, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    expect_current(*child_core);
    EXPECT_EQ(child_core->source_stats().records, 1U);
    ASSERT_TRUE(child.clear_split_memtable_overlay(*tree).ok());
    ASSERT_TRUE(tree->release_split_memtable_view(generation).ok());
    ASSERT_TRUE(child.flush().ok());
    expect_current(*child_core);
    EXPECT_EQ(child_core->source_stats().records, 0U);
}

TEST_F(TreeCoreSourceStats, RegisteredGaugesFollowCurrentSourcesWhileOldReadersRemainAlive)
{
    tree->init_metrics("source", "mem");
    ASSERT_TRUE(tree->put("a", "first").ok());
    ASSERT_TRUE(tree->put("a", "second").ok());
    const auto held          = core->capture();
    auto       expect_gauges = [&] {
        const auto                              stats      = core->source_stats();
        const auto                              output     = tree->flush_metrics_str(1.0, "now", 0);
        const std::pair<const char *, uint64_t> expected[] = {
            {"source.mt.summary.known.g",         1                            },
            {"source.mt.tables.g",                stats.tables                 },
            {"source.mt.records.g",               stats.records                },
            {"source.mt.frozen.g",                stats.frozen_tables          },
            {"source.mt.writers.g",               stats.writers                },
            {"source.mt.freezing.writers.g",      stats.freezing_writers       },
            {"source.mt.frozen.retained.bytes.g", stats.frozen_retained_bytes  },
            {"source.mt.history.live.g",          stats.memory.history_count   },
            {"source.mt.history.bytes.g",         stats.memory.history_bytes   },
            {"source.mt.descriptor.bytes.g",      stats.memory.descriptor_bytes},
            {"source.mt.node.bytes.g",            stats.memory.node_bytes      },
            {"source.mt.payload.bytes.g",         stats.memory.payload_bytes   }
        };
        for (const auto &[name, value] : expected) {
            const auto at = output.find(name);
            ASSERT_NE(at, std::string::npos) << name;
            std::istringstream input(output.substr(at + std::char_traits<char>::length(name)));
            uint64_t           actual = UINT64_MAX;
            ASSERT_TRUE(static_cast<bool>(input >> actual)) << name;
            EXPECT_EQ(actual, value) << name;
        }
    };
    expect_gauges();
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(held->sources->tables.front()->count(), 1U);
    EXPECT_EQ(core->source_stats().records, 0U);
    expect_gauges();
}
} // namespace
} // namespace crowdb::tree
