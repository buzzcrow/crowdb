// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "btree/tree_core_read_internal.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <array>

namespace crowdb::tree
{
namespace
{
TEST(TreePointReadBound, SixteenTreesRetainPointWindowsOnOneDefaultHost)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.runtime_binding.limits.routing_bytes = 16ULL * 1024 * 1024;
    std::array<std::unique_ptr<Crowdbtree>, 16>       trees;
    std::array<core_read_detail::ReadPreparation, 16> reads;
    for (size_t index = 0; index < trees.size(); ++index) {
        trees[index] = std::make_unique<Crowdbtree>(config);
        ASSERT_TRUE(trees[index]->put("key", "value").ok());
        auto *core   = TreeCoreAccess::get(*trees[index]);
        auto  status = core_read_detail::prepare(*core, false, 256, 0, &reads[index]);
        ASSERT_TRUE(status.ok()) << index << ": " << status.to_string();
    }
    for (const auto &tree : trees) {
        GetView    value;
        const auto status = TreeCoreAccess::get(*tree)->get("key", &value);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(value.value().to_string(), "value");
    }
    reads = {};
}

TEST(TreePointReadBound, MaximumL0ValueBorrowsPayloadWithSmallRoutingAllowance)
{
    Config config;
    config.runtime_binding.limits.routing_bytes = 16ULL * 1024 * 1024;
    Crowdbtree tree(config);
    auto      *core = TreeCoreAccess::get(tree);
    ASSERT_EQ(core->load_limits().max_value_bytes, 64ULL * 1024 * 1024);
    ASSERT_TRUE(tree.put("key", std::string(core->load_limits().max_value_bytes, 'v')).ok());
    GetView    value;
    const auto status = core->get("key", &value);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(value.value().size(), core->load_limits().max_value_bytes);
    EXPECT_EQ(value.value().data()[value.value().size() - 1], 'v');
    core->close_and_drain();
    EXPECT_EQ(value.value().data()[0], 'v');
}

TEST(TreePointReadBound, ColdKnownValueBoundsReadWhileUnknownSummaryRemainsConservative)
{
    auto   store = std::make_shared<MemPageStore>(4096);
    Config config;
    config.page_store                           = store.get();
    config.backend_owner                        = store;
    config.frame_bytes                          = 4096;
    config.max_inline_value                     = 64;
    config.runtime_binding.limits.routing_bytes = 32ULL * 1024 * 1024;
    std::unique_ptr<Crowdbtree> tree;
    auto                        status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string large(20000, 'v');
    ASSERT_TRUE(tree->put("key", large).ok());
    status = tree->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto *core = TreeCoreAccess::get(*tree);
    auto  view = core->capture();
    ASSERT_EQ(view->stats.kv_unknown_pages, 0U);
    ASSERT_GT(view->mapping.copy_bounds().max_leaf_bases, 0U);
    (void)tree->evict_clean_leaves(0);
    GetView value;
    status = core->get("key", &value);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(value.value().to_string(), large);
    auto unknown                    = std::make_shared<PublishedTreeState>(*view);
    unknown->stats.kv_unknown_pages = 1;
    unknown->stats.kv_known_bytes   = 0;
    core_read_detail::ReadPreparation prepared;
    status = core_read_detail::prepare(*core, std::move(unknown), core->read_limits(), false, 256, 0, &prepared);
    EXPECT_EQ(status.code(), Code::kResourceExhausted);
}
} // namespace
} // namespace crowdb::tree
