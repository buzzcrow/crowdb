// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <array>

namespace crowdb::tree
{
namespace
{
std::string key(size_t index)
{
    std::array<char, 32> text{};
    snprintf(text.data(), text.size(), "key%04zu", index);
    return text.data();
}

TEST(TreeScanTotalWindow, UnlimitedAndExplicitCountsSpanWindowsInBothDirections)
{
    Crowdbtree tree;
    for (size_t index = 0; index < 513; ++index) {
        ASSERT_TRUE(tree.put(key(index), "value").ok());
    }
    auto *core = TreeCoreAccess::get(tree);
    for (const bool materialized : {false, true}) {
        if (materialized) {
            ASSERT_TRUE(tree.flush().ok());
        }
        for (const size_t limit : {size_t{0}, size_t{129}, size_t{257}}) {
            for (const bool reverse : {false, true}) {
                CoreScanOptions options;
                options.limit   = limit;
                options.reverse = reverse;
                std::vector<scan_entry> entries;
                bool                    truncated = false;
                const auto              status    = core->scan(options, &entries, &truncated);
                ASSERT_TRUE(status.ok()) << status.to_string();
                ASSERT_EQ(entries.size(), limit == 0 ? 513 : limit);
                EXPECT_EQ(truncated, limit != 0);
                for (size_t index = 0; index < entries.size(); ++index) {
                    EXPECT_EQ(entries[index].key, key(reverse ? 512 - index : index));
                    EXPECT_EQ(entries[index].value, "value");
                }
            }
        }
    }
}

TEST(TreeScanTotalWindow, SmallWorkWindowsRespectThePublicByteBudget)
{
    Config             config;
    TreeRuntimeOptions host;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    // Read admission pays for reachable decoded references. Packed output
    // independently claims each actual capacity before allocation.
    config.runtime_binding.limits.routing_bytes = 192ULL * 1024 * 1024;
    Crowdbtree tree(config);
    for (size_t index = 0; index < 300; ++index) {
        ASSERT_TRUE(tree.put(key(index), "value").ok());
    }
    CoreScanOptions options;
    options.byte_budget = 25;
    std::vector<scan_entry> entries;
    bool                    truncated = false;
    auto                   *core      = TreeCoreAccess::get(tree);
    auto                    status    = core->scan(options, &entries, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(entries.size(), 2U);
    EXPECT_TRUE(truncated);
    options.byte_budget = 1;
    status              = core->scan(options, &entries, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(entries.size(), 1U);
    EXPECT_TRUE(truncated);
    options.strict_byte_budget = true;
    status                     = core->scan(options, &entries, &truncated);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(entries.empty());
    EXPECT_TRUE(truncated);
}

TEST(TreeScanTotalWindow, MaximumValueRemainsValidAsAnOversizedFirstResult)
{
    Config config;
    config.runtime_binding.limits.routing_bytes = 192ULL * 1024 * 1024;
    Crowdbtree tree(config);
    auto      *core    = TreeCoreAccess::get(tree);
    const auto maximum = core->load_limits().max_value_bytes;
    ASSERT_EQ(maximum, 64ULL * 1024 * 1024);
    ASSERT_TRUE(tree.put("large", std::string(maximum, 'v')).ok());
    CoreScanOptions options;
    options.byte_budget     = 1;
    bool          truncated = false;
    ScanPackedBuf packed;
    size_t        count  = 0;
    const auto    status = core->scan(options, nullptr, &truncated, &packed, &count);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(count, 1U);
    EXPECT_EQ(packed.size(), maximum + 5 + 17);
    EXPECT_EQ(packed.data()[packed.size() - 1], 'v');
    EXPECT_FALSE(truncated);
    packed                     = {};
    options.strict_byte_budget = true;
    const auto strict_status   = core->scan(options, nullptr, &truncated, &packed, &count);
    ASSERT_TRUE(strict_status.ok()) << strict_status.to_string();
    EXPECT_EQ(count, 0U);
    EXPECT_EQ(packed.size(), 0U);
    EXPECT_TRUE(truncated);
}

} // namespace
} // namespace crowdb::tree
