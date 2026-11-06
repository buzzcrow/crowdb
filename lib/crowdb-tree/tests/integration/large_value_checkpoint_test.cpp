// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
void checkpoint_large_values(size_t source_entries)
{
    MemPageStore store(1);
    Config       config;
    config.page_store                   = &store;
    config.frame_bytes                  = 65536;
    config.execution.source_max_entries = source_entries;
    // Retained cold-page descriptors coexist with the next complete maintenance window.
    config.runtime_binding.limits.reclaim_bytes = 128 * 1024 * 1024;
    config.runtime_binding.limits.handoff_bytes = 128 * 1024 * 1024;
    config.runtime_binding.limits.writer_bytes  = 256 * 1024 * 1024;
    const std::string value(16384, 'v');
    {
        Crowdbtree tree(config);
        for (uint64_t pass = 0; pass < 2; ++pass) {
            for (uint64_t index = 1; index <= 4096; ++index) {
                const uint64_t slot   = (pass * 4096) + index;
                auto           status = tree.apply(slot, Batch{.ops = {{.key   = "key" + std::to_string(10000 + index),
                                                                        .kind  = OpKind::kPut,
                                                                        .value = value}}});
                ASSERT_TRUE(status.ok()) << slot << ": " << status.to_string();
            }
            auto status = tree.flush();
            ASSERT_TRUE(status.ok()) << status.to_string();
            uint64_t durable = 0;
            status           = tree.snapshot(&durable);
            ASSERT_TRUE(status.ok()) << status.to_string();
            EXPECT_EQ(durable, (pass + 1) * 4096);
            tree.set_gc_watermark(durable, durable);
        }
    }
    Crowdbtree reopened(config);
    for (uint64_t slot = 1; slot <= 4096; ++slot) {
        auto result = reopened.get_view("key" + std::to_string(10000 + slot));
        ASSERT_TRUE(result.found()) << slot;
        EXPECT_EQ(result.value().to_string(), value) << slot;
    }
}

} // namespace

TEST(LargeValueCheckpoint, ConfiguredBudgetPersistsAndReloadsMultipleMappingSegments)
{
    checkpoint_large_values(4096);
}

TEST(LargeValueCheckpoint, SmallSourceWindowPersistsOversizedLeafSources)
{
    checkpoint_large_values(64);
}

TEST(SourceBatchCapacity, WholeBatchRotatesBeforePartiallyRemainingEntryOrKeyCapacity)
{
    for (bool key_limited : {false, true}) {
        Config config;
        config.execution.source_max_entries   = key_limited ? 4096 : 5;
        config.execution.source_max_key_bytes = key_limited ? 128 : 1024 * 1024;
        const size_t batch_size               = key_limited ? 2 : 3;
        Crowdbtree   tree(config);
        for (uint64_t slot = 1; slot <= 8; ++slot) {
            Batch batch;
            for (size_t item = 0; item < batch_size; ++item) {
                const auto suffix = std::to_string((slot * batch_size) + item);
                batch.ops.push_back(
                    {.key = std::string(48 - suffix.size(), 'k') + suffix, .kind = OpKind::kPut, .value = "value"});
            }
            const auto status = tree.apply(slot, batch);
            ASSERT_TRUE(status.ok()) << "key_limited=" << key_limited << " slot=" << slot << ' ' << status.to_string();
        }
        ASSERT_TRUE(tree.flush().ok());
        for (uint64_t slot = 1; slot <= 8; ++slot) {
            for (size_t item = 0; item < batch_size; ++item) {
                const auto suffix = std::to_string((slot * batch_size) + item);
                const auto result = tree.get_view(std::string(48 - suffix.size(), 'k') + suffix);
                ASSERT_TRUE(result.found());
                EXPECT_EQ(result.slot(), slot);
                EXPECT_EQ(result.value().to_string(), "value");
            }
        }
    }
}
} // namespace crowdb::tree
