// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/btree/range_rebuild.h"
#include "runtime/default_runtime.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(RangeRuntimeBinding, UnfundedDefaultDestinationKeepsCallerBindingAndOutput)
{
    Config source_options;
    ASSERT_TRUE(TreeRuntime::create({}, &source_options.runtime).ok());
    Crowdbtree source(source_options);
    ASSERT_TRUE(source.put("key", "value").ok());
    MemPageStore store(1);
    Config       destination_options;
    destination_options.page_store                      = &store;
    destination_options.runtime_binding.lifecycle_bytes = 1;
    std::unique_ptr<Crowdbtree> destination;
    const auto status = rebuild_range(source, KeyRange::unbounded(), destination_options, &destination);
    EXPECT_EQ(status.code(), Code::kResourceExhausted) << status.to_string();
    EXPECT_EQ(destination, nullptr);
    std::string value;
    EXPECT_TRUE(source.get("key", nullptr, &value));
    EXPECT_EQ(value, "value");
}

TEST(RangeRuntimeBinding, BothCopyPathsUseTheProcessHostAndCallerLimits)
{
    Config source_options;
    ASSERT_TRUE(TreeRuntime::create({}, &source_options.runtime).ok());
    source_options.frame_bytes = 4096;
    Crowdbtree source(source_options);
    ASSERT_TRUE(source.put("a", "one").ok());
    ASSERT_TRUE(source.put("z", "two").ok());
    Config process_options;
    ASSERT_TRUE(configure_default_runtime(&process_options).ok());
    ASSERT_NE(process_options.runtime, source_options.runtime);
    for (const bool bounded : {false, true}) {
        SCOPED_TRACE(bounded);
        MemPageStore store(1);
        Config       destination_options;
        destination_options.frame_bytes                         = 4096;
        destination_options.page_store                          = &store;
        destination_options.runtime_binding.limits.writer_bytes = 1024 * 1024;
        const auto range = bounded ? KeyRange::bounded(std::string("a"), std::string("m")) : KeyRange::unbounded();
        std::unique_ptr<Crowdbtree> destination;
        const auto                  status = rebuild_range(source, range, destination_options, &destination);
        ASSERT_TRUE(status.ok()) << status.to_string();
        const auto *core = TreeCoreAccess::get(*destination);
        EXPECT_EQ(core->config().runtime, process_options.runtime);
        EXPECT_EQ(core->binding()->options().limits.writer_bytes, 1024U * 1024U);
        std::string value;
        EXPECT_TRUE(destination->get("a", nullptr, &value));
        EXPECT_EQ(value, "one");
        EXPECT_EQ(destination->get("z", nullptr, &value), !bounded);
        EXPECT_EQ(destination->durable_snapshot_last_applied_slot(), source.last_applied_slot());
    }
}

TEST(RangeRuntimeBinding, DefaultDestinationStillRequiresAReloadableBackend)
{
    Crowdbtree source;
    ASSERT_TRUE(source.put("key", "value").ok());
    std::unique_ptr<Crowdbtree> destination;
    const auto                  status = rebuild_range(source, KeyRange::unbounded(), {}, &destination);
    EXPECT_EQ(status.code(), Code::kInvalidArgument) << status.to_string();
    EXPECT_EQ(destination, nullptr);
}
} // namespace
} // namespace crowdb::tree
