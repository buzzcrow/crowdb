// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/btree/range_rebuild.h"
#include "local_checkpoint_test_support.h"

namespace crowdb::tree
{
namespace
{
struct TestRangeBackend
{
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;

    TestRangeBackend()
    {
        store.device = device;
    }

    Config config(std::shared_ptr<TreeRuntime> runtime = {})
    {
        Config out;
        out.runtime                              = std::move(runtime);
        out.frame_bytes                          = 4096;
        out.max_inline_value                     = 32;
        out.page_store                           = device.get();
        out.async_page_store                     = &store;
        out.runtime_binding.limits.io_operations = 1;
        return out;
    }
};

class RuntimeRangeRebuild : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_TRUE(TreeRuntime::create({}, &runtime).ok());
        source_config               = source_backend->config(runtime);
        source_config.backend_owner = source_backend;
        auto status                 = Crowdbtree::open(source_config, &source);
        ASSERT_TRUE(status.ok()) << status.to_string();
        Batch batch;
        for (size_t i = 0; i != 100; ++i) {
            const auto key = std::string(3 - std::to_string(i).size(), '0') + std::to_string(i);
            batch.ops.push_back({.key = key, .kind = OpKind::kPut, .value = value});
        }
        ASSERT_TRUE(source->apply(1, batch).ok());
        ASSERT_TRUE(source->apply(2, Batch{.ops = {{.key = "050", .kind = OpKind::kDelete, .value = ""}}}).ok());
        ASSERT_TRUE(source->snapshot().ok());
    }

    Config target_options(bool explicit_runtime = true)
    {
        auto config          = target_backend->config(explicit_runtime ? runtime : nullptr);
        config.backend_owner = target_backend;
        return config;
    }

    std::shared_ptr<TreeRuntime>      runtime;
    std::shared_ptr<TestRangeBackend> source_backend = std::make_shared<TestRangeBackend>();
    std::shared_ptr<TestRangeBackend> target_backend = std::make_shared<TestRangeBackend>();
    Config                            source_config;
    std::unique_ptr<Crowdbtree>       source;
    const std::string                 value = std::string(9000, 'r');
};

TEST_F(RuntimeRangeRebuild, BoundedRebuildPersistsOnlyItsRangeAndRemainsIndependentlyMutable)
{
    auto                        options = target_options();
    const auto                  range   = KeyRange::bounded(std::string("030"), std::string("070"));
    std::unique_ptr<Crowdbtree> target;
    RangeRebuildStats           stats;
    auto                        status = rebuild_range(*source, range, options, &target, &stats);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(stats.entries_emitted, 40U);
    EXPECT_EQ(stats.pages_reused, 0U);
    EXPECT_GT(stats.pages_rebuilt, 0U);
    EXPECT_NE(TreeCoreAccess::get(*source)->binding()->account(), TreeCoreAccess::get(*target)->binding()->account());
    EXPECT_EQ(target->get_view("030").value().to_string(), value);
    EXPECT_EQ(target->get_view("069").value().to_string(), value);
    EXPECT_FALSE(target->get_view("029").found());
    EXPECT_FALSE(target->get_view("070").found());
    EXPECT_FALSE(target->get_view("050").found());
    ASSERT_TRUE(source->put("040", "source changed").ok());
    EXPECT_EQ(target->get_view("040").value().to_string(), value);
    ASSERT_TRUE(target->put("040", "target changed").ok());
    EXPECT_EQ(target->get_view("040").slot(), 3U);
    EXPECT_EQ(target->put("outside", "rejected").code(), Code::kInvalidArgument);
    ASSERT_TRUE(target->snapshot().ok());
    target.reset();
    options.key_range = range;
    status            = Crowdbtree::open(options, &target);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(target->get_view("040").value().to_string(), "target changed");
    EXPECT_FALSE(target->get_view("050").found());
    EXPECT_EQ(source->get_view("040").value().to_string(), "source changed");
}

TEST_F(RuntimeRangeRebuild, WholeNativeCopyUsesTheRequestedHostAndANewBackendDomain)
{
    auto                        options = target_options();
    std::unique_ptr<Crowdbtree> target;
    RangeRebuildStats           stats;
    auto                        status = rebuild_range(*source, KeyRange::unbounded(), options, &target, &stats);
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto *src = TreeCoreAccess::get(*source);
    auto *dst = TreeCoreAccess::get(*target);
    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(dst->config().runtime, runtime);
    EXPECT_NE(src->binding()->account(), dst->binding()->account());
    EXPECT_EQ(stats.entries_emitted, 100U);
    EXPECT_EQ(stats.pages_reused, 0U);
    EXPECT_GT(stats.pages_rebuilt, 0U);
    EXPECT_EQ(target->get_view("099").value().to_string(), value);
    auto retained = target->get_view("099");
    source.reset();
    target.reset();
    EXPECT_EQ(retained.value().to_string(), value);
    options.runtime = runtime;
    status          = Crowdbtree::open(options, &target);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(target->get_view("099").value().to_string(), value);
}

TEST_F(RuntimeRangeRebuild, DifferentGeometryAndEmptyRangePreserveCapturedCoverage)
{
    auto options                      = target_options();
    options.frame_bytes               = 8192;
    const auto                  range = KeyRange::bounded(std::string("050"), std::string("050"));
    std::unique_ptr<Crowdbtree> target;
    RangeRebuildStats           stats;
    auto                        status = rebuild_range(*source, range, options, &target, &stats);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(stats.entries_emitted, 0U);
    EXPECT_EQ(target->contiguous_slot(), 2U);
    EXPECT_FALSE(target->get_view("030").found());
    EXPECT_EQ(target->put("050", "outside").code(), Code::kInvalidArgument);
    target.reset();
    options.key_range = range;
    status            = Crowdbtree::open(options, &target);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(target->durable_snapshot_last_applied_slot(), 2U);
}

TEST_F(RuntimeRangeRebuild, FailedDestinationStagingPreservesExistingOutputAndSourceValues)
{
    auto options                                 = target_options();
    options.runtime_binding.limits.reclaim_bytes = 512;
    Config sentinel_config;
    sentinel_config.runtime = runtime;
    auto  target            = std::make_unique<Crowdbtree>(sentinel_config);
    auto *original          = target.get();
    ASSERT_TRUE(target->put("retained", "old output").ok());
    RangeRebuildStats stats;
    stats.entries_emitted = 99;
    auto status =
        rebuild_range(*source, KeyRange::bounded(std::string("030"), std::string("070")), options, &target, &stats);
    EXPECT_EQ(status.code(), Code::kResourceExhausted) << status.to_string();
    EXPECT_EQ(target.get(), original);
    EXPECT_EQ(target->get_view("retained").value().to_string(), "old output");
    EXPECT_EQ(stats.entries_emitted, 99U);
    EXPECT_EQ(source->get_view("040").value().to_string(), value);
}
} // namespace
} // namespace crowdb::tree
