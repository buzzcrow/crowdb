// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/snapshot/snapshot_io.h"
#include "native_import_test_support.h"

namespace crowdb::tree
{
namespace
{
class TreeCoreNativeImport : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.frame_bytes = 4096;
        tree               = std::make_unique<Crowdbtree>(config);
        core               = TreeCoreAccess::get(*tree);
    }

    Status install(std::vector<NativeFrame> frames, uint64_t coverage = 7)
    {
        const auto count = frames.size();
        return tree->install_snapshot_native(std::move(frames), count == 0 ? kInvalidPageId : 0, coverage, count);
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(TreeCoreNativeImport, OwnedNativeFramesReplaceGenerationAndResumeAutomaticSlots)
{
    ASSERT_TRUE(tree->put("old", "retained").ok());
    const auto        old        = tree->get_view("old");
    const auto        generation = core->capture()->generation;
    const std::string value(9000, 'n');
    auto              status = install(test_import::overflow_tree(value));
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->capture()->generation, generation + 1);
    EXPECT_EQ(tree->contiguous_slot(), 7U);
    EXPECT_EQ(core->capture()->coverage, 7U);
    EXPECT_EQ(old.value().to_string(), "retained");
    EXPECT_FALSE(tree->get_view("old").found());
    EXPECT_EQ(tree->get_view("key").value().to_string(), value);
    const auto primary = core->capture()->mapping.get(0)->leaf->bases.front().backing;
    ASSERT_NE(primary, nullptr);
    EXPECT_TRUE(primary->readable());
    EXPECT_FALSE(primary->reloadable());
    ASSERT_TRUE(tree->put("next", "value").ok());
    EXPECT_EQ(tree->get_view("next").slot(), 8U);
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(tree->get_view("key").value().to_string(), value);
}

TEST_F(TreeCoreNativeImport, EmptyImportKeepsCoverageAndBuildsAUsableEmptyRoot)
{
    ASSERT_TRUE(tree->put("old", "value").ok());
    const auto status = install({}, 17);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree->contiguous_slot(), 17U);
    EXPECT_FALSE(tree->get_view("old").found());
    ASSERT_TRUE(tree->put("new", "value").ok());
    EXPECT_EQ(tree->get_view("new").slot(), 18U);
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(core->capture()->coverage, 18U);
}

TEST_F(TreeCoreNativeImport, InvalidFrameAndGraphLeaveTheOldRootAndWriterUsable)
{
    ASSERT_TRUE(tree->put("old", "value").ok());
    const auto original = core->capture();
    for (size_t variant = 0; variant != 5; ++variant) {
        std::vector<NativeFrame> frames{test_import::inline_leaf()};
        if (variant == 0) {
            frames[0].frame.back() ^= 1;
        }
        else if (variant == 1) {
            frames.push_back(frames[0]);
        }
        else if (variant == 2) {
            frames.push_back(test_import::inline_leaf(1, "unreachable"));
        }
        else if (variant == 3) {
            frames[0] = test_import::inline_leaf(0, "future", "value", 8);
        }
        else {
            ASSERT_TRUE(inner_frame_build(frames[0].frame.data(), 4096, 0, {0}, {}));
        }
        const auto status = install(std::move(frames));
        EXPECT_EQ(status.code(), Code::kCorruption) << variant << ": " << status.to_string();
        EXPECT_EQ(core->capture(), original);
        EXPECT_EQ(tree->get_view("old").value().to_string(), "value");
    }
    ASSERT_TRUE(tree->put("new", "value").ok());
    EXPECT_EQ(tree->get_view("new").slot(), 2U);
}

TEST_F(TreeCoreNativeImport, StreamingImportUsesRemainingHostCapacity)
{
    auto       peer_config                           = config;
    const auto host_limit                            = TreeRuntimeOptions{}.limits.reclaim_bytes;
    peer_config.runtime_binding.limits.reclaim_bytes = host_limit;
    Crowdbtree peer(peer_config);
    auto pressure = detail::ResourceBudget::try_reserve(TreeCoreAccess::get(peer)->binding()->account(),
                                                        detail::BudgetClass::kReclaim,
                                                        host_limit - (config.runtime_binding.limits.reclaim_bytes / 2));
    ASSERT_TRUE(pressure);
    std::unique_ptr<SnapshotExport> exported;
    ASSERT_TRUE(snapshot_export_begin(peer, snapshot_format::kNative, 4096, &exported).ok());
    SnapshotImport imported(*tree);
    bool           done = false;
    while (!done) {
        std::string bytes;
        ASSERT_TRUE(exported->next_chunk(&bytes, &done).ok());
        const auto status = imported.feed(Slice(bytes));
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    ASSERT_TRUE(imported.finish(nullptr).ok());
    EXPECT_EQ(core->contiguous_slot(), 0U);
}

TEST_F(TreeCoreNativeImport, FailedStagingAdmissionPreservesTheOriginalGeneration)
{
    ASSERT_TRUE(tree->put("old", "value").ok());
    const auto original = core->capture();
    auto       pressure = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kReclaim,
                                                              config.runtime_binding.limits.reclaim_bytes);
    ASSERT_TRUE(pressure);
    EXPECT_EQ(install({test_import::inline_leaf()}).code(), Code::kResourceExhausted);
    EXPECT_EQ(core->capture(), original);
    pressure.reset();
    const auto status = install({test_import::inline_leaf()});
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree->get_view("key").value().to_string(), "imported");
}
} // namespace
} // namespace crowdb::tree
