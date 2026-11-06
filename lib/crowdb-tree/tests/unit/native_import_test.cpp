// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/overflow_backing.h"
#include "inner_load_test_support.h"
#include "load_budget_test_support.h"
#include "native_import_test_support.h"
#include "snapshot/operation_wait.h"
#include "snapshot/recovered_state.h"

namespace crowdb::tree
{
namespace
{
class NativeImport : public testing::Test
{
  protected:
    Status catalog(const std::vector<NativeFrame> &frames, std::shared_ptr<ReadablePageCatalog> *out) const
    {
        return build_native_catalog(frames, test_import::info(frames), {}, {}, {}, credits.credits.reservation(), out);
    }

    test_read::LoadCredits                      credits;
    std::shared_ptr<detail::FlushRuntime>       runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    std::shared_ptr<detail::FlushRuntime::Lane> lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<test_read::TestInnerStore>  store   = std::make_shared<test_read::TestInnerStore>();
    std::shared_ptr<AsyncStorePageLoader>       loader  = std::make_shared<AsyncStorePageLoader>(
        store, runtime, lane, PageLoadLimits{}, OverflowBackingResolver{}, credits.credits);
};

TEST_F(NativeImport, ResidentLeafAndOverflowUseOwnedFramesWithoutBackendIo)
{
    loader = std::make_shared<AsyncStorePageLoader>(nullptr, runtime, lane, PageLoadLimits{}, OverflowBackingResolver{},
                                                    credits.credits);
    const std::string                    value(9000, 'v');
    auto                                 frames = test_import::overflow_tree(value);
    std::shared_ptr<ReadablePageCatalog> pages;
    auto                                 status = catalog(frames, &pages);
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto primary = pages->frames.at(0).backing;
    EXPECT_TRUE(primary->readable());
    EXPECT_FALSE(primary->reloadable());
    EXPECT_EQ(primary->address, 0U);
    EXPECT_EQ(primary->length, 0U);
    frames.clear();
    pages.reset();
    std::shared_ptr<const LeafBase> leaf;
    loader->load_leaf({.identity = 1, .max_slot = 7, .backing = primary}, [&](Status result, auto loaded) {
        EXPECT_TRUE(result.ok()) << result.to_string();
        leaf = std::move(loaded);
    });
    runtime->drain();
    ASSERT_NE(leaf, nullptr);
    EXPECT_EQ(leaf->view().self_page_id(), 0U);
    std::shared_ptr<const buffer> loaded_value;
    loader->load_overflow(primary, 1, value.size(), [&](Status result, auto loaded) {
        EXPECT_TRUE(result.ok()) << result.to_string();
        loaded_value = std::move(loaded);
    });
    runtime->drain();
    ASSERT_NE(loaded_value, nullptr);
    EXPECT_EQ(Slice(*loaded_value).to_string(), value);
    EXPECT_EQ(store->reads.load(), 0U);
    primary.reset();
    leaf.reset();
    loaded_value.reset();
    loader.reset();
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], 0U);
}

TEST_F(NativeImport, ReadableGraphLoadsResidentIndexButDurableGraphRejectsIt)
{
    loader = std::make_shared<AsyncStorePageLoader>(nullptr, runtime, lane, PageLoadLimits{}, OverflowBackingResolver{},
                                                    credits.credits);
    NativeFrame index{.page_id = 0, .frame = std::vector<uint8_t>(4096)};
    ASSERT_TRUE(inner_frame_build(index.frame.data(), index.frame.size(), 0, {1, 2}, {Slice("m")}));
    const auto               cell = encode_cell_buf(7, OpKind::kPut, "left");
    std::vector<NativeFrame> frames;
    frames.push_back(std::move(index));
    frames.push_back(test_import::leaf(1, "a", Slice(cell), 2));
    frames.push_back(test_import::inline_leaf(2, "z", "right"));
    std::shared_ptr<ReadablePageCatalog> pages;
    auto                                 status = catalog(frames, &pages);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::shared_ptr<RecoveredStateOperation> operation;
    ASSERT_TRUE(RecoveredStateOperation::create_readable(pages, loader, runtime, lane, {},
                                                         credits.credits.reservation(), &operation)
                    .ok());
    status = detail::wait_snapshot_operation(operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(operation->result().state->stats.height, 2U);
    EXPECT_EQ(operation->result().state->stats.kv_known_count, 2U);
    EXPECT_EQ(store->reads.load(), 0U);
    auto durable =
        detail::reserved_shared<CapturedDurablePages>(credits.credits.reservation(), credits.credits.reservation());
    static_cast<ReadablePageCatalog &>(*durable) = *pages;
    operation.reset();
    ASSERT_TRUE(
        RecoveredStateOperation::create(durable, loader, runtime, lane, {}, credits.credits.reservation(), &operation)
            .ok());
    EXPECT_EQ(detail::wait_snapshot_operation(operation).code(), Code::kCorruption);
}

TEST_F(NativeImport, DurableReadsStillRequireABackend)
{
    auto backing                    = std::make_shared<BackingRef>();
    backing->length                 = 4096;
    backing->owner                  = std::make_shared<int>(0);
    OverflowBackingResolver resolve = [backing](uint64_t, std::shared_ptr<const BackingRef> *out) {
        *out = backing;
        return Status::Ok();
    };
    AsyncStorePageLoader missing(nullptr, runtime, lane, {}, std::move(resolve), credits.credits);
    size_t               completed = 0;
    auto                 failed    = [&](Status status, const auto &result) {
        EXPECT_EQ(status.code(), Code::kInvalidArgument) << status.to_string();
        EXPECT_FALSE(result);
        ++completed;
    };
    missing.load_leaf({.identity = 1, .max_slot = 7, .backing = backing}, failed);
    missing.load_inner(backing, failed);
    missing.load_overflow(1, 10, failed);
    runtime->drain();
    EXPECT_EQ(completed, 3U);
    EXPECT_EQ(store->reads.load(), 0U);
}

TEST_F(NativeImport, OverwrittenOverflowCellStillRetainsItsPhysicalDependency)
{
    auto                    frames = test_import::overflow_tree("old-overflow");
    std::vector<uint8_t>    updated(4096);
    std::vector<leaf_entry> delta;
    delta.push_back({.key = "key", .cell = encode_cell_buf(7, OpKind::kPut, "latest")});
    ASSERT_TRUE(leaf_frame_append_deltas(frames[0].frame.data(), 4096, delta, updated.data()));
    frames[0].frame = std::move(updated);
    std::shared_ptr<ReadablePageCatalog> pages;
    auto                                 status = catalog(frames, &pages);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(pages->frames.at(0).stored_overflow->size(), 1U);
    std::shared_ptr<RecoveredStateOperation> operation;
    ASSERT_TRUE(RecoveredStateOperation::create_readable(pages, loader, runtime, lane, {},
                                                         credits.credits.reservation(), &operation)
                    .ok());
    status = detail::wait_snapshot_operation(operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto leaf = operation->result().state->mapping.get(0)->leaf;
    EXPECT_EQ(leaf->bases.front().backing->overflow->pages.size(), 1U);
    EXPECT_EQ(store->reads.load(), 0U);
}

TEST_F(NativeImport, ResidentLoaderValidatesCrcAndRetainsNoChargeAfterFailure)
{
    auto raw = std::make_shared<std::vector<uint8_t>>(test_import::inline_leaf().frame);
    raw->back() ^= 1;
    auto backing            = std::make_shared<BackingRef>();
    backing->storage        = BackingStorage::kResidentFrame;
    backing->durability     = BackingDurability::kResident;
    backing->resident_frame = *raw;
    backing->owner          = raw;
    size_t completed        = 0;
    loader->load_leaf({.identity = 1, .max_slot = 7, .backing = backing}, [&](Status status, auto result) {
        EXPECT_EQ(status.code(), Code::kCorruption);
        EXPECT_FALSE(result);
        ++completed;
    });
    runtime->drain();
    EXPECT_EQ(completed, 1U);
    EXPECT_EQ(store->reads.load(), 0U);
    loader.reset();
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kBytes], 0U);
    EXPECT_EQ(credits.budget.snapshot().allocated[test_read::LoadCredits::kIo], 0U);
}
} // namespace
} // namespace crowdb::tree
