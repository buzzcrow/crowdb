// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/local_backend_state.h"
#include "backend/local/local_block_gc.h"
#include "btree/tree_core.h"
#include "btree/tree_core_access.h"
#include "crowdb-tree/crowdb-tree.h"
#include "extent_ledger_test_support.h"

#include <array>

namespace crowdb::tree
{
namespace
{
using namespace test_read;
constexpr uint64_t kPage = MemPageStore::kBackingPageBytes;

TEST(MemoryBackingGc, PartialPagesAndReadersRemainProtectedUntilTheirLastOwnerDrops)
{
    MemPageStore               store;
    const std::vector<uint8_t> input(3 * kPage, 42);
    ASSERT_TRUE(store.write_at(0, input.data(), input.size()).ok());
    LedgerFixture ledger({.backend_id = 1, .file_size = input.size(), .address_limit = input.size()});
    auto          anchor = ledger.recover(kPage + 4096, 4096);
    ASSERT_TRUE(anchor.status.ok());
    auto reader = anchor.extent.owner;
    ASSERT_TRUE(ledger.activate().status.ok());
    const LocalBlockGcLimits limits{.max_blocks = 1, .max_deleted_bytes = kPage};
    auto first = collect_free_local_blocks(store, *ledger.ledger, 0, limits, ledger.credits.credits.reservation());
    ASSERT_TRUE(first.status.ok());
    EXPECT_EQ(first.protected_blocks, 1U);
    EXPECT_FALSE(first.complete);
    anchor      = {};
    auto second = collect_free_local_blocks(store, *ledger.ledger, first.next_block, limits,
                                            ledger.credits.credits.reservation());
    ASSERT_TRUE(second.status.ok());
    EXPECT_TRUE(second.complete);
    EXPECT_EQ(second.deleted_bytes, kPage);
    EXPECT_EQ(store.resident_bytes(), 2 * kPage);
    uint8_t value = 0;
    ASSERT_TRUE(store.read_at(kPage + 4096, &value, 1).ok());
    EXPECT_EQ(value, 42);
    reader.reset();
    auto third = collect_free_local_blocks(store, *ledger.ledger, 1, limits, ledger.credits.credits.reservation());
    ASSERT_TRUE(third.status.ok());
    EXPECT_EQ(third.deleted_blocks, 1U);
    EXPECT_EQ(store.resident_bytes(), kPage);
    EXPECT_EQ(store.size(), input.size());
    ASSERT_TRUE(store.read_at(kPage + 4096, &value, 1).ok());
    EXPECT_EQ(value, 0);
    ASSERT_TRUE(store.read_at(0, &value, 1).ok());
    EXPECT_EQ(value, 42);
}

TEST(MemoryBackingGc, PendingWritesAndExclusiveDeletionLeasesPreventAddressReuse)
{
    MemPageStore  store;
    LedgerFixture ledger({.backend_id = 1, .region_begin = kPage, .file_size = 2 * kPage, .address_limit = 2 * kPage});
    const uint8_t value = 71;
    ASSERT_TRUE(store.write_at((2 * kPage) - 1, &value, 1).ok());
    ASSERT_TRUE(ledger.activate().status.ok());
    auto pending = ledger.reserve(kPage);
    ASSERT_TRUE(pending.status.ok());
    auto held = collect_free_local_blocks(store, *ledger.ledger, 0, {}, ledger.credits.credits.reservation());
    ASSERT_TRUE(held.status.ok());
    EXPECT_EQ(held.protected_blocks, 1U);
    EXPECT_EQ(held.deleted_bytes, 0U);
    pending       = {};
    auto deletion = await_ledger([&](auto complete) {
        return ledger.ledger->claim_free(kPage, kPage, ledger.credits.credits.reservation(), std::move(complete));
    });
    ASSERT_TRUE(deletion.status.ok());
    EXPECT_EQ(ledger.reserve(kPage).status.code(), Code::kResourceExhausted);
    auto concurrent = collect_free_local_blocks(store, *ledger.ledger, 0, {}, ledger.credits.credits.reservation());
    ASSERT_TRUE(concurrent.status.ok());
    EXPECT_EQ(concurrent.deleted_bytes, 0U);
    deletion      = {};
    auto released = collect_free_local_blocks(store, *ledger.ledger, 0, {}, ledger.credits.credits.reservation());
    ASSERT_TRUE(released.status.ok());
    EXPECT_EQ(released.deleted_bytes, kPage);
    EXPECT_EQ(store.resident_bytes(), 0U);
    auto reuse = ledger.reserve(kPage);
    ASSERT_TRUE(reuse.status.ok());
    EXPECT_EQ(reuse.extent.address, kPage);
    ASSERT_TRUE(store.write_at(reuse.extent.address, &value, 1).ok());
    uint8_t actual = 0;
    ASSERT_TRUE(store.read_at(reuse.extent.address, &actual, 1).ok());
    EXPECT_EQ(actual, value);
}

TEST(MemoryBackingGc, FailedMetadataAdmissionAndPartialTailDoNotReleaseBacking)
{
    MemPageStore               store;
    const std::vector<uint8_t> input((2 * kPage) + 4096, 19);
    ASSERT_TRUE(store.write_at(0, input.data(), input.size()).ok());
    LedgerFixture ledger({.backend_id = 1, .file_size = input.size(), .address_limit = input.size()});
    ASSERT_TRUE(ledger.activate().status.ok());
    ledger.credits.seal();
    auto failed = collect_free_local_blocks(store, *ledger.ledger, 0, {}, ledger.credits.credits.reservation());
    EXPECT_EQ(failed.status.code(), Code::kResourceExhausted);
    EXPECT_EQ(failed.next_block, 1U);
    EXPECT_EQ(store.resident_bytes(), 3 * kPage);
    auto collected = collect_free_local_blocks(store, *ledger.ledger, 0, {}, {});
    ASSERT_TRUE(collected.status.ok());
    EXPECT_TRUE(collected.complete);
    EXPECT_EQ(collected.deleted_blocks, 1U);
    EXPECT_EQ(store.resident_bytes(), 2 * kPage);
    uint8_t tail = 0;
    ASSERT_TRUE(store.read_at(2 * kPage, &tail, 1).ok());
    EXPECT_EQ(tail, 19);
}

TEST(MemoryBackingGc, OnePublicSnapshotReclaimsMultipleWindowsWithoutRewritingTheTree)
{
    auto                       store = std::make_shared<MemPageStore>(4096);
    const std::vector<uint8_t> orphan(160 * kPage, 37);
    ASSERT_TRUE(store->write_at(0, orphan.data(), orphan.size()).ok());
    // Anchors must start absent; only the remaining written backing is orphaned.
    const std::array<uint8_t, 8192> anchors{};
    ASSERT_TRUE(store->write_at(0, anchors.data(), anchors.size()).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.page_store  = store.get();
    config.frame_bytes = 4096;
    std::unique_ptr<Crowdbtree> tree;
    ASSERT_TRUE(Crowdbtree::open(config, &tree).ok());
    ASSERT_TRUE(tree->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "value"}}}).ok());
    uint64_t   coverage = 0;
    const auto status   = tree->snapshot(&coverage);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 1U);
    auto *core = TreeCoreAccess::get(*tree);
    core->operation_lifetime().drain_accepted();
    core->binding()->drain();
    const auto sequence = core->local_backend()->checkpoints->snapshot_sequence();
    EXPECT_EQ(sequence, 1U);
    EXPECT_LT(store->resident_bytes(), 3 * kPage);
    EXPECT_GE(store->size(), orphan.size());
    tree.reset();
    ASSERT_TRUE(Crowdbtree::open(config, &tree).ok());
    GetView actual;
    ASSERT_TRUE(TreeCoreAccess::get(*tree)->get("a", &actual).ok());
    EXPECT_EQ(actual.value().to_string(), "value");
}
} // namespace
} // namespace crowdb::tree
