// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/apply_frontier.h"
#include "btree/tree_core_access.h"
#include "crowdb-tree/btree/tree.h"

#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <set>
#include <thread>

namespace crowdb::tree
{
namespace
{
class TreeCoreAutoWrite : public testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions host;
        host.workers = 1;
        ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        config.frame_bytes       = 4096;
        config.leaf_split_bytes  = 3000;
        config.buffer_pool_bytes = 4 * 1024 * 1024;
        tree                     = std::make_unique<Crowdbtree>(config);
    }

    static Batch values(const std::string &value)
    {
        return {
            .ops = {{.key = "a", .kind = OpKind::kPut, .value = value},
                    {.key = "b", .kind = OpKind::kPut, .value = value}}
        };
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
};

TEST_F(TreeCoreAutoWrite, ExplicitWritesNoopsAndForceAdvancePrecedeAutomaticSlots)
{
    ASSERT_TRUE(tree->apply(1, values("explicit")).ok());
    ASSERT_TRUE(tree->put("a", "auto").ok());
    EXPECT_EQ(tree->get_view("a").slot(), 2U);
    ASSERT_TRUE(tree->apply(4, {}).ok());
    ASSERT_TRUE(tree->batch_put(values("batch")).ok());
    EXPECT_EQ(tree->get_view("a").slot(), 5U);
    EXPECT_EQ(tree->get_view("b").slot(), 5U);
    EXPECT_EQ(tree->contiguous_slot(), 2U);
    ASSERT_TRUE(tree->apply(3, {}).ok());
    EXPECT_EQ(tree->contiguous_slot(), 5U);
    ASSERT_TRUE(tree->del("a").ok());
    EXPECT_EQ(tree->contiguous_slot(), 6U);
    EXPECT_FALSE(tree->get_view("a").found());
    tree->force_advance_slot(100);
    ASSERT_TRUE(tree->put("a", "after-force").ok());
    EXPECT_EQ(tree->get_view("a").slot(), 101U);
    EXPECT_EQ(tree->contiguous_slot(), 101U);
}

TEST_F(TreeCoreAutoWrite, RejectedBatchDoesNotConsumeAnAutomaticSlot)
{
    tree.reset();
    config.runtime_binding.limits.writer_bytes = 2 * 1024 * 1024;
    tree                                       = std::make_unique<Crowdbtree>(config);
    EXPECT_EQ(tree->put(std::string(config.frame_bytes, 'k'), "invalid").code(), Code::kInvalidArgument);
    auto oversized = values("unused");
    oversized.ops[1].value.assign(config.runtime_binding.limits.writer_bytes, 'x');
    EXPECT_FALSE(tree->batch_put(oversized).ok());
    EXPECT_EQ(tree->contiguous_slot(), 0U);
    EXPECT_FALSE(tree->get_view("a").found());
    ASSERT_TRUE(tree->put("a", "first").ok());
    EXPECT_EQ(tree->get_view("a").slot(), 1U);
    ASSERT_TRUE(tree->batch_put({}).ok());
    EXPECT_EQ(tree->contiguous_slot(), 2U);
}

TEST_F(TreeCoreAutoWrite, SlotWindowAndIntegerExhaustionCannotPublishAPartialBatch)
{
    ASSERT_TRUE(tree->apply(ApplyFrontier::kMaxWindow, {}).ok());
    EXPECT_EQ(tree->batch_put(values("outside-window")).code(), Code::kResourceExhausted);
    EXPECT_FALSE(tree->get_view("a").found());
    tree->force_advance_slot(ApplyFrontier::kMaxWindow);
    ASSERT_TRUE(tree->batch_put(values("next")).ok());
    EXPECT_EQ(tree->get_view("a").slot(), ApplyFrontier::kMaxWindow + 1);
    tree->force_advance_slot(UINT64_MAX);
    EXPECT_EQ(tree->batch_put(values("wrapped")).code(), Code::kResourceExhausted);
    EXPECT_EQ(tree->get_view("a").value().to_string(), "next");
}

TEST_F(TreeCoreAutoWrite, ConcurrentBatchesUseDistinctSlotsAndLeaveNoGap)
{
    constexpr size_t                 threads    = 4;
    constexpr size_t                 per_thread = 16;
    std::barrier                     start(threads);
    std::array<std::thread, threads> writers;
    for (size_t thread = 0; thread < threads; ++thread) {
        writers[thread] = std::thread([&, thread] {
            start.arrive_and_wait();
            for (size_t index = 0; index < per_thread; ++index) {
                const auto key    = std::to_string(thread) + "/" + std::to_string(index);
                const auto status = tree->put(Slice(key), "v");
                EXPECT_TRUE(status.ok()) << status.to_string();
            }
        });
    }
    for (auto &writer : writers) {
        writer.join();
    }
    EXPECT_EQ(tree->contiguous_slot(), threads * per_thread);
    std::set<uint64_t> slots;
    for (size_t thread = 0; thread < threads; ++thread) {
        for (size_t index = 0; index < per_thread; ++index) {
            const auto key   = std::to_string(thread) + "/" + std::to_string(index);
            auto       value = tree->get_view(Slice(key));
            ASSERT_TRUE(value.found());
            slots.insert(value.slot());
        }
    }
    EXPECT_EQ(slots.size(), threads * per_thread);
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(tree->last_applied_slot(), threads * per_thread);
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST_F(TreeCoreAutoWrite, SecondPreparationFailureKeepsAllKeysAndAutomaticSlotUnchanged)
{
    ASSERT_TRUE(tree->apply(1, values("old")).ok());
    auto table = TreeCoreAccess::get(*tree)->capture()->sources->tables.back();
    table->set_hook_for_tests(nullptr, [](void *, ConcurrentSkipList::PausePoint point, Slice key) {
        if (point == ConcurrentSkipList::PausePoint::kBeforePrepare && key == Slice("b")) {
            throw std::bad_alloc();
        }
    });
    EXPECT_EQ(tree->batch_put(values("new")).code(), Code::kResourceExhausted);
    EXPECT_EQ(tree->get_view("a").value().to_string(), "old");
    EXPECT_EQ(tree->get_view("b").value().to_string(), "old");
    EXPECT_EQ(tree->contiguous_slot(), 1U);
    EXPECT_EQ(table->writers(), 0U);
    table->set_hook_for_tests(nullptr, nullptr);
    ASSERT_TRUE(tree->batch_put(values("new")).ok());
    EXPECT_EQ(tree->get_view("a").slot(), 2U);
    EXPECT_EQ(tree->get_view("b").slot(), 2U);
    EXPECT_EQ(tree->contiguous_slot(), 2U);
}

TEST_F(TreeCoreAutoWrite, ExplicitEncodedAndExternalPreparationFailuresRemainFullyRetryable)
{
    ASSERT_TRUE(tree->apply(1, values("old")).ok());
    auto table = TreeCoreAccess::get(*tree)->capture()->sources->tables.back();
    table->set_hook_for_tests(nullptr, [](void *, ConcurrentSkipList::PausePoint point, Slice key) {
        if (point == ConcurrentSkipList::PausePoint::kBeforePrepare && key == Slice("b")) {
            throw std::bad_alloc();
        }
    });
    EXPECT_EQ(tree->apply(2, values("new")).code(), Code::kResourceExhausted);
    std::vector<Crowdbtree::encoded_op>  encoded;
    std::vector<Crowdbtree::external_op> external;
    for (const auto *key : {"a", "b"}) {
        encoded.push_back({.key = key, .cell = encode_cell_buf(2, OpKind::kPut, "encoded")});
        external.push_back({.key = key, .flags = 0, .value = buffer::copy_of("external")});
    }
    EXPECT_EQ(tree->apply_encoded(2, std::move(encoded)).code(), Code::kResourceExhausted);
    EXPECT_EQ(tree->apply_external(2, std::move(external)).code(), Code::kResourceExhausted);
    EXPECT_EQ(tree->get_view("a").slot(), 1U);
    EXPECT_EQ(tree->get_view("b").slot(), 1U);
    EXPECT_EQ(tree->contiguous_slot(), 1U);
    EXPECT_EQ(table->writers(), 0U);
    table->set_hook_for_tests(nullptr, nullptr);
    ASSERT_TRUE(tree->apply(2, values("retry")).ok());
    ASSERT_TRUE(tree->apply(2, values("duplicate")).ok());
    EXPECT_EQ(tree->get_view("a").value().to_string(), "retry");
    EXPECT_EQ(tree->get_view("b").value().to_string(), "retry");
    EXPECT_EQ(tree->contiguous_slot(), 2U);
}
#endif
} // namespace
} // namespace crowdb::tree
