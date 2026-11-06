// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "btree/tree_core_access.h"
#include "crowdb-tree/btree/tree.h"
#include "crowdb-tree/external_allocation.h"

#include <gtest/gtest.h>

#include <array>
#include <future>

namespace crowdb::tree
{
namespace
{
class TreeCorePublic : public testing::Test
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

    void put(uint64_t slot, std::string key, std::string value)
    {
        const auto status =
            tree->apply(slot, Batch{.ops = {{.key = std::move(key), .kind = OpKind::kPut, .value = std::move(value)}}});
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
};

TEST_F(TreeCorePublic, OutOfOrderSlotsAndNoopKeepFutureValuesAcrossFiniteFlush)
{
    put(2, "future", "second");
    EXPECT_EQ(tree->last_applied_slot(), 0U);
    EXPECT_EQ(tree->contiguous_slot(), 0U);
    ASSERT_TRUE(tree->apply(4, {}).ok());
    EXPECT_EQ(tree->contiguous_slot(), 0U);
    put(1, "first", "one");
    EXPECT_EQ(tree->contiguous_slot(), 2U);
    const auto flushed = tree->flush();
    ASSERT_TRUE(flushed.ok()) << flushed.to_string();
    EXPECT_EQ(tree->get_view("future").value().to_string(), "second");
    EXPECT_EQ(tree->get_view("first").value().to_string(), "one");
    ASSERT_TRUE(tree->apply(3, {}).ok());
    EXPECT_EQ(tree->contiguous_slot(), 4U);
    EXPECT_EQ(tree->last_applied_slot(), 2U);
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(tree->get_view("future").slot(), 2U);
}

TEST_F(TreeCorePublic, BatchLastKeyWinsAndInvalidEncodedBatchPublishesNothing)
{
    Batch batch{
        .ops = {{.key = "same", .kind = OpKind::kPut, .value = "old"},
                {.key = "same", .kind = OpKind::kPut, .value = "latest"},
                {.key = "gone", .kind = OpKind::kDelete, .value = ""}}
    };
    ASSERT_TRUE(tree->apply(1, batch).ok());
    EXPECT_EQ(tree->get_view("same").value().to_string(), "latest");
    EXPECT_FALSE(tree->get_view("gone").found());
    std::vector<Crowdbtree::encoded_op> invalid;
    invalid.push_back({.key = "new", .cell = encode_cell_buf(2, OpKind::kPut, Slice("valid"))});
    invalid.push_back({.key = "same", .cell = encode_cell_buf(3, OpKind::kPut, Slice("invalid-slot"))});
    EXPECT_EQ(tree->apply_encoded(2, std::move(invalid)).code(), Code::kInvalidArgument);
    EXPECT_FALSE(tree->get_view("new").found());
    EXPECT_EQ(tree->get_view("same").value().to_string(), "latest");
    EXPECT_EQ(tree->contiguous_slot(), 1U);
}

TEST_F(TreeCorePublic, SingleWorkerAsyncFlushAndRetainedReadOutliveFacade)
{
    put(1, "key", std::string(1024, 'v'));
    auto held = tree->get_view("key");
    ASSERT_TRUE(held.found());
    std::promise<Status> complete;
    auto                 done = complete.get_future();
    tree->flush_async([&](Status status) { complete.set_value(std::move(status)); });
    const auto flushed = done.get();
    ASSERT_TRUE(flushed.ok()) << flushed.to_string();
    tree.reset();
    EXPECT_EQ(held.value().to_string(), std::string(1024, 'v'));
    EXPECT_GT(config.runtime->snapshot().allocated.writer_bytes, 0U);
    held = {};
    config.runtime->drain_cpu();
    EXPECT_EQ(config.runtime->snapshot().allocated.writer_bytes, 0U);
}

TEST_F(TreeCorePublic, UnfundedBatchDoesNotChangeAnyKeyOrFrontier)
{
    tree.reset();
    config.runtime_binding.limits.writer_bytes = 2 * 1024 * 1024;
    tree                                       = std::make_unique<Crowdbtree>(config);
    put(1, "existing", "before");
    const auto large = config.runtime_binding.limits.writer_bytes;
    Batch      batch{
        .ops = {{.key = "existing", .kind = OpKind::kPut, .value = "after"},
                {.key = "oversized", .kind = OpKind::kPut, .value = std::string(large, 'x')}}
    };
    EXPECT_FALSE(tree->apply(2, batch).ok());
    EXPECT_EQ(tree->get_view("existing").value().to_string(), "before");
    EXPECT_FALSE(tree->get_view("oversized").found());
    EXPECT_EQ(tree->last_applied_slot(), 0U);
    EXPECT_EQ(tree->contiguous_slot(), 1U);
}

TEST_F(TreeCorePublic, DefaultMaximumValueBatchUsesOnePayloadReservation)
{
    const auto maximum = TreeCoreAccess::get(*tree)->load_limits().max_value_bytes;
    Batch      batch{
        .ops = {{.key = "large", .kind = OpKind::kPut, .value = std::string(maximum, 'v')},
                {.key = "small", .kind = OpKind::kPut, .value = "same-batch"}}
    };
    const auto status = tree->apply(1, batch);
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto value = tree->get_view("large");
    ASSERT_TRUE(value.found());
    EXPECT_EQ(value.value().size(), maximum);
    EXPECT_EQ(value.value().data()[0], 'v');
    EXPECT_EQ(value.value().data()[maximum - 1], 'v');
    EXPECT_EQ(tree->get_view("small").value().to_string(), "same-batch");
    EXPECT_EQ(tree->contiguous_slot(), 1U);
    batch.ops[0].value.push_back('x');
    batch.ops[1].value = "must-not-apply";
    EXPECT_EQ(tree->apply(2, batch).code(), Code::kInvalidArgument);
    EXPECT_EQ(tree->get_view("small").value().to_string(), "same-batch");
    EXPECT_EQ(tree->contiguous_slot(), 1U);
    std::vector<Crowdbtree::encoded_op> encoded;
    encoded.push_back({.key = "oversized", .cell = encode_cell_buf(2, OpKind::kPut, Slice(batch.ops[0].value))});
    EXPECT_EQ(tree->apply_encoded(2, std::move(encoded)).code(), Code::kInvalidArgument);
    std::vector<Crowdbtree::external_op> external;
    const auto                           borrowed = Slice(batch.ops[0].value);
    external.push_back({.key = "oversized", .flags = 0, .value = buffer::wrap(borrowed.bytes(), borrowed.size())});
    EXPECT_EQ(tree->apply_external(2, std::move(external)).code(), Code::kInvalidArgument);
    batch.ops[0].kind  = OpKind::kDelete;
    batch.ops[1].value = "deleted";
    ASSERT_TRUE(tree->apply(2, batch).ok());
    EXPECT_FALSE(tree->get_view("large").found());
    EXPECT_EQ(tree->get_view("small").value().to_string(), "deleted");
}

TEST_F(TreeCorePublic, KnownExternalCapacityIsNotReservedAgainDuringBatchStaging)
{
    tree.reset();
    config.runtime_binding.limits.writer_bytes = 2 * 1024 * 1024;
    tree                                       = std::make_unique<Crowdbtree>(config);

    struct Payload
    {
        std::array<uint8_t, 1536 * 1024> bytes;
        size_t                          *drops;
    };

    size_t drops   = 0;
    auto  *payload = new Payload{.bytes = {}, .drops = &drops};
    payload->bytes.fill('v');
    ExternalAllocation::Owner owner(payload, [](void *context) {
        auto *owned = static_cast<Payload *>(context);
        ++*owned->drops;
        delete owned;
    });
    auto credit = detail::ResourceBudget::try_reserve(TreeCoreAccess::get(*tree)->binding()->account(),
                                                      detail::BudgetClass::kAdmittedWriter, sizeof(Payload) + 4096);
    if (!credit) {
        FAIL() << "external allocation credit rejected";
    }
    auto reservation = detail::AllocationReservation::create(std::move(credit.value()));
    auto allocation = ExternalAllocation::create(payload->bytes.data(), sizeof(Payload), std::move(owner), reservation);
    std::vector<Crowdbtree::external_op> operations;
    operations.push_back(
        {.key = "external", .flags = 0, .value = buffer::share_external(allocation, 0, payload->bytes.size())});
    reservation->seal();
    reservation.reset();
    allocation.reset();
    const auto status = tree->apply_external(1, std::move(operations));
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto held = tree->get_view("external");
    ASSERT_TRUE(held.found());
    EXPECT_EQ(held.value().data(), reinterpret_cast<const char *>(payload->bytes.data()));
    EXPECT_EQ(drops, 0U);
    tree.reset();
    EXPECT_EQ(drops, 0U);
    EXPECT_EQ(held.value().size(), 1536U * 1024);
    held = {};
    EXPECT_EQ(drops, 1U);
    // Reference metadata is retired after the final external reader exits.
    config.runtime->drain_cpu();
    EXPECT_EQ(config.runtime->snapshot().allocated.writer_bytes, 0U);
}
} // namespace
} // namespace crowdb::tree
