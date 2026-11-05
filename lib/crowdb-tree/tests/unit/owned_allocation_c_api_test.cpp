// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/source_write_budget.h"
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "c_api_owned_allocation.h"
#include "crowdb-tree/owned_allocation_c_api.h"
#include "crowdb-tree/runtime_c_api.h"

#include <gtest/gtest.h>

#include <array>
#include <set>

namespace crowdb::tree
{
namespace
{
struct TestHost
{
    TestHost()
    {
        ct_options options{};
        if (ct_runtime_create(nullptr, &runtime) != 0 || ct_open_with_runtime(&options, runtime, nullptr, &tree) != 0) {
            throw std::runtime_error("runtime Tree initialization failed");
        }
    }

    ~TestHost()
    {
        ct_close(tree);
        ct_runtime_free(runtime);
    }

    [[nodiscard]] TreeCore *core() const
    {
        return TreeCoreAccess::get(*c_api_tree(tree));
    }

    ct_runtime *runtime = nullptr;
    ct_tree    *tree    = nullptr;
};

struct TestPayload
{
    std::array<uint8_t, 8192> bytes{};
    size_t                   *drops;
};

void drop_payload(void *context)
{
    auto *payload = static_cast<TestPayload *>(context);
    ++*payload->drops;
    delete payload;
}

ct_status create_owner(ct_tree *tree, size_t *drops, ct_owned_allocation **out, const uint8_t **data = nullptr)
{
    auto *payload = new TestPayload{.bytes = {}, .drops = drops};
    payload->bytes.fill('v');
    if (data != nullptr) {
        *data = payload->bytes.data();
    }
    return ct_owned_allocation_create(tree, payload->bytes.data(), 256, payload->bytes.size(), drop_payload, payload,
                                      sizeof(payload->drops), out);
}

ct_owned_kv_ref input(ct_owned_allocation *owner, size_t offset = 0, size_t length = 128)
{
    static constexpr uint8_t key[] = {'k'};
    return {.key = key, .key_len = 1, .owner = owner, .offset = offset, .length = length, .kind = 0};
}

size_t payload_bytes(const std::shared_ptr<detail::ResourceBudget::Account> &account)
{
    return account->snapshot().physical[static_cast<size_t>(AllocationKind::kPayload)];
}

TEST(OwnedAllocationCAbi, SlicesRetainOneCapacityAndFinalForeignDrop)
{
    TestHost             host;
    const auto           account = host.core()->binding()->account();
    const auto           before  = payload_bytes(account);
    size_t               drops   = 0;
    ct_owned_allocation *owner   = nullptr;
    const uint8_t       *data    = nullptr;
    ASSERT_EQ(create_owner(host.tree, &drops, &owner, &data), 0);
    EXPECT_EQ(payload_bytes(account) - before, 8192U);
    const auto operation = input(owner, 16, 128);
    ASSERT_EQ(ct_apply_batch_owned(host.tree, 1, &operation, 1), 0);
    host.core()->binding()->drain();
    ct_owned_allocation_retain(owner);
    ct_owned_allocation_free(owner);
    GetView view;
    ASSERT_TRUE(host.core()->get(Slice("k"), &view).ok());
    EXPECT_EQ(view.value().bytes(), data + 16);
    view = {};
    ct_owned_allocation_free(owner);
    EXPECT_EQ(drops, 0U);
    ct_close(std::exchange(host.tree, nullptr));
    EXPECT_EQ(drops, 1U);
    EXPECT_EQ(payload_bytes(account), before);
}

TEST(OwnedAllocationCAbi, CrossHostCopiesAndSourceOwnerCanBeDropped)
{
    TestHost             source;
    TestHost             target;
    size_t               drops = 0;
    ct_owned_allocation *owner = nullptr;
    ASSERT_EQ(create_owner(source.tree, &drops, &owner), 0);
    const auto operation = input(owner);
    const auto before    = target.core()->state_stats().resources.payload_bytes;
    ASSERT_EQ(ct_apply_batch_owned(target.tree, 1, &operation, 1), 0);
    EXPECT_EQ(target.core()->state_stats().resources.payload_bytes - before, 128U + kCellHeaderSize);
    ct_owned_allocation_free(owner);
    EXPECT_EQ(drops, 1U);
    GetView view;
    ASSERT_TRUE(target.core()->get(Slice("k"), &view).ok());
    EXPECT_EQ(view.value().to_string(), std::string(128, 'v'));
}

TEST(OwnedAllocationCAbi, InvalidSliceAndAdmissionFailureReleaseExactlyOnce)
{
    TestHost             host;
    size_t               drops = 0;
    ct_owned_allocation *owner = nullptr;
    ASSERT_EQ(create_owner(host.tree, &drops, &owner), 0);
    const auto operation = input(owner, 256, 1);
    EXPECT_EQ(ct_apply_batch_owned(host.tree, 1, &operation, 1), static_cast<int>(Code::kInvalidArgument));
    EXPECT_EQ(drops, 0U);
    ct_owned_allocation_free(owner);
    EXPECT_EQ(drops, 1U);
    ct_runtime_stop_admission(host.runtime);
    EXPECT_NE(create_owner(host.tree, &drops, &owner), 0);
    EXPECT_EQ(owner, nullptr);
    EXPECT_EQ(drops, 2U);
}

TEST(OwnedAllocationCAbi, UnsubmittedControlAndPayloadOutliveTreeAndRuntime)
{
    TestHost             host;
    const auto           account = host.core()->binding()->account();
    size_t               drops   = 0;
    ct_owned_allocation *owner   = nullptr;
    ASSERT_EQ(create_owner(host.tree, &drops, &owner), 0);
    ct_close(std::exchange(host.tree, nullptr));
    ct_runtime_free(std::exchange(host.runtime, nullptr));
    EXPECT_EQ(drops, 0U);
    EXPECT_EQ(payload_bytes(account), 8192U);
    ct_owned_allocation_free(owner);
    EXPECT_EQ(drops, 1U);
    EXPECT_EQ(payload_bytes(account), 0U);
    // The retained observation account has no remaining foreign allocations.
    EXPECT_EQ(account->snapshot().allocated[static_cast<size_t>(detail::BudgetClass::kAdmittedWriter)], 0U);
}

TEST(OwnedAllocationCAbi, FrozenSourceKeepsExternalSlicesAfterTreeClose)
{
    TestHost             host;
    size_t               drops = 0;
    ct_owned_allocation *owner = nullptr;
    const uint8_t       *data  = nullptr;
    ASSERT_EQ(create_owner(host.tree, &drops, &owner, &data), 0);
    auto                           account = host.core()->binding()->account();
    std::array<ct_owned_kv_ref, 2> operations{input(owner, 0, 128), input(owner, 128, 128)};
    static constexpr uint8_t       second_key[] = {'z'};
    operations[1].key                           = second_key;
    ASSERT_EQ(ct_apply_batch_owned(host.tree, 1, operations.data(), operations.size()), 0);
    auto catalog = host.core()->capture()->sources;
    auto runtime = host.core()->binding();
    ct_owned_allocation_free(owner);
    ct_close(std::exchange(host.tree, nullptr));
    runtime->drain();
    EXPECT_EQ(drops, 0U);
    EXPECT_EQ(payload_bytes(account), 8192U);
    size_t cells = 0;
    for (const auto &table : catalog->tables) {
        auto guard = table->read_guard();
        for (auto cursor = table->prefix_cursor(1); cursor.valid(); cursor.advance()) {
            const auto cell = cursor.cell_owner();
            ASSERT_TRUE(cell);
            EXPECT_EQ(cell->cell.data(), data + (cells * 128));
            EXPECT_EQ(cell->cell.size(), 128U);
            ++cells;
        }
    }
    EXPECT_EQ(cells, 2U);
    catalog.reset();
    runtime->drain();
    EXPECT_EQ(drops, 1U);
    EXPECT_EQ(payload_bytes(account), 0U);
}

TEST(OwnedAllocationCAbi, ExactWriterCreditPreparesOwnersBeforeCommit)
{
    TestHost             host;
    const auto           initial = host.core()->binding()->account()->snapshot().allocated[static_cast<size_t>(
        detail::BudgetClass::kAdmittedWriter)];
    size_t               drops   = 0;
    ct_owned_allocation *owner   = nullptr;
    ASSERT_EQ(create_owner(host.tree, &drops, &owner), 0);
    auto       account  = host.core()->binding()->account();
    const auto category = detail::BudgetClass::kAdmittedWriter;
    const auto baseline = account->snapshot().allocated[static_cast<size_t>(category)];
    buffer     value;
    ASSERT_TRUE(owner->bridge->slice(0, 128, &value).ok());
    SourceWriteOptions options;
    options.account         = account;
    options.max_entries     = 4;
    options.max_gap_entries = 1;
    const WriteFootprint footprint{.entries = 1, .key_bytes = 3, .payload_bytes = 0};
    const auto           bound = SourceWriteBudget::allocation_bound(options, footprint);
    if (!bound) {
        FAIL() << "writer allocation bound overflow";
    }
    auto reserved = detail::ResourceBudget::try_reserve(account, category, *bound);
    if (!reserved) {
        FAIL() << "writer allocation reserve unavailable";
    }
    auto metadata = detail::AllocationReservation::create(std::move(*reserved));
    {
        MemTable table(1, nullptr, host.core()->binding()->lifecycle_budget());
        auto     prepared = table.prepare_write("key", std::move(value), 0, {}, 0, 5, metadata);
        metadata->seal();
        ASSERT_TRUE(table.commit_write(std::move(prepared), 1));
        auto guard = table.read_guard();
        auto cell  = table.prefix_cursor(1).cell_owner();
        ASSERT_TRUE(cell);
        EXPECT_EQ(cell->cell.size(), 128U);
    }
    metadata.reset();
    host.core()->binding()->drain();
    // The independent slice holder has also left; all writer preparation
    // allocations have returned after their last owner was released.
    EXPECT_EQ(account->snapshot().allocated[static_cast<size_t>(category)], baseline);
    ct_owned_allocation_free(owner);
    host.core()->binding()->drain();
    EXPECT_EQ(drops, 1U);
    EXPECT_EQ(account->snapshot().allocated[static_cast<size_t>(category)], initial);
    ct_close(std::exchange(host.tree, nullptr));
    EXPECT_EQ(account->snapshot().allocated[static_cast<size_t>(category)], 0U);
}
} // namespace
} // namespace crowdb::tree
