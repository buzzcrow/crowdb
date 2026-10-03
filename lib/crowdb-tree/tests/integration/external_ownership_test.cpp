// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/c_api.h"

#include <gtest/gtest.h>

#include <array>
#include <limits>

namespace
{
void release(void *context)
{
    ++*static_cast<int *>(context);
}

ct_ext_op external(int *drops, const uint8_t *value, size_t length)
{
    return {.key       = reinterpret_cast<const uint8_t *>("key"),
            .key_len   = 3,
            .value     = value,
            .value_len = length,
            .kind      = 0,
            .bytes_ref = drops,
            .drop_fn   = release};
}
} // namespace

TEST(ExternalOwnership, EmptyValueReleasesItsTransferredOwner)
{
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    int  drops = 0;
    auto op    = external(&drops, nullptr, 0);
    ASSERT_EQ(ct_apply_batch_external(tree, 1, &op, 1), 0);
    EXPECT_EQ(drops, 0);
    ct_close(tree);
    EXPECT_EQ(drops, 1);
}

TEST(ExternalOwnership, AllocationOverflowReturnsAnErrorAcrossC)
{
    ct_write_handle *handle = nullptr;
    ct_write_ptrs    pointers{};
    EXPECT_NE(ct_alloc(nullptr, 1, std::numeric_limits<size_t>::max(), &handle, &pointers), 0);
    EXPECT_EQ(handle, nullptr);
}

TEST(ExternalOwnership, InvalidLaterOperationReleasesEveryTransferredOwner)
{
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    std::array<int, 3>       drops{};
    std::array<ct_ext_op, 3> operations{external(drops.data(), nullptr, 0), external(&drops[1], nullptr, 0),
                                        external(&drops[2], nullptr, 0)};
    operations[1].key = nullptr;
    EXPECT_NE(ct_apply_batch_external(tree, 1, operations.data(), operations.size()), 0);
    for (int count : drops) {
        EXPECT_EQ(count, 1);
    }
    ct_close(tree);
    for (int count : drops) {
        EXPECT_EQ(count, 1);
    }
}
