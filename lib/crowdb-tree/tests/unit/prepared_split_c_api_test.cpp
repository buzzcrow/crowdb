// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/split_overlay_c_api.h"
#include "crowdb-tree/status.h"

#include <gtest/gtest.h>

#include <array>
#include <utility>

namespace crowdb::tree
{
namespace
{
class PreparedSplitCAbi : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
        ct_options options{};
        ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &source), 0);
        ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &destination), 0);
    }

    void TearDown() override
    {
        ct_prepared_split_overlay_destroy(token);
        ct_close(source);
        ct_close(destination);
        ct_runtime_free(runtime);
    }

    static ct_installed_split_overlay_v1 output()
    {
        return {.version = 1, .struct_size = sizeof(ct_installed_split_overlay_v1), .generation = 71, .frontier = 79};
    }

    static constexpr ct_status invalid     = static_cast<ct_status>(Code::kInvalidArgument);
    ct_runtime                *runtime     = nullptr;
    ct_tree                   *source      = nullptr;
    ct_tree                   *destination = nullptr;
    ct_prepared_split_overlay *token       = nullptr;
    ct_split_overlay_limits_v1 limits{
        .version = 1, .struct_size = sizeof(ct_split_overlay_limits_v1), .max_records = 8, .max_bytes = 4096};
};

TEST_F(PreparedSplitCAbi, ExactLimitsAndNullArgumentsLeaveOutputsUnchanged)
{
    uint64_t start = 97;
    EXPECT_EQ(ct_arm_split_overlay(nullptr, destination, &limits, &token, &start), invalid);
    EXPECT_EQ(ct_arm_split_overlay(source, nullptr, &limits, &token, &start), invalid);
    EXPECT_EQ(ct_arm_split_overlay(source, destination, nullptr, &token, &start), invalid);
    EXPECT_EQ(ct_arm_split_overlay(source, destination, &limits, nullptr, &start), invalid);
    EXPECT_EQ(ct_arm_split_overlay(source, destination, &limits, &token, nullptr), invalid);
    for (const auto version : std::array<uint32_t, 2>{0, 2}) {
        auto bad    = limits;
        bad.version = version;
        EXPECT_EQ(ct_arm_split_overlay(source, destination, &bad, &token, &start), invalid);
        EXPECT_EQ(token, nullptr);
        EXPECT_EQ(start, 97U);
    }
    for (const auto size : std::array<uint32_t, 2>{sizeof(limits) - 1, sizeof(limits) + 1}) {
        auto bad        = limits;
        bad.struct_size = size;
        EXPECT_EQ(ct_arm_split_overlay(source, destination, &bad, &token, &start), invalid);
        EXPECT_EQ(token, nullptr);
        EXPECT_EQ(start, 97U);
    }
    EXPECT_EQ(ct_prepared_split_overlay_prepare(nullptr, 0), invalid);
    EXPECT_EQ(ct_prepared_split_overlay_committed_frontier(nullptr, &start), 0);
    EXPECT_EQ(start, 97U);
    ASSERT_EQ(ct_arm_split_overlay(source, destination, &limits, &token, &start), 0);
    ASSERT_NE(token, nullptr);
    auto *same = token;
    start      = 97;
    EXPECT_EQ(ct_arm_split_overlay(source, destination, &limits, &token, &start), invalid);
    EXPECT_EQ(token, same);
    EXPECT_EQ(start, 97U);
}

TEST_F(PreparedSplitCAbi, ExactInstallOutputAndFixedFrontierArePreserved)
{
    auto out = output();
    EXPECT_EQ(ct_prepared_split_overlay_try_install(nullptr, 0, &out), invalid);
    EXPECT_EQ(out.generation, 71U);
    EXPECT_EQ(out.frontier, 79U);
    uint64_t start = 97;
    ASSERT_EQ(ct_arm_split_overlay(source, destination, &limits, &token, &start), 0);
    EXPECT_EQ(start, 0U);
    ASSERT_EQ(ct_prepared_split_overlay_prepare(token, 0), 0);
    const uint8_t key   = 'k';
    const uint8_t value = 'v';
    ASSERT_EQ(ct_apply_put(source, 1, &key, 1, &value, 1), 0);
    EXPECT_EQ(ct_prepared_split_overlay_try_install(token, 1, nullptr), invalid);
    for (const auto version : std::array<uint32_t, 2>{0, 2}) {
        out         = output();
        out.version = version;
        EXPECT_EQ(ct_prepared_split_overlay_try_install(token, 1, &out), invalid);
        EXPECT_EQ(out.version, version);
        EXPECT_EQ(out.generation, 71U);
        EXPECT_EQ(out.frontier, 79U);
    }
    for (const auto size : std::array<uint32_t, 2>{sizeof(out) - 1, sizeof(out) + 1}) {
        out             = output();
        out.struct_size = size;
        EXPECT_EQ(ct_prepared_split_overlay_try_install(token, 1, &out), invalid);
        EXPECT_EQ(out.struct_size, size);
        EXPECT_EQ(out.generation, 71U);
        EXPECT_EQ(out.frontier, 79U);
    }
    EXPECT_EQ(ct_prepared_split_overlay_committed_frontier(token, &start), 0);
    out = output();
    ASSERT_EQ(ct_prepared_split_overlay_try_install(token, 1, &out), 0);
    EXPECT_EQ(out.frontier, 1U);
    EXPECT_GT(out.generation, 0U);
    const auto generation = out.generation;
    ASSERT_EQ(ct_prepared_split_overlay_try_install(token, 1, &out), 0);
    EXPECT_EQ(out.generation, generation);
    EXPECT_EQ(ct_prepared_split_overlay_committed_frontier(token, &start), 1);
    EXPECT_EQ(start, 1U);
    EXPECT_EQ(ct_prepared_split_overlay_committed_frontier(token, nullptr), 0);
    out = output();
    EXPECT_EQ(ct_prepared_split_overlay_try_install(token, 2, &out), static_cast<ct_status>(Code::kUnavailable));
    EXPECT_EQ(out.generation, 71U);
    EXPECT_EQ(out.frontier, 79U);
    int32_t  found = 0;
    uint64_t slot  = 0;
    ct_buf   bytes{};
    ASSERT_EQ(ct_get(destination, &key, 1, &found, &slot, &bytes), 0);
    EXPECT_EQ(found, 1);
    EXPECT_EQ(slot, 1U);
    ASSERT_EQ(bytes.len, 1U);
    EXPECT_EQ(bytes.data[0], value);
    ct_free_buf(&bytes);
}

TEST_F(PreparedSplitCAbi, PreparedHandleSafelyOutlivesBothTreeFacades)
{
    uint64_t start = 97;
    ASSERT_EQ(ct_arm_split_overlay(source, destination, &limits, &token, &start), 0);
    ASSERT_EQ(ct_prepared_split_overlay_prepare(token, 0), 0);
    ct_close(std::exchange(source, nullptr));
    ct_close(std::exchange(destination, nullptr));
    auto out = output();
    EXPECT_EQ(ct_prepared_split_overlay_try_install(token, 0, &out), static_cast<ct_status>(Code::kUnavailable));
    EXPECT_EQ(out.generation, 71U);
    EXPECT_EQ(out.frontier, 79U);
    EXPECT_EQ(ct_prepared_split_overlay_committed_frontier(token, &start), 0);
    ct_prepared_split_overlay_destroy(std::exchange(token, nullptr));
}
} // namespace
} // namespace crowdb::tree
