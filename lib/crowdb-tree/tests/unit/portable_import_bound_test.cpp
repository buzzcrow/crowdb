// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/portable_import.h"
#include "btree/portable_import_plan.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(PortableImportBound, PlannedFrameAndKeyWindowsCompleteWithExactlyTheirPrepaidCredit)
{
    Config config;
    config.frame_bytes      = 512;
    config.max_inline_value = 16;
    config.inner_max_keys   = 2;
    PageLoadLimits loads;
    loads.max_leaf_entries   = 3;
    loads.max_leaf_key_bytes = 12;
    PortableImportPlan plan(config, loads);
    for (size_t i = 0; i < 37; ++i) {
        ASSERT_TRUE(plan.append(3, i % 2 == 0 ? 1000 : 0, i % 2 != 0).ok());
    }
    const auto shape = plan.finish();
    if (!shape) {
        ADD_FAILURE() << "shape is missing";
        return;
    }
    const auto bound = detail::AllocationBound()
                           .add(native_import_allocation_bound(shape.value()))
                           .add(PortableImportBuilder::allocation_bound(shape.value()))
                           .value();
    if (!bound) {
        ADD_FAILURE() << "bound is missing";
        return;
    }
    detail::ResourceBudget::Limits limits{};
    limits[static_cast<size_t>(detail::BudgetClass::kReclaim)] = bound.value();
    detail::ResourceBudget budget(limits, 1);
    auto                   account = budget.make_account(limits);
    auto reserved = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim, bound.value());
    if (!reserved) {
        ADD_FAILURE() << "reserved is missing";
        return;
    }
    auto                                 credit = detail::AllocationReservation::create(std::move(reserved.value()));
    std::shared_ptr<ReadablePageCatalog> catalog;
    {
        PortableImportBuilder builder(config, loads, 40, credit);
        for (size_t i = 0; i < 37; ++i) {
            const auto key    = std::string(3 - std::to_string(i).size(), '0') + std::to_string(i);
            const auto cell   = encode_cell_buf(i + 1, i % 2 == 0 ? OpKind::kPut : OpKind::kDelete,
                                                i % 2 == 0 ? std::string(1000, 'v') : std::string{});
            const auto status = builder.append(key, Slice(cell));
            ASSERT_TRUE(status.ok()) << status.to_string();
        }
        const auto status = builder.finish(&catalog);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    EXPECT_LE(catalog->frames.size(), shape.value().frames);
    EXPECT_EQ(shape.value().leaf_pages, 13U);
    catalog.reset();
    credit->seal();
    credit.reset();
    EXPECT_EQ(budget.snapshot().allocated[static_cast<size_t>(detail::BudgetClass::kReclaim)], 0U);
}

TEST(PortableImportBound, OversizedInputsAndArithmeticOverflowCannotBecomeSmallReservations)
{
    Config config;
    config.frame_bytes = 4096;
    PortableImportPlan plan(config, {});
    EXPECT_EQ(plan.append(SIZE_MAX, 1, false).code(), Code::kResourceExhausted);
    EXPECT_EQ(plan.append(1, SIZE_MAX, false).code(), Code::kResourceExhausted);
    NativeImportShape shape;
    shape.frames          = 1;
    shape.primary_pages   = 1;
    shape.max_frame_bytes = SIZE_MAX;
    EXPECT_FALSE(native_import_allocation_bound(shape));
}
} // namespace
} // namespace crowdb::tree
