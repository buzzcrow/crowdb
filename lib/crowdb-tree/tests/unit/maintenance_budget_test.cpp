// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/maintenance_publication.h"
#include "btree/page_encode.h"

#include <gtest/gtest.h>

#include <array>

namespace crowdb::tree
{
namespace
{
struct ReclaimBudget
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result{};
        result[static_cast<size_t>(detail::BudgetClass::kReclaim)] = 1024 * 1024;
        return result;
    }

    detail::ResourceBudget                           budget{limits(), 1};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());
    std::shared_ptr<detail::AllocationReservation>   reservation;

    ReclaimBudget()
    {
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim, 1024 * 1024);
        if (credit) {
            reservation = detail::AllocationReservation::create(std::move(*credit));
        }
        EXPECT_TRUE(reservation);
    }

    [[nodiscard]] size_t allocated() const
    {
        return budget.snapshot().allocated[static_cast<size_t>(detail::BudgetClass::kReclaim)];
    }
};

std::string key(size_t index)
{
    std::string result(64, static_cast<char>('a' + index));
    return result;
}

std::shared_ptr<PageState> input_page()
{
    auto           page = std::make_shared<PageState>();
    OverlayBuilder builder({});
    for (size_t i = 0; i < 4; ++i) {
        auto cell = std::make_shared<CellVersion>(encode_cell_buf(i + 1, OpKind::kPut, "value"), i + 1, 0);
        EXPECT_TRUE(builder.append(key(i), cell).ok());
    }
    std::shared_ptr<const OverlayBatch> run;
    EXPECT_TRUE(builder.finish(1, &run).ok());
    page->overlays.push_back(std::move(run));
    return page;
}

TEST(MaintenanceBudget, EveryCoalescedRunKeepsItsCreditAndSharedPayloadAfterWindowMoves)
{
    ReclaimBudget         budget;
    auto                  input = input_page();
    const auto            value = input->lookup(key(3)).cell;
    PageMaintenanceLimits limits;
    limits.metadata        = budget.reservation;
    limits.run.max_entries = 1;
    const auto baseline    = budget.allocated();
    auto       result      = coalesce_overlays(input, limits, 2, 10);
    ASSERT_TRUE(result.status.ok()) << result.status.to_string();
    ASSERT_EQ(result.page->overlays.size(), 4U);
    EXPECT_EQ(result.page->overlays.get_allocator().owner(), budget.reservation);
    EXPECT_GE(budget.allocated() - baseline, 4 * (sizeof(OverlayBatch) + sizeof(EntryRef)));
    input.reset();
    budget.reservation->seal();
    EXPECT_EQ(result.page->lookup(key(3)).cell.owner, value.owner);
    EXPECT_EQ(result.page->lookup(key(3)).cell.value.to_string(), "value");
    const auto charged = budget.allocated();
    result.page.reset();
    EXPECT_LT(budget.allocated(), charged);
    EXPECT_EQ(budget.allocated(), baseline);
}

TEST(MaintenanceBudget, LayoutSeparatorsAndSplitDescriptorsRetainActualCapacity)
{
    ReclaimBudget         budget;
    PageMaintenanceLimits limits;
    limits.metadata           = budget.reservation;
    limits.primary_page_bytes = 320;
    limits.target_page_bytes  = 260;
    const auto baseline       = budget.allocated();
    auto       input          = input_page();
    auto       layout         = plan_leaf_layout(input, limits);
    ASSERT_TRUE(layout.status.ok()) << layout.status.to_string();
    ASSERT_EQ(layout.primary_bytes.size(), 4U);
    EXPECT_EQ(layout.separators.get_allocator().owner(), budget.reservation);
    EXPECT_EQ(layout.primary_bytes.get_allocator().owner(), budget.reservation);
    std::array<SplitTarget, 4> targets{};
    for (size_t i = 0; i < targets.size(); ++i) {
        targets[i] = {
            .page_id = i, .version = {.route_generation = 1, .content_id = 1, .representation_id = 1}
        };
    }
    auto split = split_leaf_ranges(layout, targets, limits);
    ASSERT_TRUE(split.status.ok()) << split.status.to_string();
    ASSERT_EQ(split.pages.size(), 4U);
    for (const auto &page : split.pages) {
        EXPECT_EQ(page->overlays.get_allocator().owner(), budget.reservation);
        EXPECT_EQ(page->bases.get_allocator().owner(), budget.reservation);
    }
    {
        auto dropped = std::move(layout);
    }
    input.reset();
    budget.reservation->seal();
    EXPECT_GT(budget.allocated(), baseline);
    EXPECT_EQ(split.pages[2]->range.lower, std::optional<OwnedKey>(key(2)));
    EXPECT_EQ(split.pages[2]->lookup(key(2)).cell.value.to_string(), "value");
    // Destroy the result containers too, so their retained capacity is freed.
    {
        auto dropped = std::move(split);
    }
    EXPECT_EQ(budget.allocated(), baseline);
}

TEST(MaintenanceBudget, CursorScratchUsesCurrentCreditAndSealedCreditCannotAllocate)
{
    ReclaimBudget budget;
    auto          page     = input_page();
    const auto    baseline = budget.allocated();
    {
        LogicalPageCursor cursor(page, {}, UINT64_MAX, 8, false, budget.reservation);
        ASSERT_TRUE(cursor.valid());
        EXPECT_GT(budget.allocated(), baseline);
    }
    EXPECT_EQ(budget.allocated(), baseline);
    budget.reservation->seal();
    EXPECT_THROW((LogicalPageCursor(page, {}, UINT64_MAX, 8, false, budget.reservation)), std::bad_alloc);
}

TEST(MaintenanceBudget, StructureTransactionAndPublishedPagesRetainCreditUntilLastView)
{
    ReclaimBudget                             budget;
    const auto                                baseline = budget.allocated();
    std::shared_ptr<const PublishedTreeState> retained;
    {
        PublicationCoordinator publisher;
        auto                   page = std::make_shared<PublishedPage>();
        page->leaf                  = input_page();
        auto seed                   = std::make_shared<PublicationRequest>();
        seed->transaction           = {.record       = std::make_shared<InstallRecord>(InstallToken{}),
                                       .pages        = {{.page_id = 0, .expected = {}, .replacement = page}},
                                       .root_page_id = 0};
        seed->on_done               = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
        ASSERT_TRUE(publisher.try_submit(seed));
        MaintenancePublicationOptions options;
        options.metadata                 = budget.reservation;
        options.pages.primary_page_bytes = 320;
        options.pages.target_page_bytes  = 260;
        auto record                      = std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 2});
        auto result                      = build_split_step_publication(publisher.capture(), 0, record, options);
        ASSERT_TRUE(result.status.ok()) << result.status.to_string();
        ASSERT_TRUE(result.changed());
        EXPECT_EQ(result.transaction.pages.get_allocator().owner(), budget.reservation);
        EXPECT_EQ(result.output_leaf_pids.get_allocator().owner(), budget.reservation);
        for (const auto &replacement : result.transaction.pages) {
            if (replacement.replacement->inner) {
                EXPECT_EQ(replacement.replacement->inner->children.get_allocator().owner(), budget.reservation);
                EXPECT_EQ(replacement.replacement->inner->separators.get_allocator().owner(), budget.reservation);
            }
        }
        auto request         = detail::reserved_shared<PublicationRequest>(budget.reservation, budget.reservation);
        request->transaction = std::move(result.transaction);
        request->on_done     = [](PublicationResult published) { EXPECT_TRUE(published.status.ok()); };
        ASSERT_TRUE(publisher.try_submit(request));
        retained = publisher.capture();
        budget.reservation->seal();
        auto denied = build_split_step_publication(retained, 1, record, options);
        EXPECT_EQ(denied.status.code(), Code::kResourceExhausted);
        EXPECT_FALSE(denied.changed());
        EXPECT_EQ(publisher.capture(), retained);
    }
    EXPECT_GT(budget.allocated(), baseline);
    EXPECT_EQ(retained->mapping.get(0)->leaf->lookup(key(0)).cell.value.to_string(), "value");
    retained.reset();
    EXPECT_EQ(budget.allocated(), baseline);
}

TEST(MaintenanceBudget, EncoderRunsAfterSealAndFrameDependencyKeepsItsCharge)
{
    ReclaimBudget budget;
    const auto    baseline = budget.allocated();
    auto          view     = std::make_shared<PublishedTreeState>();
    auto          page     = std::make_shared<PublishedPage>();
    page->leaf             = input_page();
    const std::array<PageReplacement, 1> changes{{{.page_id = 0, .expected = {}, .replacement = page}}};
    MappingView::CopyStats               copied;
    view->mapping      = view->mapping.replace(changes, copied);
    view->root_page_id = 0;
    view->next_page_id = 1;
    view->coverage     = 4;
    auto prepared      = prepare_final_page(view, 0, {}, {.frame_bytes = 1024, .metadata = budget.reservation});
    ASSERT_TRUE(prepared.status.ok()) << prepared.status.to_string();
    ASSERT_TRUE(prepared.encoder);
    EXPECT_EQ(prepared.encoder.get_deleter().owner, budget.reservation);
    budget.reservation->seal();
    std::array<uint8_t, 1024> frame{};
    auto                      step = prepared.encoder->next(frame);
    ASSERT_TRUE(step.status.ok()) << step.status.to_string();
    ASSERT_TRUE(step.frame);
    EXPECT_TRUE(step.complete);
    EXPECT_TRUE(frame_validate(frame.data(), frame.size()));
    const auto before = budget.allocated();
    prepared.encoder.reset();
    EXPECT_EQ(before - budget.allocated(), sizeof(FinalPageEncoder));
    EXPECT_GT(budget.allocated(), baseline);
    step.frame.reset();
    EXPECT_EQ(budget.allocated(), baseline);
}
} // namespace
} // namespace crowdb::tree
