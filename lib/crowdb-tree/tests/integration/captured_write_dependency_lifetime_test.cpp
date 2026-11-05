// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/overflow_backing.h"
#include "captured_write_test_support.h"

namespace crowdb::tree
{
namespace
{
using namespace test_write;

TEST(CapturedWriteDependencies, ReusingOneValueDoesNotKeepTheDeletedValueFromTheOldLeafAlive)
{
    TestCapturedWrite test;
    const std::string kept_value(3000, 'k');
    const std::string deleted_value(4000, 'd');
    ASSERT_TRUE(test.prepare(captured_leaf(page({
                                 {.key = "a", .value = kept_value   },
                                 {.key = "b", .value = deleted_value}
    })))
                    .ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U) << first.status.to_string();
    auto old = test.operation->result().pages;
    ASSERT_NE(old, nullptr);
    const auto          raw = test.read(old->frames.at(0));
    const LeafFrameView original(raw.data(), raw.size());
    ASSERT_EQ(original.count(), 2U);
    const CellView                         kept(original.cell(0));
    const CellView                         deleted(original.cell(1));
    std::vector<std::weak_ptr<const void>> deleted_owners;
    for (auto pid = deleted.overflow_head(); pid != kInvalidPageId;) {
        const auto &frame = old->frames.at(pid);
        deleted_owners.emplace_back(frame.backing->physical_owner);
        pid = frame.next_overflow;
    }
    auto leaf = std::make_shared<PageState>();
    leaf->bases.push_back(
        {.identity = 1,
         .max_slot = 10,
         .resident = std::shared_ptr<const LeafBase>(LeafBase::from_frame_copy(raw.data(), raw.size())),
         .backing  = old->frames.at(0).backing});
    const auto delta = page({
        {.key = "b", .slot = 10, .value = "", .tombstone = true}
    });
    leaf->overlays.push_back(delta->overlays.front());
    auto next          = captured_leaf(leaf);
    next->next_page_id = old->next_page_id;
    test.operation.reset();
    ASSERT_TRUE(test.prepare(next, old).ok());
    Completion second;
    test.run(&second);
    ASSERT_EQ(second.values.load(), 1U) << second.status.to_string();
    auto result = test.operation->result();
    ASSERT_NE(result.pages, nullptr);
    EXPECT_EQ(result.written_frames, 1U);
    const auto          updated = test.read(result.pages->frames.at(0));
    const LeafFrameView current(updated.data(), updated.size());
    ASSERT_EQ(current.count(), 2U);
    EXPECT_TRUE(CellView(current.cell(1)).is_tombstone());
    EXPECT_LT(result.pages->frames.size(), old->frames.size());
    test.operation.reset();
    next.reset();
    leaf.reset();
    old.reset();
    for (const auto &owner : deleted_owners) {
        EXPECT_TRUE(owner.expired()) << "the retained chain still owns the deleted value's extent";
    }
    deleted_owners.clear();
    EXPECT_EQ(test.ledger->stats().protected_ranges, result.pages->frames.size());
    std::string restored;
    for (auto pid = kept.overflow_head(); pid != kInvalidPageId;) {
        const auto              bytes = test.read(result.pages->frames.at(pid));
        const OverflowFrameView chunk(bytes.data(), bytes.size());
        restored += chunk.payload().to_string();
        pid = chunk.next_page_id();
    }
    EXPECT_EQ(restored, kept_value);
    result = {};
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

std::shared_ptr<const PublishedTreeState> shared_chain_input(const CapturedDurablePages &old, Slice encoded)
{
    std::vector<leaf_entry> entries;
    entries.push_back({.key = "a", .cell = buffer::copy_of(encoded)});
    entries.push_back({.key = "c", .cell = buffer::copy_of(encoded)});
    auto       shared    = std::shared_ptr<const LeafBase>(LeafBase::build(entries));
    const auto right_pid = old.next_page_id;
    const auto root_pid  = right_pid + 1;
    auto       left      = std::make_shared<PageState>(*page({
        {.key = "b", .slot = 10, .value = std::string(4000, 'b')}
    }));
    left->range.upper    = "c";
    left->right_sibling  = right_pid;
    left->bases.push_back(
        {.identity = 1, .range = left->range, .max_slot = 10, .resident = shared, .backing = old.frames.at(0).backing});
    auto right         = std::make_shared<PageState>();
    right->range.lower = "c";
    right->bases.push_back({.identity = 1,
                            .range    = right->range,
                            .max_slot = 10,
                            .resident = shared,
                            .backing  = old.frames.at(0).backing});
    auto inner      = std::make_shared<InnerPageState>();
    inner->children = {0, right_pid};
    inner->separators.emplace_back("c");
    auto root   = std::make_shared<PublishedPage>();
    root->level = 1;
    root->inner = inner;
    const std::array<PageReplacement, 3> changes{
        {{.page_id = 0, .replacement = published(left)},
         {.page_id = right_pid, .replacement = published(right)},
         {.page_id = root_pid, .replacement = root}}
    };
    auto                   captured = std::make_shared<PublishedTreeState>();
    MappingView::CopyStats copied;
    captured->mapping      = captured->mapping.replace(changes, copied);
    captured->root_page_id = root_pid;
    captured->next_page_id = root_pid + 1;
    captured->coverage     = 10;
    return captured;
}

TEST(CapturedWriteDependencies, SharedChainsDoNotJoinTheDependencyOwnersOfDifferentOutputLeaves)
{
    TestCapturedWrite test;
    const std::string value(3000, 's');
    ASSERT_TRUE(test.prepare(captured_leaf(page({
                                 {.key = "a", .value = value}
    })))
                    .ok());
    Completion first;
    test.run(&first);
    ASSERT_EQ(first.values.load(), 1U) << first.status.to_string();
    auto old = test.operation->result().pages;
    ASSERT_NE(old, nullptr);
    const auto raw       = test.read(old->frames.at(0));
    const auto encoded   = LeafFrameView(raw.data(), raw.size()).cell(0);
    const auto right_pid = old->next_page_id;
    auto       captured  = shared_chain_input(*old, encoded);
    test.operation.reset();
    ASSERT_TRUE(test.prepare(captured, old).ok());
    Completion second;
    test.run(&second);
    ASSERT_EQ(second.values.load(), 1U) << second.status.to_string();
    auto result = test.operation->result();
    ASSERT_NE(result.pages, nullptr);
    auto retained = result.pages->frames.at(right_pid).backing;
    ASSERT_NE(retained->overflow, nullptr);
    const auto protected_count = retained->overflow->pages.size() + 1;
    test.operation.reset();
    captured.reset();
    old.reset();
    result = {};
    EXPECT_EQ(test.ledger->stats().protected_ranges, protected_count);
    const CellView cell{Slice(encoded)};
    std::string    restored;
    for (auto pid = cell.overflow_head(); pid != kInvalidPageId;) {
        const auto             &frame = retained->overflow->pages.at(pid);
        const auto              bytes = test.read({.page_id        = pid,
                                                   .type           = page_type::kOverflowFrame,
                                                   .frame_bytes    = frame.frame_bytes,
                                                   .logical_bytes  = frame.logical_bytes,
                                                   .next_overflow  = frame.next,
                                                   .overflow_bytes = frame.bytes,
                                                   .backing        = frame.backing});
        const OverflowFrameView chunk(bytes.data(), bytes.size());
        restored += chunk.payload().to_string();
        pid = chunk.next_page_id();
    }
    EXPECT_EQ(restored, value);
    retained.reset();
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    test.credits.seal();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace
} // namespace crowdb::tree
