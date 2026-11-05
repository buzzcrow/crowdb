// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/logical_page.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

using namespace crowdb::tree;

namespace
{
std::shared_ptr<const CellVersion> make_cell(uint64_t slot, Slice value, OpKind kind = OpKind::kPut)
{
    return std::make_shared<CellVersion>(encode_cell_buf(slot, kind, value), slot,
                                         kind == OpKind::kDelete ? kFlagTombstone : 0);
}

void put(MemTable &table, Slice key, uint64_t slot, Slice value, OpKind kind = OpKind::kPut)
{
    ASSERT_TRUE(table.upsert(key, slot, encode_cell_buf(slot, kind, value), 0));
}

std::shared_ptr<const OverlayBatch>
make_run(uint64_t id, const std::vector<std::pair<std::string, std::shared_ptr<const CellVersion>>> &entries)
{
    OverlayBuilder builder({});
    for (const auto &[key, cell] : entries) {
        EXPECT_TRUE(builder.append(Slice(key), cell).ok());
    }
    std::shared_ptr<const OverlayBatch> run;
    EXPECT_TRUE(builder.finish(id, &run).ok());
    return run;
}

std::shared_ptr<const LeafBase> make_base()
{
    std::vector<leaf_entry> entries;
    for (const auto *key : {"a", "b", "c", "d"}) {
        entries.push_back({.key = key, .cell = encode_cell_buf(3, OpKind::kPut, Slice("base"))});
    }
    return std::shared_ptr<const LeafBase>(LeafBase::build(entries));
}

std::shared_ptr<const BackingRef> make_backing()
{
    auto backing    = std::make_shared<BackingRef>();
    backing->length = 4096;
    backing->owner  = std::make_shared<int>(1);
    return backing;
}
} // namespace

TEST(LogicalPage, CellOwnerOutlivesSourceAndRetainsOnlyOneVersion)
{
    EpochManager epoch;
    auto         source = std::make_shared<MemTable>(1, &epoch);
    put(*source, "key", 5, "prefix");
    put(*source, "key", 9, "successor");
    EntryRef entry;
    {
        MemTableRangeMerge merge({source}, {}, 5, 0, {}, 1);
        ASSERT_TRUE(merge.status().ok());
        ASSERT_TRUE(merge.next(&entry).ok());
        EXPECT_EQ(entry.slot(), 5U);
        auto guard  = source->read_guard();
        auto cursor = source->prefix_cursor(5);
        EXPECT_EQ(entry.owner.get(), cursor.cell_version());
    }
    std::weak_ptr<MemTable> table = source;
    source.reset();
    epoch.try_reclaim();
    EXPECT_TRUE(table.expired());
    EXPECT_EQ(epoch.memtable_allocation()->load(), sizeof(CellVersion) + entry.owner->cell.size());
    EXPECT_EQ(entry.key().to_string(), "key");
    EXPECT_EQ(entry.value().to_string(), "prefix");
    entry = {};
    EXPECT_EQ(epoch.memtable_allocation()->load(), 0U);
}

TEST(LogicalPage, SelectedOwnerIsStableAcrossPruneAndOverwrite)
{
    MemTable source;
    put(source, "key", 5, "prefix");
    put(source, "key", 9, "newer");
    std::shared_ptr<const CellVersion> owner;
    {
        auto        guard    = source.read_guard();
        auto        cursor   = source.prefix_cursor(5);
        const auto *selected = cursor.cell_version();
        source.prune("key", 9);
        put(source, "key", 12, "latest");
        owner = cursor.cell_owner();
        ASSERT_EQ(owner.get(), selected);
    }
    EXPECT_EQ(owner->slot, 5U);
    EXPECT_EQ(CellView(owner->cell.slice()).value().to_string(), "prefix");
}

TEST(LogicalPage, ExternalValueHandoffDoesNotCopyAndDropsOnceAcrossThreads)
{
    struct Payload
    {
        std::string       value;
        std::atomic<int> *drops;
    };

    std::atomic<int> drops{0};
    auto            *payload = new Payload{.value = std::string(64 * 1024, 'v'), .drops = &drops};
    const char      *data    = payload->value.data();
    EntryRef         entry;
    {
        auto source = std::make_shared<MemTable>();
        auto value  = buffer::wrap_external(reinterpret_cast<const uint8_t *>(data), payload->value.size(), payload,
                                            [](void *context) {
                                               auto *owned = static_cast<Payload *>(context);
                                               owned->drops->fetch_add(1);
                                               delete owned;
                                            });
        ASSERT_TRUE(source->upsert_external("key", 7, 0, std::move(value), 0));
        MemTableRangeMerge merge({source}, {}, 7, 0, {}, 1);
        ASSERT_TRUE(merge.next(&entry).ok());
        EXPECT_EQ(entry.value().data(), data);
    }
    EXPECT_EQ(drops.load(), 0);
    std::thread consumer([owned = std::move(entry), data] {
        EXPECT_EQ(owned.value().data(), data);
        EXPECT_EQ(owned.value().size(), 64U * 1024);
    });
    consumer.join();
    EXPECT_EQ(drops.load(), 1);
}

TEST(LogicalPage, RangeMergeCombinesAllTablesBySlotAndStableSourceOrder)
{
    auto first  = std::make_shared<MemTable>();
    auto second = std::make_shared<MemTable>();
    put(*first, "a", 2, "covered");
    put(*first, "b", 5, "prefix");
    put(*first, "b", 11, "after-frontier");
    put(*first, "c", 7, "stable-first");
    put(*first, "e", 8, "outside");
    put(*second, "b", 6, "newer-prefix");
    put(*second, "c", 7, "stable-second");
    put(*second, "d", 8, {}, OpKind::kDelete);
    MemTableRangeMerge    merge({first, second}, {.lower = "b", .upper = "e"}, 8, 2, {}, 2);
    std::vector<EntryRef> entries;
    while (merge.valid()) {
        EntryRef entry;
        ASSERT_TRUE(merge.next(&entry).ok());
        entries.push_back(std::move(entry));
    }
    ASSERT_EQ(entries.size(), 3U);
    EXPECT_EQ(entries[0].key().to_string(), "b");
    EXPECT_EQ(entries[0].value().to_string(), "newer-prefix");
    EXPECT_EQ(entries[1].key().to_string(), "c");
    EXPECT_EQ(entries[1].value().to_string(), "stable-first");
    EXPECT_EQ(entries[2].key().to_string(), "d");
    EXPECT_EQ(entries[2].flags(), kFlagTombstone);
}

TEST(LogicalPage, IndependentRangeCursorsDoNotLoseSeparatorOrEmptyKey)
{
    auto source = std::make_shared<MemTable>();
    for (const auto *key : {"", "a", "b", "c"}) {
        put(*source, key, 1, key);
    }
    MemTableRangeMerge       left({source}, {.upper = "b"}, 1, 0, {}, 1);
    MemTableRangeMerge       right({source}, {.lower = "b"}, 1, 0, {}, 1);
    std::vector<std::string> keys;
    for (auto *merge : {&left, &right}) {
        while (merge->valid()) {
            EntryRef entry;
            ASSERT_TRUE(merge->next(&entry).ok());
            keys.push_back(entry.key().to_string());
        }
    }
    EXPECT_EQ(keys, (std::vector<std::string>{"", "a", "b", "c"}));
    MemTableRangeMerge empty({source}, {.lower = "b", .upper = "b"}, 1, 0, {}, 1);
    EXPECT_FALSE(empty.valid());
    MemTableRangeMerge no_sources({}, {}, 1, 0, {}, 0);
    EXPECT_TRUE(no_sources.status().ok());
    EXPECT_FALSE(no_sources.valid());
}

TEST(LogicalPage, MergeAndArenaBudgetsFailWithoutConsumingCandidate)
{
    auto source = std::make_shared<MemTable>();
    put(*source, "large-key", 1, "value");
    MemTableRangeMerge rejected({source}, {}, 1, 0, {}, 0);
    EXPECT_EQ(rejected.status().code(), Code::kResourceExhausted);
    MemTableRangeMerge merge({source}, {}, 1, 0, {.max_key_bytes = 3}, 1);
    EntryRef           entry;
    EXPECT_EQ(merge.next(&entry).code(), Code::kResourceExhausted);
    EXPECT_TRUE(merge.valid());
    EXPECT_EQ(entry.owner, nullptr);
}

TEST(LogicalPage, BoundedArenaSupportsLongKeysAndNeverRelocatesViews)
{
    RunLimits       limits{.max_entries = 4, .max_key_bytes = 1024, .key_block_bytes = 16};
    KeyArenaBuilder arena(limits);
    EntryRef        first;
    EntryRef        second;
    ASSERT_TRUE(arena.append("a", make_cell(1, "one"), &first).ok());
    const char       *key_data = first.key().data();
    const std::string long_key(600, 'z');
    ASSERT_TRUE(arena.append(Slice(long_key), make_cell(2, "two"), &second).ok());
    EXPECT_EQ(first.key().data(), key_data);
    EXPECT_EQ(first.key().to_string(), "a");
    EXPECT_EQ(second.key().to_string(), long_key);
    EXPECT_NE(first.key_arena, second.key_arena);
}

TEST(LogicalPage, MultipleArenaWindowsContinueWithoutRewindingSources)
{
    auto source = std::make_shared<MemTable>();
    for (const auto *key : {"a", "b", "c", "d", "e"}) {
        put(*source, key, 1, key);
    }
    MemTableRangeMerge    merge({source}, {}, 1, 0, {.max_entries = 2}, 1);
    std::vector<EntryRef> entries;
    size_t                windows = 1;
    while (merge.valid()) {
        EntryRef entry;
        auto     status = merge.next(&entry);
        if (status.code() == Code::kResourceExhausted) {
            merge.rotate_arena();
            ++windows;
            ASSERT_TRUE(merge.next(&entry).ok());
        }
        else {
            ASSERT_TRUE(status.ok());
        }
        entries.push_back(std::move(entry));
    }
    EXPECT_EQ(windows, 3U);
    ASSERT_EQ(entries.size(), 5U);
    for (size_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(entries[i].key().to_string(), std::string(1, static_cast<char>('a' + i)));
    }
    EXPECT_EQ(entries[0].key_arena, entries[1].key_arena);
    EXPECT_NE(entries[1].key_arena, entries[2].key_arena);
}

TEST(LogicalPage, OverlayValidatesOrderAndSharesValues)
{
    auto           value = make_cell(7, std::string(1024, 'v'));
    OverlayBuilder builder({});
    ASSERT_TRUE(builder.append("b", value).ok());
    EXPECT_EQ(builder.append("a", value).code(), Code::kInvalidArgument);
    EXPECT_EQ(builder.append("b", value).code(), Code::kInvalidArgument);
    std::shared_ptr<const OverlayBatch> run;
    ASSERT_TRUE(builder.finish(42, &run).ok());
    ASSERT_NE(run->find("b"), nullptr);
    EXPECT_EQ(run->find("b")->owner, value);
    EXPECT_EQ(run->find("b")->value().data(), CellView(value->cell.slice()).value().data());
    EXPECT_EQ(run->find("c"), nullptr);
    EXPECT_EQ(run->id(), 42U);
    EXPECT_EQ(run->min_slot(), 7U);
    EXPECT_EQ(run->max_slot(), 7U);
}

TEST(LogicalPage, LookupPreservesTombstoneOverColdBaseAndRequestsUnknownData)
{
    PageState page;
    page.bases.push_back({.identity = 1, .max_slot = 3, .backing = make_backing()});
    page.overlays.push_back(make_run(1, {
                                            {"b", make_cell(7, {}, OpKind::kDelete)}
    }));
    auto result = page.lookup("b");
    EXPECT_EQ(result.state, LogicalLookupState::kTombstone);
    EXPECT_EQ(result.cell.slot, 7U);
    EXPECT_EQ(page.lookup("a").state, LogicalLookupState::kNeedsBase);
    EXPECT_EQ(page.lookup("b", 5).state, LogicalLookupState::kNeedsBase);
    page.bases[0].max_slot.reset();
    EXPECT_EQ(page.lookup("b").state, LogicalLookupState::kNeedsBase);
    page.bases[0].max_slot = 9;
    EXPECT_EQ(page.lookup("b").state, LogicalLookupState::kNeedsBase);
}

TEST(LogicalPage, SharedBaseRangeViewsFilterLookupWithoutCopy)
{
    auto      base = make_base();
    PageState left;
    left.range = {.upper = "c"};
    left.bases.push_back({.identity = 1, .range = {.upper = "c"}, .resident = base});
    PageState right;
    right.range = {.lower = "c"};
    right.bases.push_back({.identity = 1, .range = {.lower = "c"}, .resident = base});
    EXPECT_EQ(left.lookup("c").state, LogicalLookupState::kMissing);
    EXPECT_EQ(right.lookup("b").state, LogicalLookupState::kMissing);
    auto value = right.lookup("c");
    EXPECT_EQ(value.state, LogicalLookupState::kValue);
    EXPECT_EQ(value.cell.value.data(), CellView(base->cell(2)).value().data());
    EXPECT_EQ(value.cell.owner.get(), base.get());
}

TEST(LogicalPage, CursorMergesBasesAndRunsForwardReverseAndAtFrontier)
{
    auto page = std::make_shared<PageState>();
    page->bases.push_back({.identity = 1, .resident = make_base()});
    page->overlays.push_back(make_run(2, {
                                             {"b", make_cell(7, {}, OpKind::kDelete)},
                                             {"c", make_cell(9, "latest")}
    }));
    page->overlays.push_back(make_run(1, {
                                             {"c", make_cell(6, "prefix")}
    }));
    for (bool reverse : {false, true}) {
        LogicalPageCursor cursor(page, {.lower = "b", .upper = "d"}, 7, 3, reverse);
        ASSERT_TRUE(cursor.status().ok());
        std::vector<std::string> keys;
        while (cursor.valid()) {
            const auto key  = cursor.key().to_string();
            const auto cell = cursor.cell();
            keys.push_back(key);
            if (key == "b") {
                EXPECT_TRUE(cell.tombstone());
                EXPECT_EQ(cell.slot, 7U);
            }
            else {
                EXPECT_EQ(cell.value.to_string(), "prefix");
                EXPECT_EQ(cell.slot, 6U);
            }
            cursor.next();
        }
        EXPECT_EQ(keys, (reverse ? std::vector<std::string>{"c", "b"} : std::vector<std::string>{"b", "c"}));
    }
}

TEST(LogicalPage, CursorReportsColdBaseAndBudgetsWithoutStartingPartialScan)
{
    auto page = std::make_shared<PageState>();
    page->bases.push_back({.identity = 1, .range = {.upper = "c"}, .backing = make_backing()});
    page->overlays.push_back(make_run(1, {
                                             {"z", make_cell(7, "overlay")}
    }));
    LogicalPageCursor cold(page, {}, UINT64_MAX, 2);
    EXPECT_TRUE(cold.status().ok());
    EXPECT_EQ(cold.needs_base(), 0U);
    EXPECT_FALSE(cold.valid());
    LogicalPageCursor outside(page, {.lower = "c"}, UINT64_MAX, 2);
    EXPECT_FALSE(outside.needs_base().has_value());
    ASSERT_TRUE(outside.valid());
    EXPECT_EQ(outside.key().to_string(), "z");
    LogicalPageCursor windowed(page, {}, UINT64_MAX, 1);
    EXPECT_TRUE(windowed.status().ok());
    EXPECT_EQ(windowed.needs_base(), 0U);
    EXPECT_FALSE(windowed.valid());
}

TEST(LogicalPage, BackingNeedsIndependentOwnerAndCapabilities)
{
    BackingRef backing;
    backing.length = 4096;
    EXPECT_FALSE(backing.reloadable());
    backing.owner = std::make_shared<int>(1);
    EXPECT_TRUE(backing.reloadable());
    EXPECT_FALSE(BackingCapabilities{}.can_stage());
    EXPECT_FALSE((BackingCapabilities{true, false}).can_stage());
    EXPECT_TRUE((BackingCapabilities{true, true}).can_stage());
}

TEST(LogicalPage, ColdBaseSlotTieRespectsSourceOrderBeforeSkippingLoad)
{
    PageState page;
    page.bases.push_back({.identity = 1, .max_slot = 3, .backing = make_backing()});
    page.bases.push_back({.identity = 2, .resident = make_base()});
    auto result = page.lookup("a");
    EXPECT_EQ(result.state, LogicalLookupState::kNeedsBase);
    EXPECT_EQ(result.base_index, 0U);
    // An overlay precedes every base, so an equal-slot overlay proves the
    // outcome even when the first base is cold.
    page.overlays.push_back(make_run(1, {
                                            {"a", make_cell(3, "overlay")}
    }));
    result = page.lookup("a");
    EXPECT_EQ(result.state, LogicalLookupState::kValue);
    EXPECT_EQ(result.cell.value.to_string(), "overlay");
    page.overlays.clear();
    std::swap(page.bases[0], page.bases[1]);
    result = page.lookup("a");
    EXPECT_EQ(result.state, LogicalLookupState::kValue);
    EXPECT_EQ(result.cell.value.to_string(), "base");
}

TEST(LogicalPage, InvalidColdBaseCannotMasqueradeAsLoadableOrMissing)
{
    auto page = std::make_shared<PageState>();
    page->bases.push_back({.identity = 1});
    auto result = page->lookup("a");
    EXPECT_EQ(result.state, LogicalLookupState::kInvalid);
    EXPECT_EQ(result.status.code(), Code::kCorruption);
    EXPECT_EQ(result.base_index, 0U);
    LogicalPageCursor cursor(page, {}, UINT64_MAX, 1);
    EXPECT_EQ(cursor.status().code(), Code::kCorruption);
    EXPECT_FALSE(cursor.needs_base().has_value());
    EXPECT_FALSE(cursor.valid());
}
