// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/overflow_backing.h"
#include "local_recovery_test_support.h"
#include "recovered_state_test_support.h"

namespace crowdb::tree
{
namespace
{
using namespace test_read;

std::shared_ptr<PublishedTreeState> boundary_tree()
{
    auto result          = std::make_shared<PublishedTreeState>();
    result->coverage     = 10;
    result->root_page_id = 0;
    result->next_page_id = 7;
    std::vector<PageReplacement> replacements;
    for (uint64_t pid = 0; pid < 3; ++pid) {
        auto index          = std::make_shared<PublishedPage>();
        index->level        = pid == 0 ? 2 : 1;
        index->inner        = inner({"m"}, {(2 * pid) + 1, (2 * pid) + 2});
        index->metrics.leaf = false;
        replacements.push_back({.page_id = pid, .expected = {}, .replacement = index});
    }
    for (uint64_t pid = 3; pid < 7; ++pid) {
        auto logical = std::make_shared<PageState>();
        if (pid != 3) {
            logical->range.lower = "m";
        }
        if (pid != 6) {
            logical->range.upper = "m";
        }
        logical->right_sibling = pid == 6 ? kInvalidPageId : pid + 1;
        auto resident          = base({});
        if (pid == 3) {
            resident = base({
                {.key = "a", .value = "left"}
            });
        }
        else if (pid == 6) {
            resident = base({
                {.key = "z", .value = "right"}
            });
        }
        logical->bases.push_back(
            {.identity = pid + 1, .range = logical->range, .max_slot = 10, .resident = std::move(resident)});
        replacements.push_back({.page_id = pid, .expected = {}, .replacement = published(logical)});
    }
    MappingView::CopyStats copied;
    result->mapping = result->mapping.replace(replacements, copied);
    return result;
}

void complete_with(const std::shared_ptr<detail::FlushRuntime> &runtime, detail::CallbackSender sender)
{
    Completion done;
    auto       connected = stdexec::connect(sender, Receiver{&done});
    stdexec::start(connected);
    runtime->drain();
    EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_EQ(done.errors.load(), 0U);
}
} // namespace

TEST(RecoveredStateBackend, CheckpointRecoveryPreservesBoundaryEmptyChildrenForReadsAndRecheckpoint)
{
    test_write::TestRecovery test;
    ASSERT_TRUE(test.original.source
                    .prepare(boundary_tree(), {},
                             {.frames = {.encode = {.frame_bytes = 4096}}, .write = {.max_frame_bytes = 4096}})
                    .ok());
    Completion written;
    test.original.source.run(&written);
    ASSERT_EQ(written.values.load(), 1U) << written.status.to_string();
    ASSERT_TRUE(test.original.prepare(test.original.source.operation->result().pages).ok());
    Completion committed;
    test.original.run(&committed);
    ASSERT_EQ(committed.values.load(), 1U) << committed.status.to_string();
    ASSERT_TRUE(test.prepare().ok());
    Completion recovered;
    test.run(&recovered);
    ASSERT_EQ(recovered.values.load(), 1U) << recovered.status.to_string();
    auto runtime = test.original.source.runtime;
    auto lane    = test.original.source.lane;
    auto loader  = std::make_shared<AsyncStorePageLoader>(test.store, runtime, lane, PageLoadLimits{},
                                                          OverflowBackingResolver{}, test.credits.credits);
    std::shared_ptr<RecoveredStateOperation> operation;
    ASSERT_TRUE(RecoveredStateOperation::create(test.operation->result().latest, loader, runtime, lane,
                                                {.max_step_entries = 1}, test.credits.credits.reservation(), &operation)
                    .ok());
    complete_with(runtime, operation->sender());
    const auto state = operation->result().state;
    ASSERT_NE(state, nullptr);
    EXPECT_TRUE(state->mapping.get(4)->leaf->range.empty());
    EXPECT_TRUE(state->mapping.get(5)->leaf->range.empty());
    EXPECT_EQ(state->stats.leaf_pages, 4U);
    for (const auto *key : {"a", "z", "m"}) {
        std::shared_ptr<PublishedGet> get;
        ASSERT_TRUE(PublishedGet::create(state, loader, key, {}, &get).ok());
        complete_with(runtime, get->sender());
        EXPECT_EQ(get->result().found, std::string_view(key) != "m");
    }
    for (bool reverse : {false, true}) {
        std::shared_ptr<PublishedScan> scan;
        ASSERT_TRUE(PublishedScan::create(state, loader, {}, reverse, {.max_entries = 1}, &scan).ok());
        std::vector<std::string> keys;
        for (size_t i = 0; i < 16; ++i) {
            complete_with(runtime, scan->next_sender());
            for (const auto &entry : scan->result().entries) {
                keys.push_back(Slice(entry.key).to_string());
            }
            if (scan->result().done) {
                break;
            }
        }
        EXPECT_TRUE(scan->result().done);
        EXPECT_EQ(keys, reverse ? (std::vector<std::string>{"z", "a"}) : (std::vector<std::string>{"a", "z"}));
    }
    std::shared_ptr<CapturedFrameStream> stream;
    ASSERT_TRUE(CapturedFrameStream::create(
                    state, loader, runtime, lane,
                    {
                        .encode = {.frame_bytes = 4096, .metadata = test.credits.credits.reservation()}
    },
                    {}, &stream)
                    .ok());
    for (size_t i = 0; i < 7; ++i) {
        complete_with(runtime, stream->sender());
        ASSERT_TRUE(stream->result().frame);
    }
    EXPECT_TRUE(stream->result().complete);
}

TEST(RecoveredStateBackend, LegacyMissingFencesRemainCompatibleButWrongByteFencesFail)
{
    for (bool wrong : {false, true}) {
        TestRecoveredGraph test;
        test.balanced();
        std::vector<uint8_t> raw(4096);
        ASSERT_TRUE(inner_frame_build(raw.data(), raw.size(), 0, {1, 2}, {"m"}));
        if (wrong) {
            const Slice lower("a");
            const Slice upper("y");
            ASSERT_TRUE(frame_set_fences(raw.data(), raw.size(), &lower, &upper));
        }
        test.add(0, raw);
        ASSERT_TRUE(test.prepare().ok());
        Completion done;
        test.run(&done);
        EXPECT_EQ(done.values.load(), wrong ? 0U : 1U);
        EXPECT_EQ(done.errors.load(), wrong ? 1U : 0U);
    }
}

TEST(RecoveredStateBackend, OverflowReadAndRecaptureNeedOnlyPrimaryDependencies)
{
    test_write::TestRecovery test;
    const std::string        value(12000, 'v');
    test.checkpoint(value);
    ASSERT_TRUE(test.prepare().ok());
    Completion recovered;
    test.run(&recovered);
    ASSERT_EQ(recovered.values.load(), 1U) << recovered.status.to_string();
    auto runtime = test.original.source.runtime;
    auto lane    = test.original.source.lane;
    auto ledger  = test.operation->result().ledger;
    auto loader  = std::make_shared<AsyncStorePageLoader>(test.store, runtime, lane, PageLoadLimits{},
                                                          OverflowBackingResolver{}, test.credits.credits);
    std::shared_ptr<RecoveredStateOperation> operation;
    ASSERT_TRUE(RecoveredStateOperation::create(test.operation->result().latest, loader, runtime, lane, {},
                                                test.credits.credits.reservation(), &operation)
                    .ok());
    complete_with(runtime, operation->sender());
    auto state = operation->result().state;
    ASSERT_TRUE(state);
    const auto &primary = state->mapping.get(state->root_page_id)->leaf->bases[0].backing;
    ASSERT_TRUE(primary->overflow);
    EXPECT_FALSE(primary->overflow->pages.empty());
    operation.reset();
    test.operation.reset();
    std::shared_ptr<PublishedGet> get;
    ASSERT_TRUE(PublishedGet::create(state, loader, "a", {}, &get).ok());
    complete_with(runtime, get->sender());
    ASSERT_TRUE(get->result().found);
    EXPECT_EQ(get->result().cell.value.to_string(), value);
    std::shared_ptr<CapturedWriteOperation> write;
    ASSERT_TRUE(CapturedWriteOperation::create(
                    state, loader, test.store, runtime, lane,
                    {.frames = {.encode = {.frame_bytes = 4096}}, .write = {.max_frame_bytes = 4096}},
                    test.credits.credits, {}, extent_write_reservation(ledger, test.credits.credits.reservation()), {},
                    &write)
                    .ok());
    complete_with(runtime, write->sender());
    ASSERT_TRUE(write->result().pages);
    EXPECT_EQ(write->result().written_frames, 1U);
    EXPECT_EQ(write->result().reused_frames, primary->overflow->pages.size());
}
} // namespace crowdb::tree
