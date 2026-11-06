// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "inner_load_test_support.h"
#include "load_budget_test_support.h"
#include "snapshot/captured_frames.h"

#include <stdexcept>

namespace crowdb::tree
{
namespace
{
using namespace test_read;

struct TestStream
{
    std::shared_ptr<detail::FlushRuntime>       runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    std::shared_ptr<detail::FlushRuntime::Lane> lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<CapturedFrameStream>        stream;

    [[nodiscard]] const CapturedFrame &frame() const
    {
        const auto &value = stream->result().frame;
        if (!value) {
            throw std::runtime_error("snapshot did not emit a frame");
        }
        return *value;
    }

    Status create(std::shared_ptr<const PublishedTreeState> captured, std::shared_ptr<PageLoader> loader = {},
                  CapturedFrameLimits limits = {}, CapturedFrameStream::ReserveOverflow reserve = {})
    {
        return CapturedFrameStream::create(std::move(captured), std::move(loader), runtime, lane, std::move(limits),
                                           std::move(reserve), &stream);
    }

    void next(Completion *done)
    {
        auto operation = stdexec::connect(stream->sender(), Receiver{.done = done});
        stdexec::start(operation);
        runtime->drain();
        EXPECT_EQ(done->values.load() + done->errors.load() + done->stopped.load(), 1U);
    }
};

std::shared_ptr<PublishedTreeState> snapshot_leaf(std::shared_ptr<const PageState> page, uint64_t coverage = 10)
{
    auto result          = std::make_shared<PublishedTreeState>(*view(std::move(page), {}, coverage));
    result->next_page_id = 1;
    return result;
}

std::shared_ptr<PublishedTreeState> snapshot_tree(size_t leaves, size_t value_bytes = 1)
{
    auto                         result = std::make_shared<PublishedTreeState>();
    std::vector<PageReplacement> changes;
    auto                         index = std::make_shared<InnerPageState>();
    for (size_t i = 0; i < leaves; ++i) {
        const auto   key   = std::string(4 - std::to_string(i).size(), '0') + std::to_string(i);
        const auto   upper = std::string(4 - std::to_string(i + 1).size(), '0') + std::to_string(i + 1);
        PageKeyRange range;
        if (i != 0) {
            range.lower = key;
            index->separators.emplace_back(key);
        }
        if (i + 1 != leaves) {
            range.upper = upper;
        }
        index->children.push_back(i);
        changes.push_back({
            .page_id     = i,
            .expected    = {                                                   },
            .replacement = published(page({{.key = key, .value = std::string(value_bytes, 'v')}},
                 range))
        });
    }
    auto root          = std::make_shared<PublishedPage>();
    root->level        = 1;
    root->metrics.leaf = false;
    root->inner        = index;
    changes.push_back({.page_id = leaves, .expected = {}, .replacement = root});
    MappingView::CopyStats copied;
    result->mapping      = result->mapping.replace(changes, copied);
    result->root_page_id = leaves;
    result->next_page_id = leaves + 1;
    result->coverage     = 10;
    return result;
}

TEST(CapturedFrames, FinalLeafOutputEncodesWithoutBuildingAnotherMapping)
{
    auto       output = published(page(
        {
            {.key = "a", .value = "final"}
    },
        {.upper = "m"}));
    TestStream test;
    ASSERT_TRUE(CapturedFrameStream::create({.page         = output,
                                             .page_id      = 17,
                                             .coverage     = 10,
                                             .gc_floor     = 0,
                                             .next_page_id = 24,
                                             .context      = {.range = {.upper = "m"}}},
                                            {}, test.runtime, test.lane, {.encode = {.frame_bytes = 512}}, {},
                                            &test.stream)
                    .ok());
    output.reset();
    Completion done;
    test.next(&done);
    ASSERT_EQ(done.values.load(), 1U);
    ASSERT_TRUE(test.stream->result().complete);
    EXPECT_EQ(test.stream->stats().pages, 1U);
    EXPECT_EQ(test.stream->stats().loads, 0U);
    EXPECT_EQ(test.frame().info.page_id, 17U);
    const auto &buffer = test.frame().buffer->bytes;
    ASSERT_TRUE(frame_validate(buffer.data(), buffer.size()));
    LeafFrameView leaf(buffer.data(), buffer.size());
    ASSERT_EQ(leaf.count(), 1U);
    EXPECT_EQ(leaf.key(0).to_string(), "a");
    EXPECT_EQ(CellView(leaf.cell(0)).value().to_string(), "final");
}

TEST(CapturedFrames, FinalIndexOutputUsesJoinedFencesWithoutTraversingChildren)
{
    auto inner        = std::make_shared<InnerPageState>();
    inner->children   = {7, 12};
    inner->separators = {"m"};
    auto output       = std::make_shared<PublishedPage>();
    output->level     = 2;
    output->inner     = inner;
    TestStream test;
    ASSERT_TRUE(
        CapturedFrameStream::create({.page         = output,
                                     .page_id      = 20,
                                     .coverage     = 10,
                                     .gc_floor     = 0,
                                     .next_page_id = 24,
                                     .context = {.inner_fences = InnerEncodeFences{.lower_leaf = 3, .upper_leaf = 9}}},
                                    {}, test.runtime, test.lane, {.encode = {.frame_bytes = 512}}, {}, &test.stream)
            .ok());
    Completion done;
    test.next(&done);
    ASSERT_EQ(done.values.load(), 1U);
    ASSERT_TRUE(test.stream->result().complete);
    EXPECT_EQ(test.stream->stats().pages, 1U);
    EXPECT_EQ(test.stream->stats().loads, 0U);
    EXPECT_EQ(test.frame().info.page_id, 20U);
    const auto &buffer = test.frame().buffer->bytes;
    ASSERT_TRUE(frame_validate(buffer.data(), buffer.size()));
    InnerFrameView index(buffer.data(), buffer.size());
    EXPECT_EQ(index.child_at(0), 7U);
    EXPECT_EQ(index.child_at(1), 12U);
    EXPECT_EQ(frame_lower_fence_page_id(buffer.data()), 3U);
    EXPECT_EQ(frame_upper_fence_page_id(buffer.data()), 9U);
}

TEST(CapturedFrames, MaterializationBytesFollowTheLastOutputOwnerAfterStreamDestruction)
{
    LoadCredits credits;
    TestStream  test;
    ASSERT_TRUE(test.create(snapshot_leaf(page({
                                {.key = "a", .value = "captured"}
    })),
                            {}, {.encode = {.frame_bytes = 4096, .metadata = credits.credits.reservation()}})
                    .ok());
    Completion done;
    test.next(&done);
    ASSERT_EQ(done.values.load(), 1U);
    auto retained = test.frame().buffer;
    ASSERT_TRUE(retained);
    EXPECT_EQ(credits.resources->snapshot().materialize_inflight_bytes, retained->bytes.capacity());
    EXPECT_EQ(retained->bytes.capacity(), 4096U);
    test.stream.reset();
    test.runtime->drain();
    credits.seal();
    EXPECT_EQ(credits.resources->snapshot().materialize_inflight_bytes, 4096U);
    retained.reset();
    EXPECT_EQ(credits.resources->snapshot().materialize_inflight_bytes, 0U);
}

TEST(CapturedFrames, CapturedMappingSurvivesUnvisitedReplacementAndFiltersFutureVersions)
{
    auto                   captured = snapshot_tree(2);
    PublicationCoordinator publisher;
    auto                   request = std::make_shared<PublicationRequest>();
    request->transaction.record    = std::make_shared<InstallRecord>(InstallToken{});
    for (uint64_t pid = 0; pid < 3; ++pid) {
        request->transaction.pages.push_back(
            {.page_id = pid, .expected = {}, .replacement = captured->mapping.get(pid)});
    }
    request->transaction.root_page_id = 2;
    auto sources                      = std::make_shared<PublishedSources>();
    sources->floor                    = 10;
    sources->tables.push_back(std::make_shared<MemTable>());
    request->transaction.expected_sources = publisher.capture()->sources;
    request->transaction.sources          = sources;
    request->transaction.coverage         = 10;
    request->on_done                      = [](auto result) { EXPECT_TRUE(result.status.ok()); };
    ASSERT_TRUE(publisher.try_submit(request));
    auto capture                           = std::make_shared<PublicationRequest>();
    capture->transaction.record            = std::make_shared<InstallRecord>(InstallToken{});
    capture->transaction.expected_coverage = 10;
    capture->transaction.checkpoint        = {.action = CheckpointAction::Capture, .token = 1};
    std::shared_ptr<const PublishedTreeState> fixed;
    capture->on_done = [&](auto result) {
        EXPECT_TRUE(result.status.ok());
        fixed = std::move(result.checkpoint_view);
    };
    ASSERT_TRUE(publisher.try_submit(capture));
    ASSERT_TRUE(fixed);
    EXPECT_TRUE(fixed->sources->tables.empty());
    TestStream test;
    ASSERT_TRUE(test.create(fixed, {}, {.encode = {.frame_bytes = 512}}).ok());
    Completion first;
    test.next(&first);
    ASSERT_EQ(first.values.load(), 1U);
    ASSERT_TRUE(test.stream->result().frame);
    EXPECT_EQ(test.frame().info.page_id, 0U);
    auto replace                = std::make_shared<PublicationRequest>();
    replace->transaction.record = std::make_shared<InstallRecord>(InstallToken{});
    replace->transaction.pages.push_back({
        .page_id     = 1,
        .expected    = publisher.capture()->mapping.get(1),
        .replacement = published(page({{.key = "0001", .slot = 11, .value = "new"}},
          {.lower = "0001"                            }
          ))
    });
    replace->on_done = [](auto result) { EXPECT_TRUE(result.status.ok()); };
    ASSERT_TRUE(publisher.try_submit(replace));
    Completion second;
    test.next(&second);
    ASSERT_EQ(second.values.load(), 1U);
    ASSERT_TRUE(test.stream->result().frame);
    const auto   &bytes = test.frame().buffer->bytes;
    LeafFrameView leaf(bytes.data(), bytes.size());
    ASSERT_EQ(leaf.count(), 1U);
    EXPECT_EQ(CellView(leaf.cell(0)).value().to_string(), "v");
    EXPECT_EQ(CellView(leaf.cell(0)).slot(), 1U);
    Completion root;
    test.next(&root);
    ASSERT_EQ(root.values.load(), 1U);
    EXPECT_EQ(test.frame().info.page_id, 2U);
    EXPECT_TRUE(test.stream->result().complete);
    EXPECT_EQ(publisher.capture()->mapping.get(1)->leaf->lookup("0001").cell.slot, 11U);
}

TEST(CapturedFrames, ColdIndexTraversalLoadsOnDemandWithOneCpuWorker)
{
    TestColdTree cold_tree;
    cold_tree.view->coverage     = 10;
    cold_tree.view->next_page_id = 7;
    cold_tree.store->deferred    = true;
    TestStream test;
    auto       loader = std::make_shared<AsyncStorePageLoader>(cold_tree.store, test.runtime, test.lane);
    ASSERT_TRUE(test.create(cold_tree.view, loader, {.encode = {.frame_bytes = 512}}).ok());
    std::vector<uint64_t> ids;
    for (size_t i = 0; i < 7; ++i) {
        Completion done;
        auto       operation = stdexec::connect(test.stream->sender(), Receiver{.done = &done});
        stdexec::start(operation);
        test.runtime->drain();
        for (size_t load = 0; load < 3 && done.values.load() + done.errors.load() == 0; ++load) {
            ASSERT_FALSE(cold_tree.store->pending.empty());
            cold_tree.store->finish();
            test.runtime->drain();
        }
        ASSERT_EQ(done.values.load(), 1U) << test.stream->result().status.to_string();
        ASSERT_TRUE(test.stream->result().frame);
        const auto &frame = test.frame();
        ids.push_back(frame.info.page_id);
        EXPECT_TRUE(frame_validate(frame.buffer->bytes.data(), frame.info.bytes));
    }
    EXPECT_EQ(ids, (std::vector<uint64_t>{3, 4, 1, 5, 6, 2, 0}));
    EXPECT_EQ(cold_tree.store->reads.load(), 3U);
    EXPECT_EQ(test.stream->stats().loads, 3U);
    EXPECT_TRUE(test.stream->result().complete);
    EXPECT_FALSE(cold_tree.view->mapping.get(0)->inner);
}

TEST(CapturedFrames, PinnedOutputAppliesBackpressureWithoutOverwritingOrAdvancing)
{
    TestStream test;
    ASSERT_TRUE(test.create(snapshot_tree(2), {}, {.encode = {.frame_bytes = 512}}).ok());
    Completion first;
    test.next(&first);
    ASSERT_EQ(first.values.load(), 1U);
    auto pinned = std::make_unique<CapturedFrame>(test.frame());
    ASSERT_TRUE(pinned);
    const auto crc = frame_u32(pinned->buffer->bytes.data(), pinned->info.bytes - kFrameTrailerSize);
    Completion rejected;
    test.next(&rejected);
    EXPECT_EQ(rejected.errors.load(), 1U);
    EXPECT_EQ(test.stream->result().status.code(), Code::kUnavailable);
    EXPECT_EQ(frame_u32(pinned->buffer->bytes.data(), pinned->info.bytes - kFrameTrailerSize), crc);
    EXPECT_EQ(test.stream->stats().frames, 1U);
    pinned.reset();
    Completion second;
    test.next(&second);
    ASSERT_EQ(second.values.load(), 1U);
    EXPECT_EQ(test.frame().info.page_id, 1U);
}

TEST(CapturedFrames, OverflowStreamsThroughOneWindowWithOwnedDisjointIdentities)
{
    TestStream        test;
    const std::string value(4096, 'x');
    auto              captured = snapshot_leaf(page({
        {.key = "a", .value = value},
        {.key = "b", .slot = 11, .value = "future"}
    }));
    auto              owner    = std::make_shared<int>(0);
    uint64_t          next     = captured->next_page_id;
    ASSERT_TRUE(test.create(captured,
                            {
    },
                            {.encode = {.frame_bytes = 256, .inline_value_bytes = 8}},
                            [owner, &next](uint64_t count, ReservedOverflowIds *out) {
                                *out = {.first = next, .count = count, .owner = owner};
                                next += count;
                                return Status::Ok();
                            })
                    .ok());
    const uint8_t *address = nullptr;
    std::string    restored;
    for (size_t i = 0; i < 100; ++i) {
        Completion done;
        test.next(&done);
        ASSERT_EQ(done.values.load(), 1U) << test.stream->result().status.to_string();
        ASSERT_TRUE(test.stream->result().frame);
        const auto &frame = test.frame();
        if (address == nullptr) {
            address = frame.buffer->bytes.data();
        }
        EXPECT_EQ(frame.buffer->bytes.data(), address);
        EXPECT_TRUE(frame_validate(address, frame.info.bytes));
        if (frame.info.type == page_type::kOverflowFrame) {
            OverflowFrameView overflow(address, frame.info.bytes);
            restored += overflow.payload().to_string();
        }
        else {
            LeafFrameView leaf(address, frame.info.bytes);
            ASSERT_EQ(leaf.count(), 1U);
            EXPECT_EQ(CellView(leaf.cell(0)).overflow_head(), 1U);
            EXPECT_EQ(CellView(leaf.cell(0)).overflow_len(), value.size());
        }
        if (test.stream->result().complete) {
            break;
        }
    }
    EXPECT_TRUE(test.stream->result().complete);
    EXPECT_EQ(restored, value);
    EXPECT_EQ(test.stream->stats().encoding.overflow_value_copy_bytes, value.size());
    EXPECT_EQ(test.stream->stats().encoding.primary_frames, 1U);
}

TEST(CapturedFrames, CancelledColdLeafRetainsCallbackOwnershipAndDoesNotProduceLateFrames)
{
    LoadCredits credits;
    auto        leaf = std::make_shared<PageState>();
    leaf->bases.push_back(cold(1));
    auto loader       = std::make_shared<TestLoader>();
    loader->deferred  = true;
    loader->frames[1] = base({
        {.key = "a", .value = "value"}
    });
    TestStream test;
    ASSERT_TRUE(test.create(snapshot_leaf(leaf), loader,
                            {
                                .encode = {.frame_bytes = 512, .metadata = credits.credits.reservation()}
    })
                    .ok());
    Completion done;
    auto       operation = stdexec::connect(test.stream->sender(), Receiver{.done = &done});
    stdexec::start(operation);
    test.runtime->drain();
    EXPECT_EQ(loader->calls, 1U);
    EXPECT_EQ(done.values.load(), 0U);
    test.stream->request_stop();
    test.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 1U);
    test.stream.reset();
    credits.seal();
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    loader->finish();
    test.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_EQ(done.values.load(), 0U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(CapturedFrames, OutputMemoryIsBoundedAcrossManyPagesUnderExhaustedHostBudget)
{
    LoadCredits credits;
    TestStream  test;
    ASSERT_TRUE(
        test.create(snapshot_tree(80),
                    {
    },
                    {.encode = {.frame_bytes = 4096, .max_entries = 128, .metadata = credits.credits.reservation()}})
            .ok());
    size_t peak    = 0;
    size_t outputs = 0;
    for (size_t i = 0; i < 81; ++i) {
        Completion done;
        test.next(&done);
        ASSERT_EQ(done.values.load(), 1U) << test.stream->result().status.to_string();
        ASSERT_TRUE(test.stream->result().frame);
        ++outputs;
        peak = std::max(peak, credits.budget.snapshot().allocated[LoadCredits::kBytes]);
    }
    EXPECT_EQ(outputs, 81U);
    EXPECT_TRUE(test.stream->result().complete);
    EXPECT_LT(peak, outputs * 4096 / 2);
    RecordProperty("peak_stream_allocated_bytes", static_cast<int>(peak));
    RecordProperty("encoded_output_bytes", static_cast<int>(outputs * 4096));
    test.stream.reset();
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(CapturedFrames, RejectsPartialCaptureAndOversizedLayoutBeforeEmittingPage)
{
    TestStream test;
    auto       captured       = snapshot_leaf(page({
        {.key = "a", .value = std::string(500, 'v')}
    }));
    captured->pending_handoff = {.batch = 1, .frontier = 11};
    EXPECT_EQ(test.create(captured).code(), Code::kUnavailable);
    captured->pending_handoff.reset();
    ASSERT_TRUE(test.create(captured,
                            {
    },
                            {.encode = {.frame_bytes = 256, .inline_value_bytes = 1000}})
                    .ok());
    Completion done;
    test.next(&done);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(test.stream->result().status.code(), Code::kResourceExhausted);
    EXPECT_FALSE(test.stream->result().frame);
    EXPECT_EQ(test.stream->stats().frames, 0U);
}

TEST(CapturedFrames, RuntimeShutdownWhileLoadingReturnsErrorWithoutLateOutput)
{
    auto leaf = std::make_shared<PageState>();
    leaf->bases.push_back(cold(1));
    auto loader       = std::make_shared<TestLoader>();
    loader->deferred  = true;
    loader->frames[1] = base({
        {.key = "a", .value = "value"}
    });
    TestStream test;
    ASSERT_TRUE(test.create(snapshot_leaf(leaf), loader).ok());
    Completion done;
    auto       operation = stdexec::connect(test.stream->sender(), Receiver{.done = &done});
    stdexec::start(operation);
    test.runtime->drain();
    ASSERT_EQ(loader->calls, 1U);
    test.runtime->close();
    loader->finish();
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_FALSE(test.stream->result().frame);
    EXPECT_EQ(test.stream->stats().frames, 0U);
}

TEST(CapturedFrames, GcEmptyLeavesDoNotBecomeInnerFenceEndpoints)
{
    auto captured      = snapshot_tree(3);
    captured->gc_floor = 10;
    MappingView::CopyStats               copied;
    const std::array<PageReplacement, 2> changes{
        {{.page_id  = 0,
          .expected = {},
          .replacement =
              published(page({{.key = "0000", .slot = 2, .value = "", .tombstone = true}}, {.upper = "0001"}))},
         {.page_id  = 2,
          .expected = {},
          .replacement =
              published(page({{.key = "0002", .slot = 2, .value = "", .tombstone = true}}, {.lower = "0002"}))}}
    };
    captured->mapping = captured->mapping.replace(changes, copied);
    TestStream test;
    ASSERT_TRUE(test.create(captured, {}, {.encode = {.frame_bytes = 512}}).ok());
    for (size_t i = 0; i < 4; ++i) {
        Completion done;
        test.next(&done);
        ASSERT_EQ(done.values.load(), 1U) << test.stream->result().status.to_string();
    }
    const auto &frame = test.frame();
    EXPECT_EQ(frame.info.page_id, 3U);
    EXPECT_EQ(frame_lower_fence_page_id(frame.buffer->bytes.data()), 1U);
    EXPECT_EQ(frame_upper_fence_page_id(frame.buffer->bytes.data()), 1U);
}

TEST(CapturedFrames, ReusedOverflowDependencyOutlivesStreamAndCapturedInput)
{
    auto               logical    = std::make_shared<PageState>();
    auto               backing    = std::make_shared<BackingRef>();
    auto               protection = std::make_shared<int>(1);
    std::weak_ptr<int> weak       = protection;
    backing->owner                = protection;
    backing->length               = 4096;
    std::vector<leaf_entry> entries;
    entries.push_back({.key = "a", .cell = encode_overflow_cell_buf(2, 40, 1000)});
    auto base = std::shared_ptr<const LeafBase>(LeafBase::build(entries));
    logical->bases.push_back({.identity = 1, .resident = base, .backing = backing});
    auto captured          = snapshot_leaf(logical);
    captured->next_page_id = 100;
    TestStream test;
    ASSERT_TRUE(test.create(captured, {}, {.encode = {.frame_bytes = 512}}).ok());
    Completion done;
    test.next(&done);
    ASSERT_EQ(done.values.load(), 1U);
    auto frame = std::make_unique<CapturedFrame>(test.frame());
    ASSERT_EQ(frame->reused_overflow.size(), 1U);
    EXPECT_EQ(frame->reused_overflow[0].head, 40U);
    EXPECT_EQ(frame->reused_overflow[0].bytes, 1000U);
    EXPECT_EQ(test.stream->stats().encoding.overflow_frames, 0U);
    test.stream.reset();
    captured.reset();
    logical.reset();
    backing.reset();
    protection.reset();
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(frame->reused_overflow[0].head, 40U);
    frame.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(CapturedFrames, ColdLeafFailureAndInvalidSlotProofNeverBecomeEmptySuccess)
{
    for (const bool io_failure : {false, true}) {
        auto logical = std::make_shared<PageState>();
        logical->bases.push_back(cold(1, 2));
        auto loader       = std::make_shared<TestLoader>();
        loader->deferred  = true;
        loader->frames[1] = base({
            {.key = "a", .slot = 3, .value = "value"}
        });
        TestStream test;
        ASSERT_TRUE(test.create(snapshot_leaf(logical), loader).ok());
        Completion done;
        auto       operation = stdexec::connect(test.stream->sender(), Receiver{.done = &done});
        stdexec::start(operation);
        test.runtime->drain();
        loader->finish(io_failure ? Status::io_error() : Status::Ok());
        test.runtime->drain();
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(test.stream->result().status.code(), io_failure ? Code::kIoError : Code::kCorruption);
        EXPECT_FALSE(test.stream->result().frame);
        EXPECT_FALSE(test.stream->result().complete);
        EXPECT_EQ(test.stream->stats().frames, 0U);
    }
}

TEST(CapturedFrames, ReusedNewOverflowIntervalFailsBeforeItCanOverwriteEarlierFrames)
{
    auto       captured = snapshot_tree(2, 512);
    auto       owner    = std::make_shared<int>(0);
    TestStream test;
    ASSERT_TRUE(test.create(captured,
                            {
    },
                            {.encode = {.frame_bytes = 256, .inline_value_bytes = 8}},
                            [owner](uint64_t count, ReservedOverflowIds *out) {
                                *out = {.first = 10, .count = count, .owner = owner};
                                return Status::Ok();
                            })
                    .ok());
    size_t errors = 0;
    for (size_t i = 0; i < 16; ++i) {
        Completion done;
        test.next(&done);
        if (done.errors.load() != 0) {
            ++errors;
            break;
        }
        ASSERT_EQ(done.values.load(), 1U);
    }
    EXPECT_EQ(errors, 1U);
    EXPECT_EQ(test.stream->result().status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(test.stream->result().frame);
    EXPECT_EQ(test.stream->stats().encoding.primary_frames, 1U);
}

struct TestNextReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    detail::InlineFunction<void()>       next;
    detail::InlineFunction<void(Status)> error;

    void set_value() const noexcept
    {
        try {
            next();
        }
        catch (const std::exception &error) {
            ADD_FAILURE() << error.what();
        }
        catch (...) {
            ADD_FAILURE() << "stream receiver threw";
        }
    }

    void set_error(Status status) const noexcept
    {
        try {
            if (error) {
                error(std::move(status));
            }
            else {
                ADD_FAILURE() << status.to_string();
            }
        }
        catch (...) {
            ADD_FAILURE() << "stream error receiver threw";
        }
    }

    void set_stopped() const noexcept
    {
        ADD_FAILURE() << "unexpected stream cancellation";
    }
};

struct TestNextOperation
{
    TestNextOperation(detail::CallbackSender sender, TestNextReceiver receiver)
        : operation(stdexec::connect(sender, std::move(receiver)))
    {
    }

    stdexec::connect_result_t<detail::CallbackSender, TestNextReceiver> operation;
};

TEST(CapturedFrames, ReceiverCanRequestNextAndDropLastHandleInsideCompletion)
{
    TestStream test;
    ASSERT_TRUE(test.create(snapshot_tree(64), {}, {.encode = {.frame_bytes = 4096}}).ok());
    std::vector<std::unique_ptr<TestNextOperation>> operations;
    operations.reserve(65);
    size_t completed = 0;
    for (size_t i = 0; i < 65; ++i) {
        operations.push_back(std::make_unique<TestNextOperation>(
            test.stream->sender(), TestNextReceiver{.next =
                                                        [&] {
                                                            EXPECT_EQ(test.frame().info.page_id, completed);
                                                            ++completed;
                                                            if (completed == 65) {
                                                                EXPECT_TRUE(test.stream->result().complete);
                                                                test.stream.reset();
                                                            }
                                                            else {
                                                                stdexec::start(operations[completed]->operation);
                                                            }
                                                        },
                                                    .error = {}}));
    }
    stdexec::start(operations[0]->operation);
    test.runtime->drain();
    EXPECT_EQ(completed, 65U);
    EXPECT_FALSE(test.stream);
}

TEST(CapturedFrames, ReentrantErrorReceiverCannotTurnFailedColdLoadIntoEmptyCompletion)
{
    auto logical = std::make_shared<PageState>();
    logical->bases.push_back(cold(1));
    auto loader       = std::make_shared<TestLoader>();
    loader->deferred  = true;
    loader->frames[1] = base({
        {.key = "a", .value = "value"}
    });
    TestStream test;
    ASSERT_TRUE(test.create(snapshot_leaf(logical), loader).ok());
    std::vector<std::unique_ptr<TestNextOperation>> operations;
    operations.reserve(2);
    size_t errors = 0;
    for (size_t i = 0; i < 2; ++i) {
        operations.push_back(std::make_unique<TestNextOperation>(
            test.stream->sender(), TestNextReceiver{.next = [] { ADD_FAILURE() << "failed stream returned success"; },
                                                    .error =
                                                        [&](Status status) {
                                                            EXPECT_EQ(status.code(), Code::kIoError);
                                                            ++errors;
                                                            if (errors == 1) {
                                                                stdexec::start(operations[1]->operation);
                                                            }
                                                        }}));
    }
    stdexec::start(operations[0]->operation);
    test.runtime->drain();
    loader->finish(Status::io_error());
    test.runtime->drain();
    EXPECT_EQ(errors, 2U);
    EXPECT_FALSE(test.stream->result().frame);
    EXPECT_FALSE(test.stream->result().complete);
}

TEST(CapturedFrames, RejectedNextAfterSuccessfulFrameClearsPreviousOutput)
{
    TestStream test;
    ASSERT_TRUE(test.create(snapshot_tree(2)).ok());
    Completion first;
    test.next(&first);
    ASSERT_EQ(first.values.load(), 1U);
    ASSERT_TRUE(test.stream->result().frame);
    test.runtime->close();
    Completion failed;
    test.next(&failed);
    EXPECT_EQ(failed.errors.load(), 1U);
    EXPECT_FALSE(test.stream->result().frame);
    EXPECT_FALSE(test.stream->result().complete);
}
} // namespace
} // namespace crowdb::tree
