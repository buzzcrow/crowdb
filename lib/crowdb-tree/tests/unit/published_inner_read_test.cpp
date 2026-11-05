// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "inner_load_test_support.h"

#include <algorithm>

namespace crowdb::tree
{
using namespace test_read;

TEST(PublishedInnerRead, StandaloneRouteOwnsMappingAndResumesExactPath)
{
    TestColdTree         tree;
    PublishedRouteCursor route(tree.view->root_page_id, tree.view->mapping, 8, false);
    tree.view.reset();
    ASSERT_TRUE(route.seek("m", false).ok());
    ASSERT_NE(route.needs_inner(), nullptr);
    EXPECT_EQ(route.needs_inner()->address, 0U);
    ASSERT_TRUE(route.resume_inner(tree.loader->states[0]).ok());
    ASSERT_NE(route.needs_inner(), nullptr);
    EXPECT_EQ(route.needs_inner()->address, 2U);
    ASSERT_TRUE(route.resume_inner(tree.loader->states[2]).ok());
    EXPECT_EQ(route.needs_inner(), nullptr);
    ASSERT_NE(route.leaf(), nullptr);
    EXPECT_EQ(route.leaf()->lookup("m").cell.value.to_string(), "m");
    ASSERT_TRUE(route.next().ok());
    EXPECT_EQ(route.leaf()->lookup("t").cell.value.to_string(), "t");
    EXPECT_EQ(route.resume_inner(tree.loader->states[0]).code(), Code::kInvalidArgument);
}

TEST(PublishedInnerRead, PointInlineLoadsBothLevelsWithoutMutatingCapturedMapping)
{
    TestColdTree                  tree;
    std::shared_ptr<PublishedGet> request;
    ASSERT_TRUE(PublishedGet::create(tree.view, tree.loader, "m", {}, &request).ok());
    complete_inline(request->sender());
    EXPECT_TRUE(request->result().found);
    EXPECT_EQ(request->result().cell.value.to_string(), "m");
    EXPECT_EQ(tree.loader->calls, 2U);
    EXPECT_EQ(tree.view->mapping.get(0)->inner, nullptr);
    EXPECT_EQ(tree.view->mapping.get(2)->inner, nullptr);
}

TEST(PublishedInnerRead, LoadedRootIsConsumedBeforeTraversingAResidentChild)
{
    TestColdTree tree;
    auto         child = std::make_shared<PublishedPage>(*tree.view->mapping.get(2));
    child->inner       = tree.loader->states[2];
    const PageReplacement  change{.page_id = 2, .expected = tree.view->mapping.get(2), .replacement = child};
    MappingView::CopyStats copied;
    tree.view->mapping = tree.view->mapping.replace({&change, 1}, copied);
    for (bool reverse : {false, true}) {
        PublishedRouteCursor route(tree.view->root_page_id, tree.view->mapping, 3, reverse);
        ASSERT_TRUE(route.seek("m", false).ok());
        ASSERT_NE(route.needs_inner(), nullptr);
        ASSERT_TRUE(route.resume_inner(tree.loader->states[0]).ok());
        EXPECT_EQ(route.needs_inner(), nullptr);
        EXPECT_EQ(route.leaf_page_id(), 5U);
        ASSERT_NE(route.leaf(), nullptr);
        EXPECT_EQ(route.leaf()->lookup("m").cell.value.to_string(), "m");
    }
}

TEST(PublishedInnerRead, ScanCrossesColdIndexesInBothDirectionsWithSingleWorker)
{
    TestColdTree tree;
    auto         runtime = std::make_shared<detail::FlushRuntime>(1, 1);
    auto         lane    = std::make_shared<detail::FlushRuntime::Lane>(1);
    auto loader = std::make_shared<AsyncStorePageLoader>(tree.store, runtime, lane, PageLoadLimits{.max_inflight = 1});
    for (bool reverse : {false, true}) {
        std::shared_ptr<PublishedScan> scan;
        ASSERT_TRUE(PublishedScan::create(tree.view, loader, {.lower = "g", .upper = "t"}, reverse,
                                          {.max_work = 2, .max_entries = 1}, &scan)
                        .ok());
        std::vector<std::string> keys;
        for (size_t window = 0; window < 40; ++window) {
            Completion done;
            auto       operation = stdexec::connect(scan->next_sender(), Receiver{.done = &done});
            stdexec::start(operation);
            runtime->drain();
            ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
            ASSERT_EQ(done.errors.load(), 0U);
            EXPECT_LE(scan->result().work, 2U);
            for (const auto &entry : scan->result().entries) {
                keys.push_back(Slice(entry.key).to_string());
            }
            if (scan->result().done) {
                break;
            }
        }
        EXPECT_TRUE(scan->result().done);
        std::vector<std::string> expected{"g", "l", "m", "n"};
        if (reverse) {
            std::reverse(expected.begin(), expected.end());
        }
        EXPECT_EQ(keys, expected);
    }
    EXPECT_EQ(tree.store->reads.load(), 6U);
}

TEST(PublishedInnerRead, LateLoadUsesOldMappingAfterReplacement)
{
    TestColdTree tree;
    tree.loader->deferred = true;
    std::shared_ptr<PublishedGet> request;
    ASSERT_TRUE(PublishedGet::create(tree.view, tree.loader, "m", {}, &request).ok());
    Completion done;
    auto       operation = stdexec::connect(request->sender(), Receiver{.done = &done});
    stdexec::start(operation);
    auto                                 changed = std::make_shared<PublishedTreeState>(*tree.view);
    const std::array<PageReplacement, 1> changes{
        {{.page_id     = 5,
          .expected    = tree.view->mapping.get(5),
          .replacement = published(page({{.key = "m", .slot = 2, .value = "new"}}, {.lower = "m", .upper = "t"}))}}
    };
    MappingView::CopyStats copied;
    changed->mapping = changed->mapping.replace(changes, copied);
    tree.view        = std::move(changed);
    tree.loader->finish();
    EXPECT_EQ(done.values.load(), 0U);
    tree.loader->finish();
    EXPECT_EQ(done.values.load(), 1U);
    EXPECT_EQ(request->result().cell.value.to_string(), "m");
}

TEST(PublishedInnerRead, LateFailureAndCancellationCompleteExactlyOnce)
{
    for (bool stopped : {false, true}) {
        TestColdTree tree;
        tree.loader->deferred = true;
        std::shared_ptr<PublishedGet> request;
        ASSERT_TRUE(PublishedGet::create(tree.view, tree.loader, "m", {}, &request).ok());
        Completion done;
        auto       operation = stdexec::connect(request->sender(), Receiver{.done = &done});
        stdexec::start(operation);
        if (stopped) {
            request->request_stop();
            request.reset();
        }
        tree.loader->finish(Status::io_error("index read failed"));
        EXPECT_EQ(done.values.load(), 0U);
        EXPECT_EQ(done.errors.load(), stopped ? 0U : 1U);
        EXPECT_EQ(done.stopped.load(), stopped ? 1U : 0U);
        EXPECT_EQ(tree.loader->calls, 1U);
    }
}

TEST(PublishedInnerRead, RejectsMissingChildInvalidSeparatorsAndNullLoadedState)
{
    for (size_t scenario = 0; scenario < 3; ++scenario) {
        TestColdTree tree;
        if (scenario == 0) {
            tree.loader->states[0] = inner({"m"}, {1, 99});
        }
        else if (scenario == 1) {
            tree.loader->states[2] = inner({"a"}, {5, 6});
        }
        else {
            tree.loader->states[0].reset();
        }
        std::shared_ptr<PublishedGet> request;
        ASSERT_TRUE(PublishedGet::create(tree.view, tree.loader, "m", {}, &request).ok());
        Completion done;
        auto       operation = stdexec::connect(request->sender(), Receiver{.done = &done});
        stdexec::start(operation);
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(request->result().status.code(), Code::kCorruption);
    }
}

TEST(PublishedInnerRead, MissingTransportReturnsUnavailableAndScanCancelsPendingIndex)
{
    TestColdTree                  tree;
    std::shared_ptr<PublishedGet> request;
    ASSERT_TRUE(PublishedGet::create(tree.view, {}, "m", {}, &request).ok());
    Completion done;
    auto       operation = stdexec::connect(request->sender(), Receiver{.done = &done});
    stdexec::start(operation);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_EQ(request->result().status.code(), Code::kUnavailable);
    tree.loader->deferred = true;
    std::shared_ptr<PublishedScan> scan;
    ASSERT_TRUE(PublishedScan::create(tree.view, tree.loader, {}, true, {}, &scan).ok());
    Completion scanned;
    auto       range = stdexec::connect(scan->next_sender(), Receiver{.done = &scanned});
    stdexec::start(range);
    scan->request_stop();
    tree.loader->finish();
    EXPECT_EQ(scanned.stopped.load(), 1U);
    EXPECT_EQ(scanned.values.load(), 0U);
    EXPECT_EQ(scanned.errors.load(), 0U);
    EXPECT_EQ(tree.loader->calls, 1U);
}
} // namespace crowdb::tree
