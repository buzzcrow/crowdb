// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_batch.h"
#include "btree/page_load.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree
{
namespace
{
class RoutingLoader final : public PageLoader
{
  public:
    std::promise<void> entered;
    InnerComplete      pending;
    size_t             calls = 0;

    void load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete) override
    {
        EXPECT_TRUE(backing->reloadable());
        ++calls;
        pending = std::move(complete);
        entered.set_value();
    }

    void load_leaf(BaseRef base, LeafComplete complete) override
    {
        (void)base;
        ADD_FAILURE() << "handoff must not load cold leaves";
        complete(Status::internal_error(), {});
    }

    void finish(Status status = Status::Ok())
    {
        auto inner        = std::make_shared<InnerPageState>();
        inner->children   = {0, 1};
        inner->separators = {"m"};
        auto callback     = std::move(pending);
        entered           = std::promise<void>();
        callback(std::move(status), std::move(inner));
    }
};

void seed(PublicationCoordinator &publisher)
{
    auto left          = std::make_shared<PageState>();
    left->range.upper  = "m";
    auto right         = std::make_shared<PageState>();
    right->range.lower = "m";
    auto backing       = std::make_shared<BackingRef>();
    backing->length    = 4096;
    backing->owner     = std::make_shared<int>(0);
    right->bases.push_back({.range = right->range, .max_slot = 0, .backing = backing});
    auto first          = std::make_shared<PublishedPage>();
    first->leaf         = left;
    auto second         = std::make_shared<PublishedPage>();
    second->leaf        = right;
    auto root           = std::make_shared<PublishedPage>();
    root->level         = 1;
    root->metrics.leaf  = false;
    root->inner_backing = backing;
    auto table          = std::make_shared<MemTable>();
    ASSERT_TRUE(table->upsert("z", 1, encode_cell_buf(1, OpKind::kPut, "payload")));
    table->close();
    auto sources = std::make_shared<PublishedSources>();
    sources->tables.push_back(std::move(table));
    auto request         = std::make_shared<PublicationRequest>();
    request->transaction = {
        .record           = std::make_shared<InstallRecord>(InstallToken{}
                    ),
        .pages            = {{.page_id = 0, .expected = {}, .replacement = first},
                                                                         {.page_id = 1, .expected = {}, .replacement = second},
                                                                         {.page_id = 2, .expected = {}, .replacement = root}},
        .root_page_id     = 2,
        .expected_sources = publisher.capture()->sources,
        .sources          = sources
    };
    request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()) << result.status.to_string(); };
    ASSERT_TRUE(publisher.try_submit(request));
}

struct Receiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::promise<void> *done;

    void set_value() const noexcept
    {
        done->set_value();
    }

    void set_error(Status status) const noexcept
    {
        ADD_FAILURE() << status.to_string();
        done->set_value();
    }

    void set_stopped() const noexcept
    {
        ADD_FAILURE() << "flush must return its progress result";
        done->set_value();
    }
};

TEST(FlushRouting, ColdIndexWaitReleasesSingleWorkerAndNeverLoadsLeaf)
{
    detail::FlushRuntime   runtime(1, 8);
    auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
    PublicationCoordinator publisher;
    seed(publisher);
    auto              loader = std::make_shared<RoutingLoader>();
    FlushBatchOptions options;
    options.loader                  = loader;
    options.routing_leaves_per_step = 1;
    std::shared_ptr<FlushBatch> batch;
    ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 1, 1, options, {}, &batch).ok());
    std::promise<void> done;
    auto               terminal  = done.get_future();
    auto               entered   = loader->entered.get_future();
    auto               operation = stdexec::connect(batch->sender(), Receiver{&done});
    stdexec::start(operation);
    entered.get();
    EXPECT_EQ(publisher.capture()->coverage, 0U);
    EXPECT_TRUE(publisher.capture()->pending_handoff);
    std::promise<void> spare;
    auto               spare_task = spare.get_future();
    ASSERT_TRUE(
        runtime
            .try_submit(
                lane, [](void *context) noexcept { static_cast<std::promise<void> *>(context)->set_value(); }, &spare)
            .ok());
    spare_task.get();
    loader->finish();
    terminal.get();
    runtime.drain();
    EXPECT_TRUE(batch->result().handoff_status.ok()) << batch->result().handoff_status.to_string();
    EXPECT_EQ(batch->result().published_coverage, 1U);
    EXPECT_EQ(loader->calls, 1U);
    EXPECT_TRUE(publisher.capture()->mapping.get(0)->leaf->overlays.empty());
    EXPECT_EQ(publisher.capture()->mapping.get(1)->leaf->lookup("z").cell.value.to_string(), "payload");
    EXPECT_FALSE(publisher.capture()->mapping.get(2)->inner);
    EXPECT_FALSE(publisher.capture()->pending_handoff);
}

TEST(FlushRouting, LoadFailureAndCancellationRetainRepairIdentity)
{
    for (bool cancel : {false, true}) {
        detail::FlushRuntime   runtime(1, 8);
        auto                   lane = std::make_shared<detail::FlushRuntime::Lane>(8);
        PublicationCoordinator publisher;
        seed(publisher);
        auto              loader = std::make_shared<RoutingLoader>();
        FlushBatchOptions options;
        options.loader = loader;
        std::shared_ptr<FlushBatch> batch;
        ASSERT_TRUE(FlushBatch::prepare(publisher, runtime, lane, 7, 1, options, {}, &batch).ok());
        for (size_t attempt = 0; attempt < 2; ++attempt) {
            auto               entered = loader->entered.get_future();
            std::promise<void> done;
            auto               terminal  = done.get_future();
            auto               operation = stdexec::connect(batch->sender(), Receiver{&done});
            stdexec::start(operation);
            entered.get();
            if (attempt == 0 && cancel) {
                batch->request_stop();
            }
            loader->finish(attempt == 0 && !cancel ? Status::io_error("controlled cold index failure") : Status::Ok());
            terminal.get();
            runtime.drain();
            if (attempt == 0) {
                EXPECT_FALSE(batch->result().handoff_status.ok());
                EXPECT_EQ(batch->result().published_coverage, 0U);
                const auto pending = publisher.capture()->pending_handoff;
                ASSERT_TRUE(pending.has_value());
                if (pending) {
                    EXPECT_EQ(pending->batch, 7U);
                }
                EXPECT_EQ(publisher.capture()->sources->tables.size(), 1U);
            }
        }
        EXPECT_TRUE(batch->result().handoff_status.ok());
        EXPECT_EQ(batch->result().published_coverage, 1U);
        EXPECT_FALSE(publisher.capture()->pending_handoff);
    }
}
} // namespace
} // namespace crowdb::tree
