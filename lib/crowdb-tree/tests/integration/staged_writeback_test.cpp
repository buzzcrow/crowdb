// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/checkpoint_operation.h"
#include "snapshot/operation_wait.h"
#include "test_tmp.h"

namespace crowdb::tree
{
namespace
{
using namespace test_write;

struct TestStagedWriteback
{
    TestCheckpoint                            local;
    std::shared_ptr<LocalBackendState>        backend = std::make_shared<LocalBackendState>();
    std::shared_ptr<const PublishedTreeState> initial = [] {
        auto result = captured_leaf(page({
            {.key = "a", .value = "old"}
        }));
        EXPECT_TRUE(apply_page_metrics(result->stats, nullptr, &result->mapping.get(0)->metrics));
        return result;
    }();
    PublicationCoordinator publisher{
        4, 1, {},
          std::pmr::get_default_resource(), {},
          {.metadata = {}, .initial = initial, .source_catalog = {}}
    };
    std::shared_ptr<CheckpointOperation> operation;

    TestStagedWriteback()
    {
        backend->store       = local.store;
        backend->ledger      = local.source.ledger;
        backend->checkpoints = local.registry;
    }

    Status prepare()
    {
        auto reserved = local.source.credits.credits.reservation()->take_reserved(
            CheckpointOperation::finish_allocation_bound() + 512);
        if (!reserved) {
            return Status::resource_exhausted();
        }
        auto              finish = detail::AllocationReservation::create(std::move(*reserved));
        CheckpointOptions options;
        options.mode                            = CheckpointMode::Writeback;
        options.write.frames.encode.frame_bytes = 4096;
        options.write.write.max_frame_bytes     = 4096;
        auto loader                             = std::make_shared<test_read::TestLoader>();
        return CheckpointOperation::create({}, publisher, backend, local.store, local.source.runtime, local.source.lane,
                                           options, local.source.credits.credits, finish, loader, {}, {}, &operation);
    }
};

TEST(StagedWriteback, ReloadablePagesDoNotAdvanceTheDurableCheckpoint)
{
    TestStagedWriteback test;
    ASSERT_TRUE(test.prepare().ok());
    const auto status = detail::wait_snapshot_operation(test.operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto &result = test.operation->result();
    EXPECT_TRUE(result.writeback_completed);
    EXPECT_FALSE(result.durable_committed);
    EXPECT_EQ(result.captured_coverage, 10U);
    EXPECT_EQ(result.rebased_pages, 1U);
    EXPECT_EQ(result.durable_coverage, 0U);
    EXPECT_EQ(result.snapshot_sequence, 0U);
    EXPECT_EQ(test.local.store->events, std::vector<std::string>{"metadata"});
    auto view = test.publisher.capture();
    EXPECT_FALSE(view->checkpoint_inflight);
    ASSERT_TRUE(view->mapping.get(0)->leaf->bases.front().backing->reloadable());
    EXPECT_FALSE(view->mapping.get(0)->metrics.dirty);
    EXPECT_EQ(view->stats.checkpoint_pending_pages, test.initial->stats.checkpoint_pending_pages);
    const auto metrics = test.publisher.work_metrics()->totals();
    EXPECT_EQ(metrics[static_cast<size_t>(TreeStage::SnapshotCommit)].attempts, 0U);
    EXPECT_EQ(metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)].inflight, 0U);
    EXPECT_EQ(metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)]
                  .outcomes[static_cast<size_t>(TreeOutcome::Success)],
              1U);
    test.operation.reset();
    ASSERT_TRUE(test.prepare().ok());
    ASSERT_TRUE(detail::wait_snapshot_operation(test.operation).ok());
    EXPECT_EQ(test.publisher.work_metrics()->totals()[static_cast<size_t>(TreeStage::PageWritebackPipeline)].attempts,
              metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)].attempts);
}

TEST(StagedWriteback, ANewerPublicationSurvivesDelayedWriteback)
{
    TestStagedWriteback test;
    test.local.store->defer_at = 1;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    ASSERT_TRUE(test.local.store->pending);
    auto before                 = test.publisher.capture()->mapping.get(0);
    auto replacement            = std::make_shared<PublishedPage>(*before);
    auto leaf                   = std::make_shared<PageState>(*page({
        {.key = "a", .value = "new"}
    }));
    leaf->content_id            = before->leaf->content_id + 1;
    replacement->leaf           = leaf;
    auto request                = std::make_shared<PublicationRequest>();
    request->transaction.record = std::make_shared<InstallRecord>(InstallToken{.sequence = 123});
    request->transaction.pages.push_back({.page_id = 0, .expected = before, .replacement = replacement});
    request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
    ASSERT_TRUE(test.publisher.try_submit(request));
    test.local.store->finish();
    test.local.source.runtime->drain();
    EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_EQ(test.operation->result().rebased_pages, 0U);
    EXPECT_EQ(test.operation->result().rebase_deferred, 1U);
    EXPECT_EQ(test.publisher.capture()->mapping.get(0), replacement);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    EXPECT_EQ(test.local.registry->durable_coverage(), 0U);
}

TEST(StagedWriteback, CancelRetainsTheAcceptedWriteUntilItsCallback)
{
    TestStagedWriteback test;
    test.local.store->defer_at = 1;
    ASSERT_TRUE(test.prepare().ok());
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    ASSERT_TRUE(test.local.store->pending);
    test.operation->request_stop();
    test.local.source.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 0U);
    EXPECT_TRUE(test.publisher.capture()->checkpoint_inflight);
    test.local.store->finish();
    test.local.source.runtime->drain();
    EXPECT_EQ(done.stopped.load(), 1U);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    EXPECT_EQ(test.local.registry->durable_coverage(), 0U);
    EXPECT_EQ(test.publisher.capture()->mapping.get(0), test.initial->mapping.get(0));
    const auto metrics = test.publisher.work_metrics()->totals();
    EXPECT_EQ(metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)].inflight, 0U);
    EXPECT_EQ(metrics[static_cast<size_t>(TreeStage::PageWritebackPipeline)]
                  .outcomes[static_cast<size_t>(TreeOutcome::Cancelled)],
              1U);
}

TEST(StagedWriteback, FailureAbortsTheCaptureAndKeepsItsInput)
{
    TestStagedWriteback test;
    test.local.store->fail_at = 1;
    ASSERT_TRUE(test.prepare().ok());
    const auto status = detail::wait_snapshot_operation(test.operation);
    EXPECT_EQ(status.code(), Code::kIoError);
    EXPECT_FALSE(test.operation->result().writeback_completed);
    EXPECT_FALSE(test.publisher.capture()->checkpoint_inflight);
    EXPECT_EQ(test.publisher.capture()->mapping.get(0), test.initial->mapping.get(0));
    EXPECT_EQ(test.local.registry->durable_coverage(), 0U);
}

TEST(StagedWriteback, BlockReopenUsesTheAnchorUntilStagedPagesAreCommitted)
{
    tree_test::TempDir              directory("staged_writeback_");
    std::unique_ptr<BlockPageStore> device;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &device).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.frame_bytes                          = 4096;
    config.max_inline_value                     = 64;
    config.page_store                           = device.get();
    config.runtime_binding.limits.io_operations = 1;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "old"}}}).ok());
    const auto initial_checkpoint = core->checkpoint();
    ASSERT_TRUE(initial_checkpoint.ok()) << initial_checkpoint.to_string();
    const std::string value(16000, 'v');
    ASSERT_TRUE(core->apply(2, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = value}}}).ok());
    std::shared_ptr<CheckpointOperation> writeback;
    ASSERT_TRUE(core->prepare_writeback(&writeback).ok());
    const auto staged = detail::wait_snapshot_operation(writeback);
    ASSERT_TRUE(staged.ok()) << staged.to_string();
    EXPECT_EQ(writeback->result().captured_coverage, 2U);
    EXPECT_EQ(writeback->result().durable_coverage, 1U);
    EXPECT_GT(core->capture()->stats.checkpoint_pending_pages, 0U);
    GetView actual;
    ASSERT_TRUE(core->get("a", &actual).ok());
    EXPECT_EQ(actual.value().to_string(), value);
    actual = {};
    writeback.reset();
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->get("a", &actual).ok());
    EXPECT_EQ(actual.value().to_string(), "old");
    ASSERT_TRUE(core->apply(2, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = value}}}).ok());
    ASSERT_TRUE(core->prepare_writeback(&writeback).ok());
    ASSERT_TRUE(detail::wait_snapshot_operation(writeback).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    actual = {};
    writeback.reset();
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->get("a", &actual).ok());
    EXPECT_EQ(actual.value().to_string(), value);
}
} // namespace
} // namespace crowdb::tree
