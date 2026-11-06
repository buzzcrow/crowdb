// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "local_checkpoint_test_support.h"
#include "snapshot/checkpoint_operation.h"

namespace crowdb::tree
{
namespace
{
using namespace test_write;

struct TestHandoffCheckpoint
{
    TestCheckpoint                            local;
    SourceAdmission                           sources{10};
    std::shared_ptr<LocalBackendState>        backend = std::make_shared<LocalBackendState>();
    std::shared_ptr<const PublishedTreeState> initial = [this] {
        auto state = captured_leaf(page({
            {.key = "a", .value = "old"}
        }));
        EXPECT_TRUE(apply_page_metrics(state->stats, nullptr, &state->mapping.get(0)->metrics));
        auto catalog  = sources.catalog();
        auto inputs   = std::make_shared<PublishedSources>();
        inputs->floor = catalog->floor;
        inputs->tables.assign(catalog->tables.begin(), catalog->tables.end());
        state->sources = std::move(inputs);
        return state;
    }();
    PublicationCoordinator publisher{
        4,  1,
        {},
          std::pmr::get_default_resource(),
        {},
          {.metadata = {}, .initial = initial, .source_catalog = sources.catalog()}
    };
    std::atomic<uint64_t>                          contiguous{11};
    std::shared_ptr<detail::AllocationReservation> handoff_credit;
    std::shared_ptr<detail::AllocationReservation> finish_credit;
    std::shared_ptr<FlushOperation>                handoff;
    std::shared_ptr<CheckpointOperation>           operation;

    TestHandoffCheckpoint()
    {
        backend->store       = local.store;
        backend->ledger      = local.source.ledger;
        backend->checkpoints = local.registry;
        handoff_credit       = partition(1024 * 1024);
        finish_credit        = partition(CheckpointOperation::finish_allocation_bound() + 512);
    }

    std::shared_ptr<detail::AllocationReservation> partition(size_t bytes)
    {
        auto credit = local.source.credits.credits.reservation()->take_reserved(bytes);
        if (!credit) {
            ADD_FAILURE() << "test credit unavailable";
            return {};
        }
        return detail::AllocationReservation::create(std::move(credit.value()));
    }

    Status prepare()
    {
        CheckpointOptions options;
        options.write.frames.encode.frame_bytes        = 4096;
        options.write.frames.encode.inline_value_bytes = 16;
        options.write.write.max_frame_bytes            = 4096;
        return CheckpointOperation::create(
            {}, publisher, backend, local.store, local.source.runtime, local.source.lane, options,
            local.source.credits.credits, finish_credit, std::make_shared<test_read::TestLoader>(),
            [this](std::shared_ptr<FlushOperation> *out) {
                *out = handoff;
                return Status::Ok();
            },
            {}, &operation);
    }

    void expect_no_durable_work(Code expected)
    {
        Completion done;
        auto       connected = stdexec::connect(operation->sender(), Receiver{.done = &done});
        stdexec::start(connected);
        local.source.runtime->drain();
        EXPECT_EQ(done.errors.load(), 1U);
        EXPECT_EQ(done.values.load(), 0U);
        EXPECT_EQ(done.status.code(), expected);
        EXPECT_FALSE(operation->result().durable_committed);
        EXPECT_EQ(local.registry->durable_coverage(), 0U);
        EXPECT_EQ(local.registry->snapshot_sequence(), 0U);
        EXPECT_FALSE(publisher.capture()->checkpoint_inflight);
        EXPECT_TRUE(local.store->events.empty());
        EXPECT_EQ(local.source.store->writes, 0U);
    }
};

TEST(CheckpointHandoffReport, FailedAdmissionNeverCommitsTheOlderCoverage)
{
    TestHandoffCheckpoint test;
    FlushController       controller(test.publisher, *test.local.source.runtime, test.sources, test.contiguous,
                                     test.local.source.lane);
    const auto            before = test.publisher.capture();
    ASSERT_TRUE(controller.prepare(test.handoff_credit, &test.handoff, before->commit_seq + 1).ok());
    ASSERT_TRUE(test.prepare().ok());
    test.expect_no_durable_work(Code::kUnavailable);
    EXPECT_EQ(test.publisher.capture(), before);
    EXPECT_EQ(test.publisher.capture()->coverage, 10U);
    EXPECT_FALSE(test.sources.catalog()->active->closed());
}

TEST(CheckpointHandoffReport, MaintenanceFailureKeepsHandoffProgressButDoesNotWriteAnAnchor)
{
    TestHandoffCheckpoint   test;
    SourceAdmission::Writer writer;
    ASSERT_TRUE(test.sources.try_admit(10, &writer).ok());
    ASSERT_TRUE(writer.table()->upsert("b", 11, encode_cell_buf(11, OpKind::kPut, "new")));
    writer.release();
    FlushController controller(test.publisher, *test.local.source.runtime, test.sources, test.contiguous,
                               test.local.source.lane, {},
                               [](auto, auto complete) { complete(Status::io_error(), 7); });
    ASSERT_TRUE(controller.prepare(test.handoff_credit, &test.handoff).ok());
    ASSERT_TRUE(test.prepare().ok());
    test.expect_no_durable_work(Code::kIoError);
    EXPECT_EQ(test.publisher.capture()->coverage, 11U);
    EXPECT_EQ(test.sources.catalog()->floor, 11U);
}

struct TestWorkerGate
{
    std::atomic<bool> entered{false}, released{false};

    ~TestWorkerGate()
    {
        release();
    }

    void release()
    {
        released.store(true);
        released.notify_all();
    }

    void wait() const
    {
        while (!entered.load()) {
            entered.wait(false);
        }
    }

    static void run(void *context) noexcept
    {
        auto &self = *static_cast<TestWorkerGate *>(context);
        self.entered.store(true);
        self.entered.notify_all();
        while (!self.released.load()) {
            self.released.wait(false);
        }
    }
};

TEST(CheckpointHandoffReport, ReconnectCannotChangeTheDeliveredFrontier)
{
    TestHandoffCheckpoint test;
    auto                  handoff_runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    TestWorkerGate        handoff_gate;
    TestWorkerGate        checkpoint_gate;
    FlushController controller(test.publisher, *handoff_runtime, test.sources, test.contiguous, test.local.source.lane);
    SourceAdmission::Writer writer;
    ASSERT_TRUE(test.sources.try_admit(10, &writer).ok());
    ASSERT_TRUE(writer.upsert("b", 11, encode_cell_buf(11, OpKind::kPut, "eleven")).ok());
    writer.release();
    ASSERT_TRUE(controller.prepare(test.handoff_credit, &test.handoff).ok());
    ASSERT_TRUE(test.prepare().ok());
    ASSERT_TRUE(handoff_runtime->try_submit(test.local.source.lane, &TestWorkerGate::run, &handoff_gate).ok());
    handoff_gate.wait();
    Completion done;
    auto       connected = stdexec::connect(test.operation->sender(), Receiver{.done = &done});
    stdexec::start(connected);
    test.local.source.runtime->drain();
    ASSERT_TRUE(
        test.local.source.runtime->try_submit(test.local.source.lane, &TestWorkerGate::run, &checkpoint_gate).ok());
    checkpoint_gate.wait();
    handoff_gate.release();
    handoff_runtime->drain();
    EXPECT_EQ(test.handoff->result().frontier, 11U);

    // Keep the checkpoint continuation queued while a later flush and a split
    // capture reconnect change the shared handoff object's mutable report.
    ASSERT_TRUE(test.sources.try_admit(11, &writer).ok());
    ASSERT_TRUE(writer.upsert("c", 12, encode_cell_buf(12, OpKind::kPut, "twelve")).ok());
    writer.release();
    test.contiguous.store(12);
    std::shared_ptr<FlushOperation> next;
    ASSERT_TRUE(controller.prepare(test.partition(1024 * 1024), &next).ok());
    ASSERT_TRUE(next->wait().ok());
    FlushResult recaptured;
    ASSERT_TRUE(test.handoff->wait_capture(&recaptured).ok());
    EXPECT_EQ(recaptured.frontier, 12U);
    checkpoint_gate.release();
    test.local.source.runtime->drain();
    EXPECT_EQ(done.status.code(), Code::kUnavailable);
    EXPECT_EQ(done.errors.load(), 1U);
    EXPECT_FALSE(test.operation->result().durable_committed);
    EXPECT_EQ(test.local.registry->durable_coverage(), 0U);
    EXPECT_TRUE(test.local.store->events.empty());
}
} // namespace
} // namespace crowdb::tree
