// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "published_read_test_support.h"
#include "runtime_tree_access.h"
#include "snapshot/operation_wait.h"

namespace crowdb::tree
{
namespace
{
struct TestCheckpointContinuation
{
    MemPageStore store{1};

    static Config config(PageStore *store)
    {
        Config value;
        value.page_store       = store;
        value.frame_bytes      = 4096;
        value.leaf_split_bytes = 160;
        return value;
    }

    Crowdbtree                           tree{config(&store)};
    TreeCore                            &core = RuntimeTreeAccess_for_tests::core(tree);
    std::shared_ptr<CheckpointOperation> operation;
    std::shared_ptr<ApplyDispatcher>     progress;

    ~TestCheckpointContinuation()
    {
        operation.reset();
        if (progress) {
            progress->close_and_drain();
        }
    }

    Status large_leaf() const
    {
        auto before                = core.capture();
        auto replacement           = std::make_shared<PublishedPage>();
        replacement->revision      = before->mapping.get(0)->revision + 1;
        replacement->leaf          = test_read::page({
            {.key = "a", .value = std::string(80, 'a')},
            {.key = "b", .value = std::string(80, 'b')},
            {.key = "c", .value = std::string(80, 'c')},
            {.key = "d", .value = std::string(80, 'd')},
            {.key = "e", .value = std::string(80, 'e')},
            {.key = "f", .value = std::string(80, 'f')}
        });
        replacement->metrics.dirty = replacement->metrics.split_candidate = true;
        auto request                                                      = std::make_shared<PublicationRequest>();
        request->transaction.record =
            std::make_shared<InstallRecord>(InstallToken{.generation = before->generation, .sequence = 99});
        request->transaction.pages.push_back(
            {.page_id = 0, .expected = before->mapping.get(0), .replacement = std::move(replacement)});
        std::atomic<bool> done{false};
        Status            result;
        request->on_done = [&](PublicationResult completed) {
            result = std::move(completed.status);
            done.store(true, std::memory_order_release);
            done.notify_all();
        };
        if (!core.publisher().try_submit(request)) {
            return Status::unavailable();
        }
        while (!done.load(std::memory_order_acquire)) {
            done.wait(false, std::memory_order_acquire);
        }
        return result;
    }

    Status prepare(std::function<Status()> before_split, size_t *handoffs)
    {
        using namespace detail;
        auto reserved = ResourceBudget::try_reserve(core.binding()->account(), BudgetClass::kReclaim, 1024 * 1024);
        if (!reserved) {
            return Status::resource_exhausted();
        }
        auto credit          = AllocationReservation::create(std::move(*reserved));
        auto preparation     = reserved_shared<checkpoint_detail::Preparation>(credit);
        preparation->core    = &core;
        preparation->control = credit;
        preparation->options = checkpoint_detail::defaults(core);
        auto finish          = credit->take_reserved(CheckpointOperation::finish_allocation_bound() + 512);
        if (!finish) {
            return Status::resource_exhausted();
        }
        preparation->finish = reserved_shared<AllocationReservation>(credit, std::move(*finish));
        auto status         = core.operation_lifetime().acquire(credit, &preparation->ticket);
        if (!status.ok()) {
            return status;
        }
        status = ApplyDispatcher::create(core.binding()->cpu(), core.binding()->lane(), 8, credit, &progress);
        if (!status.ok()) {
            return status;
        }
        preparation->cpu_progress                    = progress;
        auto &limits                                 = preparation->structure_options;
        limits.publication.pages.primary_page_bytes  = 4096;
        limits.publication.pages.target_page_bytes   = 256;
        limits.publication.pages.merge_trigger_bytes = 64;
        limits.publication.pages.merge_target_bytes  = 128;
        limits.publication.pages.max_work_entries    = 16;
        limits.publication.pages.max_metadata_bytes  = 1024 * 1024;
        limits.publication.max_inner_bytes           = 4096;
        return CheckpointOperation::create(
            preparation, core.publisher(), core.local_backend(), preparation->ticket->backend(core.async_backend()),
            core.binding()->cpu(), core.binding()->lane(), preparation->options,
            {.bytes = credit, .io = {}, .cpu_progress = progress}, preparation->finish, {},
            [preparation, handoffs](std::shared_ptr<FlushOperation> *out) {
                ++*handoffs;
                return preparation->core->prepare_flush(out, preparation->ticket);
            },
            [preparation](const PublishedTreeState &view, const CapturedWritePlan *plan, CheckpointWork *out) {
                return preparation->work(view, plan, out);
            },
            &operation, {}, {}, {}, {}, {},
            [preparation, before_split = std::move(before_split)](std::shared_ptr<const PublishedTreeState> view,
                                                                  uint64_t                                  pid,
                                                                  std::shared_ptr<PageSplitOperation>      *out) {
                auto status = before_split();
                return status.ok() ? preparation->structure(std::move(view), pid, out) : status;
            });
    }
};

TEST(CheckpointContinuation, ConcurrentNewL0IsOutsideTheSingleCapturedFrontier)
{
    TestCheckpointContinuation test;
    ASSERT_TRUE(test.tree.apply(1, Batch{{{.key = "seed", .kind = OpKind::kPut, .value = "v"}}}).ok());
    ASSERT_TRUE(test.tree.flush().ok());
    ASSERT_TRUE(test.large_leaf().ok());
    size_t handoffs = 0;
    size_t windows  = 0;
    ASSERT_TRUE(test.prepare(
                        [&] {
                            ++windows;
                            if (windows == 1) {
                                Batch batch{{{.key = "new", .kind = OpKind::kPut, .value = "later"}}};
                                return test.tree.apply(2, batch);
                            }
                            return Status::Ok();
                        },
                        &handoffs)
                    .ok());
    const auto status = detail::wait_snapshot_operation(test.operation);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(handoffs, 1U);
    EXPECT_GT(windows, 0U);
    EXPECT_EQ(test.operation->result().structural_windows, windows);
    EXPECT_EQ(test.operation->result().durable_coverage, 1U);
    EXPECT_EQ(test.core.capture()->coverage, 1U);
    EXPECT_EQ(test.tree.contiguous_slot(), 2U);
    std::string value;
    uint64_t    slot = 0;
    ASSERT_TRUE(test.tree.get("new", &slot, &value));
    EXPECT_EQ(value, "later");
    EXPECT_EQ(slot, 2U);
    test.operation.reset();
    ASSERT_TRUE(test.tree.flush().ok());
    EXPECT_EQ(test.core.capture()->coverage, 2U);
    ASSERT_TRUE(test.tree.snapshot().ok());
    EXPECT_EQ(test.core.durable_coverage(), 2U);
}

TEST(CheckpointContinuation, FailedStructureAdmissionPreservesThePreviousDurableCheckpoint)
{
    TestCheckpointContinuation test;
    ASSERT_TRUE(test.tree.apply(1, Batch{{{.key = "seed", .kind = OpKind::kPut, .value = "v"}}}).ok());
    ASSERT_TRUE(test.tree.snapshot().ok());
    const auto sequence = test.core.snapshot_sequence();
    ASSERT_TRUE(test.large_leaf().ok());
    size_t handoffs = 0;
    size_t windows  = 0;
    ASSERT_TRUE(test.prepare(
                        [&] {
                            ++windows;
                            return Status::resource_exhausted();
                        },
                        &handoffs)
                    .ok());
    const auto status = detail::wait_snapshot_operation(test.operation);
    EXPECT_EQ(status.code(), Code::kResourceExhausted);
    EXPECT_EQ(handoffs, 1U);
    EXPECT_EQ(windows, 1U);
    EXPECT_FALSE(test.operation->result().durable_committed);
    EXPECT_EQ(test.core.durable_coverage(), 1U);
    EXPECT_EQ(test.core.snapshot_sequence(), sequence);
    EXPECT_FALSE(test.core.capture()->checkpoint_inflight);
    EXPECT_FALSE(test.core.local_backend()->checkpoints->needs_recovery());
}
} // namespace
} // namespace crowdb::tree
