// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_state.h"

#include <gtest/gtest.h>

#include <future>
#include <latch>
#include <thread>

namespace crowdb::tree
{
namespace
{
std::shared_ptr<PublicationRequest> request(uint64_t sequence)
{
    auto value = std::make_shared<PublicationRequest>();
    value->transaction.record =
        std::make_shared<InstallRecord>(InstallToken{.generation = 1, .batch = 1, .range = 0, .sequence = sequence});
    return value;
}

PublicationResult submit(PublicationCoordinator &publisher, const std::shared_ptr<PublicationRequest> &value)
{
    std::promise<PublicationResult> completed;
    auto                            future = completed.get_future();
    value->on_done                         = [&](PublicationResult result) { completed.set_value(std::move(result)); };
    if (!publisher.try_submit(value)) {
        ADD_FAILURE() << "publication rejected";
        return {.status = Status::unavailable()};
    }
    return future.get();
}

TEST(PublicationCheckpointCapacity, CompletionHasIndependentCapacityAndSurvivesClose)
{
    PublicationCoordinator publisher(1);
    auto                   capture         = request(0);
    capture->transaction.expected_coverage = 0;
    capture->transaction.checkpoint        = {.action = CheckpointAction::Capture, .token = 7};
    ASSERT_TRUE(submit(publisher, capture).status.ok());

    std::latch entered{1};
    std::latch release{1};
    auto       blocked = request(1);
    blocked->on_done   = [&](PublicationResult result) {
        EXPECT_TRUE(result.status.ok());
        entered.count_down();
        release.wait();
    };
    std::thread first([&] { EXPECT_TRUE(publisher.try_submit(blocked)); });
    entered.wait();
    auto queued = request(2);
    EXPECT_TRUE(publisher.try_submit(queued));
    EXPECT_FALSE(publisher.try_submit(request(3)));
    publisher.close();

    auto finish                    = request(4);
    finish->transaction.checkpoint = {.action = CheckpointAction::Commit, .token = 7};
    std::promise<PublicationResult> completed;
    auto                            outcome = completed.get_future();
    finish->on_done                         = [&](PublicationResult result) { completed.set_value(std::move(result)); };
    EXPECT_TRUE(publisher.try_submit_checkpoint_completion(finish));
    auto duplicate                    = request(5);
    duplicate->transaction.checkpoint = {.action = CheckpointAction::Abort, .token = 7};
    EXPECT_FALSE(publisher.try_submit_checkpoint_completion(duplicate));
    release.count_down();
    first.join();
    EXPECT_TRUE(outcome.get().status.ok());
    EXPECT_FALSE(publisher.capture()->checkpoint_inflight);
    EXPECT_FALSE(publisher.try_submit_checkpoint_completion(duplicate));
}

TEST(PublicationCheckpointCapacity, CaptureReservesSharedPidAuthorityWithoutLogicalMutation)
{
    PublicationCoordinator publisher;
    const auto             before              = publisher.capture();
    auto                   capture             = request(0);
    capture->transaction.expected_coverage     = 0;
    capture->transaction.expected_commit_seq   = before->commit_seq;
    capture->transaction.checkpoint            = {.action = CheckpointAction::Capture, .token = 1};
    capture->transaction.expected_next_page_id = before->next_page_id;
    capture->transaction.next_page_id          = before->next_page_id + 10;
    const auto outcome                         = submit(publisher, capture);
    ASSERT_TRUE(outcome.status.ok()) << outcome.status.to_string();
    ASSERT_TRUE(outcome.checkpoint_view);
    EXPECT_EQ(publisher.capture()->next_page_id, before->next_page_id + 10);
    EXPECT_EQ(outcome.checkpoint_view->next_page_id, before->next_page_id + 10);
    EXPECT_EQ(publisher.capture()->stats.leaf_pages, before->stats.leaf_pages);
    EXPECT_EQ(publisher.capture()->stats.checkpoint_mapping_pending, before->stats.checkpoint_mapping_pending);
    auto stale                               = request(1);
    stale->transaction.expected_next_page_id = before->next_page_id;
    EXPECT_EQ(submit(publisher, stale).status.code(), Code::kUnavailable);
    auto backwards                               = request(2);
    backwards->transaction.expected_next_page_id = before->next_page_id + 10;
    backwards->transaction.next_page_id          = before->next_page_id;
    EXPECT_EQ(submit(publisher, backwards).status.code(), Code::kInvalidArgument);
    auto finish                    = request(3);
    finish->transaction.checkpoint = {.action = CheckpointAction::Commit, .token = 1};
    ASSERT_TRUE(submit(publisher, finish).status.ok());
    EXPECT_FALSE(publisher.capture()->stats.checkpoint_mapping_pending);
    EXPECT_EQ(publisher.capture()->next_page_id, before->next_page_id + 10);
}
} // namespace
} // namespace crowdb::tree
