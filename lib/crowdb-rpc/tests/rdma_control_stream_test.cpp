// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_control_stream.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <thread>

using namespace crowdb::rpc;
using namespace std::chrono_literals;

namespace
{
ControlFragment test_fragment(uint64_t total, uint32_t capacity, uint64_t offset = 0, uint64_t stream_id = 1)
{
    return {.request_id           = 43,
            .rpc_create_nano      = 11,
            .response_create_nano = 12,
            .stream_id            = stream_id,
            .generation           = 7,
            .sequence             = offset / capacity,
            .offset               = offset,
            .total                = total,
            .capacity             = capacity,
            .message_type         = 3701,
            .stream_flags         = static_cast<uint8_t>((offset == 0 ? CONTROL_STREAM_START : 0) |
                                                         (total - offset <= capacity ? CONTROL_STREAM_END : 0))};
}

TEST(ControlStreamTest, ClassesReassembleExactBytesAndRetainAdmissionThroughFinalOwner)
{
    for (const uint32_t capacity : {RDMA_METADATA_CAPACITY, RDMA_AGGREGATE_CAPACITY, RDMA_DATAPATH_CAPACITY}) {
        ControlStreamBudget   budget(4 << 20);
        ControlStreamReceiver receiver(budget, capacity, 7, 4 << 20, 10ms);
        std::vector<uint8_t>  original(2 * capacity + 13);
        for (size_t index = 0; index < original.size(); ++index)
            original[index] = static_cast<uint8_t>(index * 131U);
        std::unique_ptr<Frame> completed;
        for (size_t offset = 0; offset < original.size(); offset += capacity) {
            const auto part = std::span(original).subspan(offset, std::min<size_t>(capacity, original.size() - offset));
            const auto expected =
                offset + part.size() == original.size() ? ControlStreamResult::Complete : ControlStreamResult::Accepted;
            EXPECT_EQ(receiver.accept(test_fragment(original.size(), capacity, offset), part, {}, completed), expected);
            EXPECT_EQ(budget.used(), original.size());
        }
        ASSERT_NE(completed, nullptr);
        EXPECT_EQ(completed->control, original);
        EXPECT_EQ(completed->request_id, 43U);
        EXPECT_EQ(completed->header.msg_type, 3701U);
        EXPECT_EQ(completed->header.msg_size, original.size());
        EXPECT_EQ(completed->header.data_size, 0U);
        EXPECT_EQ(completed->rpc_create_nano, 11U);
        EXPECT_EQ(completed->response_create_nano, 12U);
        EXPECT_FALSE(receiver.active());
        auto retained = completed->control_owner;
        completed.reset();
        EXPECT_EQ(budget.used(), original.size());
        retained.reset();
        EXPECT_EQ(budget.used(), 0U);
        EXPECT_LE(budget.high_water(), budget.limit());
    }
}

TEST(ControlStreamTest, HeldCompletedControlBlocksTheNextMessageAndReleaseWakesAdmission)
{
    std::atomic<uint64_t>   wakes{0};
    ControlStreamBudget     budget(32, [&] { wakes.fetch_add(1); });
    ControlStreamReceiver   receiver(budget, RDMA_METADATA_CAPACITY, 7, 32, 10ms);
    std::array<uint8_t, 32> bytes{};
    std::unique_ptr<Frame>  first;
    ASSERT_EQ(receiver.accept(test_fragment(bytes.size(), RDMA_METADATA_CAPACITY), bytes, {}, first),
              ControlStreamResult::Complete);
    std::unique_ptr<Frame> second;
    const auto             fragment = test_fragment(bytes.size(), RDMA_METADATA_CAPACITY, 0, 2);
    EXPECT_EQ(receiver.accept(fragment, bytes, {}, second), ControlStreamResult::Backpressure);
    EXPECT_EQ(second, nullptr);
    EXPECT_FALSE(receiver.active());
    EXPECT_EQ(budget.used(), bytes.size());
    EXPECT_EQ(wakes.load(), 0U);
    first.reset();
    EXPECT_EQ(wakes.load(), 1U);
    EXPECT_EQ(receiver.accept(fragment, bytes, {}, second), ControlStreamResult::Complete);
    second.reset();
    EXPECT_EQ(budget.used(), 0U);
    EXPECT_EQ(wakes.load(), 2U);
}

TEST(ControlStreamTest, MetadataDisagreementAndGapsNeverPublishPartialControl)
{
    using Change = std::function<void(ControlFragment &)>;
    const std::array<Change, 11> changes{[](auto &f) { ++f.request_id; },
                                         [](auto &f) { ++f.rpc_create_nano; },
                                         [](auto &f) { ++f.response_create_nano; },
                                         [](auto &f) { ++f.stream_id; },
                                         [](auto &f) { ++f.generation; },
                                         [](auto &f) { ++f.sequence; },
                                         [](auto &f) { --f.offset; },
                                         [](auto &f) { ++f.total; },
                                         [](auto &f) { f.capacity = RDMA_AGGREGATE_CAPACITY; },
                                         [](auto &f) { ++f.message_type; },
                                         [](auto &f) { f.message_flags = FLAG_ONE_WAY; }};
    const uint32_t               capacity = RDMA_METADATA_CAPACITY;
    std::vector<uint8_t>         bytes(capacity);
    for (const auto &change : changes) {
        ControlStreamBudget    budget(2 * capacity);
        ControlStreamReceiver  receiver(budget, capacity, 7, 2 * capacity, 10ms);
        std::unique_ptr<Frame> completed;
        ASSERT_EQ(receiver.accept(test_fragment(2 * capacity, capacity), bytes, {}, completed),
                  ControlStreamResult::Accepted);
        auto invalid = test_fragment(2 * capacity, capacity, capacity);
        change(invalid);
        EXPECT_EQ(receiver.accept(invalid, bytes, {}, completed), ControlStreamResult::Invalid);
        EXPECT_EQ(completed, nullptr);
        EXPECT_EQ(budget.used(), 2 * capacity);
        receiver.reset();
        EXPECT_EQ(budget.used(), 0U);
    }
}

TEST(ControlStreamTest, DuplicateCompletedStreamAndDuplicateStartAreRejected)
{
    const uint32_t         capacity = RDMA_METADATA_CAPACITY;
    ControlStreamBudget    budget(2 * capacity);
    ControlStreamReceiver  receiver(budget, capacity, 7, 2 * capacity, 10ms);
    std::vector<uint8_t>   bytes(capacity);
    std::unique_ptr<Frame> completed;
    const auto             first = test_fragment(2 * capacity, capacity);
    ASSERT_EQ(receiver.accept(first, bytes, {}, completed), ControlStreamResult::Accepted);
    EXPECT_EQ(receiver.accept(first, bytes, {}, completed), ControlStreamResult::Invalid);
    ASSERT_EQ(receiver.accept(test_fragment(2 * capacity, capacity, capacity), bytes, {}, completed),
              ControlStreamResult::Complete);
    completed.reset();
    EXPECT_EQ(receiver.accept(first, bytes, {}, completed), ControlStreamResult::Invalid);
    EXPECT_EQ(budget.used(), 0U);
}

TEST(ControlStreamTest, InvalidBoundsFlagsAndLengthsRejectBeforeByteAdmission)
{
    const uint32_t                                              capacity = RDMA_METADATA_CAPACITY;
    ControlStreamBudget                                         budget(2 * capacity);
    ControlStreamReceiver                                       receiver(budget, capacity, 7, 2 * capacity, 10ms);
    const auto                                                  first = test_fragment(2 * capacity, capacity);
    const std::array<std::function<void(ControlFragment &)>, 9> changes{
        [](auto &f) { f.request_id = 0; },
        [](auto &f) { f.stream_id = 0; },
        [](auto &f) { f.total = UINT64_MAX; },
        [](auto &f) { f.offset = UINT64_MAX; },
        [](auto &f) { f.stream_flags |= CONTROL_STREAM_END; },
        [](auto &f) { f.stream_flags |= 8; },
        [](auto &f) { f.message_flags = 0x80; },
        [](auto &f) { f.message_type = RPC_CONTROL_FRAGMENT_MSG_TYPE; },
        [](auto &f) { f.capacity = 128 << 10; }};
    std::vector<uint8_t>   bytes(capacity);
    std::unique_ptr<Frame> completed;
    for (const auto &change : changes) {
        auto invalid = first;
        change(invalid);
        EXPECT_EQ(receiver.accept(invalid, bytes, {}, completed), ControlStreamResult::Invalid);
        EXPECT_EQ(budget.used(), 0U);
    }
    EXPECT_EQ(receiver.accept(first, std::span(bytes).first(capacity - 1), {}, completed),
              ControlStreamResult::Invalid);
    EXPECT_EQ(receiver.accept(first, {}, {}, completed), ControlStreamResult::Invalid);
    EXPECT_EQ(budget.used(), 0U);
}

TEST(ControlStreamTest, ErrorAndPartialExpiryReturnAdmissionWithoutPublishingBytes)
{
    const uint32_t         capacity = RDMA_METADATA_CAPACITY;
    ControlStreamBudget    budget(2 * capacity);
    ControlStreamReceiver  receiver(budget, capacity, 7, 2 * capacity, 10ms);
    std::vector<uint8_t>   bytes(capacity);
    std::unique_ptr<Frame> completed;
    const auto             first = test_fragment(2 * capacity, capacity);
    ASSERT_EQ(receiver.accept(first, bytes, {}, completed), ControlStreamResult::Accepted);
    auto failure         = test_fragment(2 * capacity, capacity, capacity);
    failure.stream_flags = CONTROL_STREAM_END | CONTROL_STREAM_ERROR;
    EXPECT_EQ(receiver.accept(failure, {}, {}, completed), ControlStreamResult::Aborted);
    EXPECT_EQ(budget.used(), 0U);
    ASSERT_EQ(receiver.accept(test_fragment(2 * capacity, capacity, 0, 2), bytes, {}, completed),
              ControlStreamResult::Accepted);
    EXPECT_FALSE(receiver.expired(ControlStreamReceiver::Clock::time_point{} + 9ms));
    EXPECT_TRUE(receiver.expired(ControlStreamReceiver::Clock::time_point{} + 10ms));
    EXPECT_EQ(receiver.accept(test_fragment(2 * capacity, capacity, capacity, 2), bytes,
                              ControlStreamReceiver::Clock::time_point{} + 10ms, completed),
              ControlStreamResult::Invalid);
    EXPECT_EQ(completed, nullptr);
    receiver.reset();
    EXPECT_EQ(budget.used(), 0U);
}

TEST(ControlStreamTest, CompletedOwnerOutlivesReceiverAndBudgetFacades)
{
    auto                   wakes = std::make_shared<std::atomic<uint64_t>>(0);
    std::unique_ptr<Frame> completed;
    {
        ControlStreamBudget     budget(32, [wakes] { wakes->fetch_add(1); });
        ControlStreamReceiver   receiver(budget, RDMA_METADATA_CAPACITY, 7, 32, 10ms);
        std::array<uint8_t, 32> bytes{};
        ASSERT_EQ(receiver.accept(test_fragment(bytes.size(), RDMA_METADATA_CAPACITY), bytes, {}, completed),
                  ControlStreamResult::Complete);
    }
    EXPECT_EQ(wakes->load(), 0U);
    completed.reset();
    EXPECT_EQ(wakes->load(), 1U);
}

TEST(ControlStreamTest, ConcurrentByteReservationsNeverExceedTheHardBound)
{
    ControlStreamBudget        budget(4096);
    std::array<std::thread, 8> workers;
    std::atomic<bool>          valid{true};
    for (auto &worker : workers)
        worker = std::thread([&] {
            for (uint64_t index = 0; index < 2000; ++index) {
                auto lease = budget.try_reserve(1 + index % 128);
                if (budget.used() > budget.limit())
                    valid.store(false);
                if (lease == nullptr)
                    std::this_thread::yield();
            }
        });
    for (auto &worker : workers)
        worker.join();
    EXPECT_TRUE(valid.load());
    EXPECT_EQ(budget.used(), 0U);
    EXPECT_LE(budget.high_water(), budget.limit());
    EXPECT_EQ(budget.try_reserve(UINT64_MAX), nullptr);
    EXPECT_EQ(budget.try_reserve(0), nullptr);
}
} // namespace
