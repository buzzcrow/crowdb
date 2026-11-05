// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "extent_ledger_test_support.h"

#include <set>
#include <thread>

namespace crowdb::tree
{
using namespace test_read;

TEST(ExtentLedger, RecoveryProtectsBothAnchorOwnersBeforeAllocationAndReaderPinsSurviveRotation)
{
    LedgerFixture test({.backend_id = 7, .file_size = 65536, .address_limit = 65536});
    auto          first  = test.recover(8192, 8192);
    auto          second = test.recover(8192, 8192);
    auto          other  = test.recover(24576, 4096);
    ASSERT_TRUE(first.status.ok());
    ASSERT_TRUE(second.status.ok());
    ASSERT_TRUE(other.status.ok());
    EXPECT_EQ(first.extent.owner, second.extent.owner);
    EXPECT_EQ(test.ledger->stats().protected_bytes, 12288U);
    EXPECT_EQ(test.reserve(4096).status.code(), Code::kUnavailable);
    EXPECT_EQ(test.recover(12288, 8192).status.code(), Code::kCorruption);
    ASSERT_TRUE(test.activate().status.ok());
    auto gap = test.reserve(4096);
    ASSERT_TRUE(gap.status.ok());
    EXPECT_EQ(gap.extent.address, 16384U);
    auto reader = first.extent;
    first       = {};
    second      = {};
    auto later  = test.reserve(8192);
    ASSERT_TRUE(later.status.ok());
    EXPECT_GE(later.extent.address, 28672U);
    const auto old_identity = reader.identity;
    reader                  = {};
    auto reused             = test.reserve(8192);
    ASSERT_TRUE(reused.status.ok());
    EXPECT_EQ(reused.extent.address, 8192U);
    EXPECT_NE(reused.extent.identity, old_identity);
    EXPECT_EQ(reused.extent.backend_id, 7U);
}

TEST(ExtentLedger, LastOwnerReleaseCoalescesBothNeighborsWithoutAllocatingAfterSeal)
{
    LedgerFixture test({.backend_id = 1, .file_size = 24576, .address_limit = 24576});
    ASSERT_TRUE(test.activate().status.ok());
    auto a = test.reserve(4096);
    auto b = test.reserve(4096);
    auto c = test.reserve(4096);
    ASSERT_TRUE(a.status.ok());
    ASSERT_TRUE(b.status.ok());
    ASSERT_TRUE(c.status.ok());
    EXPECT_EQ(test.ledger->stats().protected_ranges, 3U);
    test.credits.seal();
    a = {};
    c = {};
    b = {};
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    EXPECT_EQ(test.ledger->stats().protected_bytes, 0U);
    // The free-space index stays charged until the allocation domain ends.
    EXPECT_GT(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    test.ledger.reset();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(ExtentLedger, RecoveryRejectsPartialOverlapAndCapacityNeverWrapsOrInventsSpace)
{
    LedgerFixture test({.backend_id = 1, .file_size = 16384, .address_limit = 20480});
    auto          recovered = test.recover(8192, 4096);
    ASSERT_TRUE(recovered.status.ok());
    EXPECT_EQ(test.recover(8192, 8192).status.code(), Code::kCorruption);
    EXPECT_EQ(test.recover(1, 4096).status.code(), Code::kInvalidArgument);
    ASSERT_TRUE(test.activate().status.ok());
    auto tail = test.reserve(8192);
    EXPECT_EQ(tail.status.code(), Code::kResourceExhausted);
    auto gap    = test.reserve(4096);
    auto append = test.reserve(4096);
    ASSERT_TRUE(gap.status.ok());
    ASSERT_TRUE(append.status.ok());
    EXPECT_EQ(gap.extent.address, 12288U);
    EXPECT_EQ(append.extent.address, 16384U);
    EXPECT_EQ(test.reserve(4096).status.code(), Code::kResourceExhausted);
    EXPECT_EQ(test.reserve(UINT64_MAX).status.code(), Code::kInvalidArgument);
    EXPECT_EQ(test.recover(12288, 4096).status.code(), Code::kUnavailable);
    auto retained = await_ledger(
        [&](auto done) { return test.ledger->retain(8192, 4096, test.credits.credits.bytes, std::move(done)); });
    EXPECT_TRUE(retained.status.ok());
    EXPECT_EQ(retained.extent.owner, recovered.extent.owner);
}

TEST(ExtentLedger, MetadataBoundPaysForReserveAndAllocationFailuresLeaveIndexesUnchanged)
{
    bool rejected            = false;
    bool failed_after_submit = false;
    bool succeeded           = false;
    for (size_t amount = 64; amount <= ExtentLedger::reservation_metadata_bound(); amount += 64) {
        LoadCredits credits;
        auto        carved = credits.credits.bytes->take_reserved(amount);
        if (!carved.has_value()) {
            FAIL() << "test credit cannot fund reservation";
        }
        auto limited = std::make_shared<detail::AllocationReservation>(std::move(carved.value()));
        std::shared_ptr<ExtentLedger> ledger;
        ASSERT_TRUE(ExtentLedger::create({.backend_id = 1, .file_size = 65536}, {}, &ledger).ok());
        ASSERT_TRUE(await_ledger([&](auto done) { return ledger->activate({}, std::move(done)); }).status.ok());
        bool               called = false;
        ExtentLedgerResult outcome;
        auto               submitted = ledger->reserve(4096, limited, [&](ExtentLedgerResult result) {
            called  = true;
            outcome = std::move(result);
        });
        if (!submitted.ok()) {
            rejected = true;
            EXPECT_FALSE(called);
        }
        else if (!outcome.status.ok()) {
            failed_after_submit = true;
            EXPECT_EQ(ledger->stats().protected_ranges, 0U);
            auto retry = await_ledger([&](auto done) { return ledger->reserve(4096, {}, std::move(done)); });
            ASSERT_TRUE(retry.status.ok());
            EXPECT_EQ(retry.extent.address, 8192U);
        }
        else {
            succeeded = true;
            EXPECT_EQ(outcome.extent.address, 8192U);
        }
        outcome = {};
        ledger.reset();
        limited->seal();
        credits.seal();
        EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    }
    EXPECT_TRUE(rejected);
    EXPECT_TRUE(failed_after_submit);
    EXPECT_TRUE(succeeded);
}

TEST(ExtentLedger, ConcurrentReservationsAndCrossThreadReleasesShareOneReuseDomain)
{
    LedgerFixture test({.backend_id = 1, .address_limit = 2 * 1024 * 1024});
    ASSERT_TRUE(test.activate().status.ok());
    std::vector<ProtectedExtent> extents(200);
    std::vector<std::thread>     workers;
    workers.reserve(4);
    for (size_t worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker] {
            for (size_t i = worker; i < extents.size(); i += 4) {
                auto result = test.reserve(4096);
                EXPECT_TRUE(result.status.ok());
                extents[i] = std::move(result.extent);
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    std::set<uint64_t> addresses;
    std::set<uint64_t> identities;
    for (const auto &extent : extents) {
        addresses.insert(extent.address);
        identities.insert(extent.identity);
    }
    EXPECT_EQ(addresses.size(), extents.size());
    EXPECT_EQ(identities.size(), extents.size());
    EXPECT_EQ(test.ledger->stats().protected_ranges, extents.size());
    workers.clear();
    for (size_t worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker] {
            for (size_t i = worker; i < extents.size(); i += 4) {
                extents[i] = {};
            }
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    auto reused = test.reserve(4096);
    ASSERT_TRUE(reused.status.ok());
    EXPECT_EQ(reused.extent.address, 8192U);
    EXPECT_EQ(test.ledger->stats().protected_ranges, 1U);
}

TEST(ExtentLedger, RangeLimitRejectsSplitsWithoutLosingTheOriginalGap)
{
    LedgerFixture test({.backend_id = 1, .file_size = 24576, .address_limit = 24576, .max_ranges = 2});
    EXPECT_EQ(test.recover(12288, 4096).status.code(), Code::kResourceExhausted);
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    auto first = test.recover(8192, 4096);
    ASSERT_TRUE(first.status.ok());
    ASSERT_TRUE(test.activate().status.ok());
    EXPECT_EQ(test.reserve(4096).status.code(), Code::kResourceExhausted);
    auto rest = test.reserve(12288);
    ASSERT_TRUE(rest.status.ok());
    EXPECT_EQ(rest.extent.address, 12288U);
    first      = {};
    rest       = {};
    auto whole = test.reserve(16384);
    ASSERT_TRUE(whole.status.ok());
    EXPECT_EQ(whole.extent.address, 8192U);
}

TEST(ExtentLedger, MiddleRecoveryFitsItsBoundAndEveryFailedAllocationRollsBack)
{
    const auto bound = ExtentLedger::reservation_metadata_bound();
    for (size_t amount = 64; amount <= bound; amount = std::min(amount + 64, bound)) {
        LoadCredits credits;
        auto        carved = credits.credits.bytes->take_reserved(amount);
        if (!carved.has_value()) {
            FAIL() << "test credit cannot fund recovery";
        }
        auto limited = std::make_shared<detail::AllocationReservation>(std::move(carved.value()));
        std::shared_ptr<ExtentLedger> ledger;
        ASSERT_TRUE(ExtentLedger::create({.backend_id = 1, .file_size = 32768}, {}, &ledger).ok());
        auto result = await_ledger([&](auto done) { return ledger->recover(16384, 4096, limited, std::move(done)); });
        if (amount == bound) {
            ASSERT_TRUE(result.status.ok());
        }
        if (!result.status.ok()) {
            EXPECT_EQ(result.status.code(), Code::kResourceExhausted);
            EXPECT_EQ(ledger->stats().protected_ranges, 0U);
            result = await_ledger([&](auto done) { return ledger->recover(8192, 24576, {}, std::move(done)); });
            ASSERT_TRUE(result.status.ok());
        }
        result = {};
        ledger.reset();
        limited->seal();
        credits.seal();
        EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
        if (amount == bound) {
            break;
        }
    }
}

TEST(ExtentLedger, QueueReentryAndThrowingReceiverDoNotStrandAllocationOrRelease)
{
    LedgerFixture test({.backend_id = 1, .queue_capacity = 1});
    ASSERT_TRUE(test.activate().status.ok());
    size_t          delivered = 0;
    ProtectedExtent retained;
    ASSERT_TRUE(
        test.ledger
            ->reserve(4096, test.credits.credits.bytes,
                      [&](ExtentLedgerResult first) {
                          EXPECT_TRUE(first.status.ok());
                          EXPECT_TRUE(test.ledger
                                          ->reserve(4096, test.credits.credits.bytes,
                                                    [&](ExtentLedgerResult next) {
                                                        EXPECT_TRUE(next.status.ok());
                                                        retained = std::move(next.extent);
                                                        ++delivered;
                                                    })
                                          .ok());
                          EXPECT_EQ(
                              test.ledger->reserve(4096, test.credits.credits.bytes, [](ExtentLedgerResult) {}).code(),
                              Code::kResourceExhausted);
                          throw std::runtime_error("controlled receiver failure");
                      })
            .ok());
    EXPECT_EQ(delivered, 1U);
    EXPECT_EQ(test.ledger->stats().protected_ranges, 1U);
    test.ledger->close();
    test.ledger.reset();
    test.credits.seal();
    retained = {};
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace crowdb::tree
