// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "extent_ledger_test_support.h"

namespace crowdb::tree
{
using namespace test_read;

namespace
{
ExtentLedgerResult claim(LedgerFixture &test, uint64_t address, uint64_t length)
{
    return await_ledger([&](auto done) {
        return test.ledger->claim_free(address, length, test.credits.credits.reservation(), std::move(done));
    });
}

TEST(ExtentGcLease, RequiresActivatedFreeRangeAndNeverBorrowsDataOwners)
{
    LedgerFixture test({.backend_id = 1, .file_size = 32768, .address_limit = 32768});
    auto          live = test.recover(16384, 4096);
    ASSERT_TRUE(live.status.ok());
    EXPECT_EQ(claim(test, 8192, 4096).status.code(), Code::kUnavailable);
    ASSERT_TRUE(test.activate().status.ok());
    EXPECT_EQ(claim(test, 16384, 4096).status.code(), Code::kNotFound);
    EXPECT_EQ(claim(test, 12288, 8192).status.code(), Code::kNotFound);
    EXPECT_EQ(claim(test, 8192, 24576).status.code(), Code::kNotFound);
    EXPECT_EQ(claim(test, 32768, 4096).status.code(), Code::kNotFound);
    EXPECT_EQ(test.ledger->stats().address_high_water, 32768U);
    auto leased = claim(test, 8192, 8192);
    ASSERT_TRUE(leased.status.ok());
    EXPECT_EQ(leased.extent.address, 8192U);
    EXPECT_NE(leased.extent.owner, live.extent.owner);
    EXPECT_EQ(claim(test, 8192, 8192).status.code(), Code::kNotFound);
    auto retained = await_ledger([&](auto done) {
        return test.ledger->retain(8192, 4096, test.credits.credits.reservation(), std::move(done));
    });
    EXPECT_EQ(retained.status.code(), Code::kNotFound);
    auto allocated = test.reserve(8192);
    ASSERT_TRUE(allocated.status.ok());
    EXPECT_GE(allocated.extent.address, 20480U);
    leased      = {};
    auto reused = test.reserve(8192);
    ASSERT_TRUE(reused.status.ok());
    EXPECT_EQ(reused.extent.address, 8192U);
}

TEST(ExtentGcLease, OldReaderPreventsDeletionUntilItsFinalOwnerReleases)
{
    LedgerFixture test({.backend_id = 1, .file_size = 16384, .address_limit = 16384});
    auto          anchor = test.recover(8192, 8192);
    ASSERT_TRUE(anchor.status.ok());
    auto reader = anchor.extent.owner;
    ASSERT_TRUE(test.activate().status.ok());
    anchor = {};
    EXPECT_EQ(claim(test, 8192, 8192).status.code(), Code::kNotFound);
    reader.reset();
    auto leased = claim(test, 8192, 8192);
    ASSERT_TRUE(leased.status.ok());
    EXPECT_EQ(test.reserve(4096).status.code(), Code::kResourceExhausted);
    test.credits.seal();
    leased = {};
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    test.ledger.reset();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(ExtentGcLease, ExhaustedMetadataLeavesFreeIndexAndFrontierUnchanged)
{
    LedgerFixture test({.backend_id = 1, .file_size = 32768, .address_limit = 32768});
    ASSERT_TRUE(test.activate().status.ok());
    test.credits.seal();
    auto denied = claim(test, 12288, 8192);
    EXPECT_EQ(denied.status.code(), Code::kResourceExhausted);
    EXPECT_EQ(test.ledger->stats().protected_ranges, 0U);
    EXPECT_EQ(test.ledger->stats().address_high_water, 32768U);
    auto leased = await_ledger([&](auto done) { return test.ledger->claim_free(8192, 24576, {}, std::move(done)); });
    ASSERT_TRUE(leased.status.ok());
    EXPECT_EQ(leased.extent.address, 8192U);
    EXPECT_EQ(leased.extent.length, 24576U);
}
} // namespace
} // namespace crowdb::tree
