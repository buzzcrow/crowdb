// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "extent_ledger_test_support.h"

namespace crowdb::tree
{
using namespace test_read;

namespace
{
ExtentLedgerResult exclude(LedgerFixture &test, uint64_t address, uint64_t length)
{
    return await_ledger([&](auto done) {
        return test.ledger->exclude_allocations(address, length, test.credits.credits.reservation(), std::move(done));
    });
}

TEST(ExtentExclusion, ExistingReadersRemainProtectedAndNewWritesAvoidTheWholeInterval)
{
    LedgerFixture test({.backend_id = 1, .file_size = 65536, .address_limit = 65536});
    auto          data = test.recover(16384, 4096);
    ASSERT_TRUE(data.status.ok());
    ASSERT_TRUE(test.activate().status.ok());
    auto frozen = exclude(test, 8192, 24576);
    ASSERT_TRUE(frozen.status.ok());
    EXPECT_EQ(frozen.occupied_bytes, 4096U);
    EXPECT_EQ(test.ledger->stats().protected_bytes, 4096U);
    auto retained = await_ledger([&](auto done) {
        return test.ledger->retain(16384, 4096, test.credits.credits.reservation(), std::move(done));
    });
    ASSERT_TRUE(retained.status.ok());
    EXPECT_EQ(retained.extent.owner, data.extent.owner);
    EXPECT_EQ(exclude(test, 32768, 8192).status.code(), Code::kUnavailable);
    auto outside = test.reserve(4096);
    ASSERT_TRUE(outside.status.ok());
    EXPECT_GE(outside.extent.address, 32768U);
    data               = {};
    retained           = {};
    auto after_release = test.reserve(4096);
    ASSERT_TRUE(after_release.status.ok());
    EXPECT_GE(after_release.extent.address, 32768U);
    frozen      = {};
    auto reused = test.reserve(4096);
    ASSERT_TRUE(reused.status.ok());
    EXPECT_EQ(reused.extent.address, 8192U);
}

TEST(ExtentExclusion, FreeDeletionLeaseIsAllowedWhileFutureAllocationStaysFenced)
{
    LedgerFixture test({.backend_id = 1, .file_size = 32768, .address_limit = 32768});
    ASSERT_TRUE(test.activate().status.ok());
    auto frozen = exclude(test, 8192, 24576);
    ASSERT_TRUE(frozen.status.ok());
    EXPECT_EQ(frozen.occupied_bytes, 0U);
    EXPECT_EQ(test.reserve(4096).status.code(), Code::kResourceExhausted);
    auto deletion = await_ledger([&](auto done) {
        return test.ledger->claim_free(8192, 24576, test.credits.credits.reservation(), std::move(done));
    });
    ASSERT_TRUE(deletion.status.ok());
    deletion = {};
    EXPECT_EQ(test.reserve(4096).status.code(), Code::kResourceExhausted);
    test.credits.seal();
    frozen = {};
    test.ledger.reset();
    EXPECT_EQ(test.credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(ExtentExclusion, GeometryAndCreditFailureLeaveTheAllocationIndexesUnchanged)
{
    LedgerFixture test({.backend_id = 1, .file_size = 32768, .address_limit = 32768});
    EXPECT_EQ(exclude(test, 8192, 4096).status.code(), Code::kUnavailable);
    ASSERT_TRUE(test.activate().status.ok());
    EXPECT_EQ(exclude(test, 0, 4096).status.code(), Code::kInvalidArgument);
    EXPECT_EQ(exclude(test, 32768, 4096).status.code(), Code::kInvalidArgument);
    EXPECT_EQ(exclude(test, 8192, 1).status.code(), Code::kInvalidArgument);
    test.credits.seal();
    EXPECT_EQ(exclude(test, 8192, 4096).status.code(), Code::kResourceExhausted);
    auto allocated = await_ledger([&](auto done) { return test.ledger->reserve(4096, {}, std::move(done)); });
    ASSERT_TRUE(allocated.status.ok());
    EXPECT_EQ(allocated.extent.address, 8192U);
    EXPECT_EQ(test.ledger->stats().address_high_water, 32768U);
}
} // namespace
} // namespace crowdb::tree
