// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_chunk_stage_backend.h"

#include <array>

namespace crowdb::tree::detail
{
namespace
{
struct TestPackBudget
{
    static constexpr ResourceBudget::Limits  limits{1U << 20, 1U << 20, 1U << 20, 1U << 20, 1U << 20, 16};
    ResourceBudget                           host{limits, 4};
    std::shared_ptr<ResourceBudget::Account> account = host.make_account(limits);

    [[nodiscard]] std::shared_ptr<AllocationReservation> bytes(size_t count = 1U << 20) const
    {
        auto value = ResourceBudget::try_reserve(account, BudgetClass::kHandoff, count);
        if (!value) {
            throw std::bad_alloc();
        }
        return AllocationReservation::create(std::move(*value));
    }
};

constexpr ChunkId      fixed{uint64_t{2} << 56, 1};
constexpr ChunkStageId operation{.owner_epoch = 1, .operation_high = 10, .operation_low = 20};
} // namespace

TEST(ChunkFixedPack, UnknownAllocateRetainsPreexistingStageIdentity)
{
    auto catalog                = std::make_shared<TestStageCatalog>();
    auto transport              = std::make_shared<TestFixedTransport>(catalog);
    transport->unknown_allocate = true;
    ChunkFixedPackStore     store(17, 1, 2, catalog, transport);
    TestPackBudget          budget;
    auto                    credit = budget.bytes();
    std::array<uint8_t, 32> input{};
    ChunkFixedPackRef       output;
    EXPECT_FALSE(store.write(operation, fixed, PagePurpose::kBtreePage, input, credit, &output).ok());
    if (!catalog->record.has_value()) {
        FAIL() << "durable stage missing";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Reserved);
    EXPECT_EQ(catalog->record->chunk_low, fixed.low);
    EXPECT_EQ(transport->allocations, 1U);
    EXPECT_EQ(output.length, 0U);
    ChunkLayout physical;
    EXPECT_TRUE(transport->query_chunk(fixed, &physical).ok());
}

TEST(ChunkFixedPack, UnsupportedReadFenceRetainsAllocationWithoutPublishingBacking)
{
    auto catalog          = std::make_shared<TestStageCatalog>();
    auto transport        = std::make_shared<TestFixedTransport>(catalog);
    transport->fail_fence = true;
    ChunkFixedPackStore     store(17, 1, 2, catalog, transport);
    TestPackBudget          budget;
    auto                    credit = budget.bytes();
    std::array<uint8_t, 32> input{};
    ChunkFixedPackRef       output;
    EXPECT_EQ(store.write(operation, fixed, PagePurpose::kBtreePage, input, credit, &output).code(),
              Code::kNotSupported);
    if (!catalog->record.has_value()) {
        FAIL() << "durable stage missing";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Reserved);
    EXPECT_EQ(transport->allocations, 1U);
    EXPECT_EQ(output.length, 0U);
    ChunkLayout physical;
    EXPECT_TRUE(transport->query_chunk(fixed, &physical).ok());
    EXPECT_EQ(physical.acknowledged_bytes, 0U);
}

TEST(ChunkFixedPack, AdmissionFailurePrecedesStageAndAllocation)
{
    auto                      catalog   = std::make_shared<TestStageCatalog>();
    auto                      transport = std::make_shared<TestFixedTransport>(catalog);
    ChunkFixedPackStore       store(17, 1, 2, catalog, transport);
    TestPackBudget            budget;
    auto                      credit = budget.bytes(1024);
    std::array<uint8_t, 4096> input{};
    ChunkFixedPackRef         output;
    EXPECT_FALSE(store.write(operation, fixed, PagePurpose::kBtreePage, input, credit, &output).ok());
    EXPECT_FALSE(catalog->record);
    EXPECT_EQ(transport->allocations, 0U);
}

TEST(ChunkFixedPack, ReloadsFixedPackWithoutAManifestAndFallsBackFromCorruptMirror)
{
    auto                 catalog   = std::make_shared<TestStageCatalog>();
    auto                 transport = std::make_shared<TestFixedTransport>(catalog);
    ChunkFixedPackStore  store(17, 1, 2, catalog, transport);
    TestPackBudget       budget;
    auto                 credit = budget.bytes();
    std::vector<uint8_t> input(70'000, 87);
    ChunkFixedPackRef    output;
    ASSERT_TRUE(store.write(operation, fixed, PagePurpose::kBtreePage, input, credit, &output).ok());
    if (!catalog->record.has_value()) {
        FAIL() << "durable stage missing";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Reloadable);
    transport->memory.corrupt_mirror(fixed, 0, 20);
    std::shared_ptr<const ChunkPackWindow> loaded;
    ASSERT_TRUE(store.read(output, credit, &loaded).ok());
    EXPECT_EQ(std::vector<uint8_t>(loaded->bytes.begin(), loaded->bytes.end()), input);
    EXPECT_EQ(catalog->manifest_loads, 0U);
    loaded.reset();
    ++output.checksum;
    EXPECT_FALSE(store.read(output, credit, &loaded).ok());
    catalog->record.reset();
    credit->seal();
    credit.reset();
    EXPECT_EQ(budget.host.snapshot().allocated[0], 0U);
}

TEST(ChunkFixedPack, WriteFailureKeepsProvisionalReferencesProtected)
{
    auto catalog          = std::make_shared<TestStageCatalog>();
    auto transport        = std::make_shared<TestFixedTransport>(catalog);
    transport->fail_write = true;
    ChunkFixedPackStore     store(17, 1, 2, catalog, transport);
    TestPackBudget          budget;
    auto                    credit = budget.bytes();
    std::array<uint8_t, 32> input{};
    ChunkFixedPackRef       output;
    EXPECT_FALSE(store.write(operation, fixed, PagePurpose::kBtreePage, input, credit, &output).ok());
    if (!catalog->record.has_value()) {
        FAIL() << "durable stage missing";
        return;
    }
    EXPECT_EQ(catalog->record->state, ChunkStageState::Writing);
    EXPECT_EQ(catalog->record->packs.size(), 1U);
    EXPECT_EQ(output.length, 0U);
}
} // namespace crowdb::tree::detail
