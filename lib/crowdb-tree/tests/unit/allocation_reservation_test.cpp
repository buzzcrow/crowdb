// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "btree/logical_page.h"
#include "btree/published_state.h"

#include <gtest/gtest.h>

#include <barrier>
#include <thread>

namespace crowdb::tree
{
namespace
{
using detail::AllocationReservation;
using detail::BudgetClass;
using detail::ResourceBudget;
constexpr size_t kMetadata = static_cast<size_t>(BudgetClass::kHandoff);

ResourceBudget::Limits limits(size_t bytes)
{
    ResourceBudget::Limits result{};
    result[kMetadata] = bytes;
    return result;
}

std::shared_ptr<AllocationReservation> reserve(ResourceBudget &budget, size_t bytes)
{
    auto credit = ResourceBudget::try_reserve(budget.make_account(limits(bytes)), BudgetClass::kHandoff, bytes);
    if (!credit.has_value()) {
        ADD_FAILURE() << "test metadata reservation was rejected";
        return {};
    }
    return std::make_shared<AllocationReservation>(std::move(credit.value()));
}

TEST(AllocationReservation, ActualAllocationTransfersCreditAndOutlivesSeal)
{
    ResourceBudget budget(limits(4096), 4);
    auto           reservation = reserve(budget, 4096);
    auto           value       = detail::reserved_shared<std::array<uint64_t, 32>>(reservation);
    const auto     allocated   = budget.snapshot().allocated[kMetadata];
    EXPECT_GE(allocated, sizeof(*value));
    EXPECT_EQ(budget.snapshot().reserved[kMetadata] + allocated, 4096U);
    reservation->seal();
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 0U);
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], allocated);
    EXPECT_THROW(detail::reserved_shared<uint64_t>(reservation), std::bad_alloc);
    reservation.reset();
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], allocated);
    value.reset();
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}

TEST(AllocationReservation, RepeatedChildWindowsReturnUnusedAndReleasedBytesToTheirParent)
{
    ResourceBudget budget(limits(4096), 4);
    auto           parent = reserve(budget, 4096);
    for (size_t step = 0; step < 1000; ++step) {
        auto child = AllocationReservation::create(parent, 1024);
        auto value = detail::reserved_shared<std::array<uint64_t, 16>>(child);
        child->seal();
        value.reset();
        child.reset();
        EXPECT_EQ(parent->available(), 4096U) << step;
        EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U) << step;
        EXPECT_EQ(budget.snapshot().reserved[kMetadata], 4096U) << step;
    }
}

TEST(AllocationReservation, ChildRetainsAcceptedCreditAcrossParentSealAndReturnsEveryLateFree)
{
    ResourceBudget budget(limits(4096), 4);
    auto           parent = reserve(budget, 4096);
    auto           child  = AllocationReservation::create(parent, 1024);
    parent->seal();
    auto value = detail::reserved_shared<std::array<uint64_t, 16>>(child);
    child->seal();
    value.reset();
    child.reset();
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 0U);
}

TEST(AllocationReservation, FailedChildControlAllocationRestoresTheOriginalWindow)
{
    ResourceBudget budget(limits(4096), 4);
    auto           parent = reserve(budget, 4096);
    EXPECT_THROW((void)AllocationReservation::create(parent, 4096), std::bad_alloc);
    EXPECT_EQ(parent->available(), 4096U);
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 4096U);
}

TEST(AllocationReservation, ParentSealRacesChildRefundWithoutLosingOrDuplicatingCredit)
{
    ResourceBudget budget(limits(4096), 4);
    for (size_t pass = 0; pass < 128; ++pass) {
        auto         parent = reserve(budget, 4096);
        auto         child  = AllocationReservation::create(parent, 1024);
        auto         value  = detail::reserved_shared<std::array<uint64_t, 16>>(child);
        std::barrier ready(2);
        std::thread  sealing([parent, &ready] {
            ready.arrive_and_wait();
            parent->seal();
        });
        ready.arrive_and_wait();
        child->seal();
        value.reset();
        child.reset();
        sealing.join();
        parent.reset();
        EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U) << pass;
        EXPECT_EQ(budget.snapshot().reserved[kMetadata], 0U) << pass;
    }
}

TEST(AllocationReservation, BootstrapChargesItsControlBlockUntilLastWeakOwnerReleases)
{
    ResourceBudget budget(limits(4096), 4);
    auto           credit = ResourceBudget::try_reserve(budget.make_account(limits(4096)), BudgetClass::kHandoff, 4096);
    if (!credit.has_value()) {
        ADD_FAILURE() << "test bootstrap credit unavailable";
        return;
    }
    auto       reservation = AllocationReservation::create(std::move(credit.value()));
    const auto bootstrap   = budget.snapshot().allocated[kMetadata];
    EXPECT_GE(bootstrap, sizeof(AllocationReservation));
    EXPECT_EQ(reservation->available() + bootstrap, 4096U);
    std::weak_ptr<AllocationReservation> weak  = reservation;
    auto                                 value = detail::reserved_shared<uint64_t>(reservation, 17);
    reservation->seal();
    reservation.reset();
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(*value, 17U);
    value.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], bootstrap);
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 0U);
    weak.reset();
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}

TEST(AllocationReservation, BootstrapFailureReturnsTheEntireOriginalCredit)
{
    ResourceBudget budget(limits(1), 4);
    auto           credit = ResourceBudget::try_reserve(budget.make_account(limits(1)), BudgetClass::kHandoff, 1);
    if (!credit.has_value()) {
        ADD_FAILURE() << "test bootstrap credit unavailable";
        return;
    }
    EXPECT_THROW((void)AllocationReservation::create(std::move(credit.value())), std::bad_alloc);
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 0U);
}

TEST(AllocationReservation, FailedAllocationReturnsReusableCreditBeforeSeal)
{
    ResourceBudget                         budget(limits(64), 4);
    auto                                   reservation = reserve(budget, 64);
    detail::ReservationAllocator<uint64_t> allocator(reservation);
    EXPECT_THROW((void)allocator.allocate(9), std::bad_alloc);
    auto *memory = allocator.allocate(8);
    EXPECT_EQ(reservation->available(), 0U);
    EXPECT_THROW((void)allocator.allocate(1), std::bad_alloc);
    allocator.deallocate(memory, 8);
    EXPECT_EQ(reservation->available(), 64U);
    memory = allocator.allocate(8);
    allocator.deallocate(memory, 8);
    reservation->seal();
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 0U);
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}

TEST(AllocationReservation, ConcurrentAllocationsAndSealReturnEveryByteOnce)
{
    ResourceBudget            budget(limits(8192), 4);
    auto                      reservation = reserve(budget, 8192);
    std::barrier              ready(5);
    std::vector<std::jthread> workers;
    workers.reserve(4);
    for (size_t i = 0; i < 4; ++i) {
        workers.emplace_back([&] {
            detail::ReservationAllocator<uint64_t> allocator(reservation);
            auto                                  *memory = allocator.allocate(64);
            ready.arrive_and_wait();
            allocator.deallocate(memory, 64);
        });
    }
    ready.arrive_and_wait();
    reservation->seal();
    workers.clear();
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 0U);
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}

TEST(AllocationReservation, DetachedProgressCreditRemainsReservedAcrossParentSeal)
{
    ResourceBudget budget(limits(4096), 4);
    auto           reservation = reserve(budget, 4096);
    auto           progress    = reservation->take_reserved(1024);
    if (!progress) {
        FAIL() << "progress credit was not split";
    }
    EXPECT_EQ(reservation->available(), 3072U);
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 4096U);
    EXPECT_FALSE(reservation->take_reserved(3073).has_value());
    reservation->seal();
    EXPECT_FALSE(reservation->take_reserved(1).has_value());
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 1024U);
    auto child = std::make_shared<AllocationReservation>(std::move(progress.value()));
    auto value = detail::reserved_shared<std::array<char, 512>>(child);
    child->seal();
    EXPECT_EQ(budget.snapshot().reserved[kMetadata], 0U);
    EXPECT_GE(budget.snapshot().allocated[kMetadata], 512U);
    value.reset();
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}

TEST(AllocationReservation, ImmutableRangeCopiesRetainOneChargedBinaryKeyAllocation)
{
    ResourceBudget budget(limits(16384), 4);
    auto           reservation = reserve(budget, 16384);
    std::string    bytes(8192, 'k');
    bytes[31] = '\0';
    PageKeyRange range{.lower = OwnedKey(Slice(bytes), reservation)};
    const auto   allocated = budget.snapshot().allocated[kMetadata];
    EXPECT_GT(allocated, bytes.size());
    reservation->seal();
    PageKeyRange copy(range);
    if (!copy.lower || !range.lower) {
        FAIL() << "range copy lost its lower bound";
    }
    EXPECT_EQ(copy.lower->data(), range.lower->data());
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], allocated);
    range = {};
    EXPECT_EQ(Slice(*copy.lower).to_string(), bytes);
    EXPECT_TRUE(copy.contains(Slice(bytes)));
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], allocated);
    copy = {};
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}

TEST(AllocationReservation, NewOwnedKeyFailsBeforeUnreservedCapacityAndPreservesOldKey)
{
    ResourceBudget budget(limits(1024), 4);
    auto           reservation = reserve(budget, 1024);
    OwnedKey       key(Slice(std::string(256, 'a')), reservation);
    const auto     allocated = budget.snapshot().allocated[kMetadata];
    EXPECT_THROW(key = OwnedKey(Slice(std::string(1024, 'b')), reservation), std::bad_alloc);
    EXPECT_EQ(Slice(key).to_string(), std::string(256, 'a'));
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], allocated);
    OwnedKey moved(std::move(key));
    EXPECT_EQ(moved.size(), 256U);
}

TEST(AllocationReservation, SharedRoutingSeparatorSurvivesIndexOwnerRelease)
{
    auto inner = std::make_shared<InnerPageState>();
    inner->separators.emplace_back(std::string(8192, 'm'));
    const auto                         *address = inner->separators.front().data();
    std::weak_ptr<const InnerPageState> old     = inner;
    std::optional<OwnedKey>             boundary(inner->separators.front());
    inner.reset();
    EXPECT_TRUE(old.expired());
    EXPECT_EQ(boundary->data(), address);
    EXPECT_EQ(Slice(*boundary).to_string(), std::string(8192, 'm'));
    boundary.reset();
    EXPECT_TRUE(old.expired());
    PageKeyRange empty_lower{.lower = OwnedKey("")};
    EXPECT_TRUE(empty_lower.lower.has_value());
    EXPECT_TRUE(empty_lower.contains(""));
    EXPECT_FALSE(PageKeyRange{}.lower.has_value());
}

TEST(AllocationReservation, PrivateHandleControlBlockFailureDestroysTheConstructedObject)
{
    struct Handle
    {
        size_t *destroyed;

        ~Handle()
        {
            ++*destroyed;
        }
    };

    ResourceBudget budget(limits(sizeof(Handle)), 4);
    auto           reservation = reserve(budget, sizeof(Handle));
    size_t         destroyed   = 0;
    EXPECT_THROW((void)detail::reserved_construct<Handle>(
                     reservation,
                     [&](Handle *storage) { ::new (static_cast<void *>(storage)) Handle{.destroyed = &destroyed}; }),
                 std::bad_alloc);
    EXPECT_EQ(destroyed, 1U);
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
    EXPECT_EQ(reservation->available(), sizeof(Handle));
}

TEST(AllocationReservation, PublicationArraysKeepTheirBudgetAfterTransactionAssignment)
{
    ResourceBudget budget(limits(8192), 4);
    auto           reservation = reserve(budget, 8192);
    auto           request     = detail::reserved_shared<PublicationRequest>(reservation, reservation);
    request->transaction       = {.expected_coverage = 0, .metadata = reservation};
    const auto before          = budget.snapshot().allocated[kMetadata];
    request->transaction.pages.resize(16);
    request->transaction.required_installs.resize(32);
    EXPECT_GE(budget.snapshot().allocated[kMetadata] - before,
              (16 * sizeof(PageReplacement)) + (32 * sizeof(std::shared_ptr<InstallRecord>)));
    const auto *original = request->transaction.pages.data();
    EXPECT_THROW(request->transaction.pages.reserve(8192), std::bad_alloc);
    EXPECT_EQ(request->transaction.pages.data(), original);
    reservation->seal();
    EXPECT_GT(budget.snapshot().allocated[kMetadata], before);
    request.reset();
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}

TEST(AllocationReservation, InlineCallbacksRetainCapturedOwnersAfterSealAndReleaseTheLastCopy)
{
    ResourceBudget budget(limits(4096), 4);
    auto           reservation = reserve(budget, 4096);
    auto           value       = detail::reserved_shared<uint64_t>(reservation, 10);
    const auto     allocated   = budget.snapshot().allocated[kMetadata];
    reservation->seal();
    detail::InlineFunction<uint64_t(uint64_t)> callback([value](uint64_t delta) { return *value + delta; });
    auto                                       copy  = callback;
    auto                                       moved = std::move(callback);
    value.reset();
    EXPECT_EQ(copy(3), 13U);
    EXPECT_EQ(moved(4), 14U);
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], allocated);
    copy.reset();
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], allocated);
    moved.reset();
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
    EXPECT_THROW(moved(1), std::bad_function_call);
}

TEST(AllocationReservation, InlineCallbackCopyFailurePreservesThePreviousTarget)
{
    struct ThrowingCopy
    {
        bool *fail;

        explicit ThrowingCopy(bool *flag) : fail(flag)
        {
        }

        ThrowingCopy(ThrowingCopy &&) noexcept = default;

        ThrowingCopy(const ThrowingCopy &other) : fail(other.fail)
        {
            if (*fail) {
                throw std::bad_alloc();
            }
        }

        int operator()(int value) const
        {
            return value + 10;
        }
    };

    bool                             fail = false;
    detail::InlineFunction<int(int)> source{ThrowingCopy(&fail)};
    detail::InlineFunction<int(int)> target([](int value) { return value + 1; });
    fail = true;
    EXPECT_THROW(target = source, std::bad_alloc);
    EXPECT_EQ(target(5), 6);
    EXPECT_EQ(source(5), 15);
    target = std::move(source);
    EXPECT_EQ(target(5), 15);

    struct Oversized
    {
        std::array<char, 256> data;

        void operator()() const
        {
        }
    };

    static_assert(!std::is_constructible_v<detail::InlineFunction<void()>, Oversized>);
}

TEST(AllocationReservation, MappingOldViewRetainsItsChargeAfterReplacement)
{
    ResourceBudget budget(limits(65536), 4);
    auto           reservation = reserve(budget, 65536);
    auto           page        = std::make_shared<PublishedPage>();
    page->leaf                 = std::make_shared<PageState>();
    std::array changes{
        PageReplacement{.page_id = 0, .expected = {}, .replacement = page}
    };
    MappingView::CopyStats copied;
    auto                   before      = MappingView{}.replace(changes, copied, reservation);
    const auto             first_bytes = budget.snapshot().allocated[kMetadata];
    EXPECT_LE(first_bytes, MappingView::path_copy_bound());
    changes[0].expected    = page;
    changes[0].replacement = std::make_shared<PublishedPage>(*page);
    auto after             = before.replace(changes, copied, reservation);
    reservation->seal();
    EXPECT_GT(budget.snapshot().allocated[kMetadata], first_bytes);
    after = {};
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], first_bytes);
    before = {};
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}

TEST(AllocationReservation, PublicationBudgetFailureDoesNotCommitOrDetach)
{
    ResourceBudget         budget(limits(64), 4);
    auto                   reservation = reserve(budget, 64);
    PublicationCoordinator publisher;
    auto                   before = publisher.capture();
    auto                   page   = std::make_shared<PublishedPage>();
    page->leaf                    = std::make_shared<PageState>();
    auto record                   = std::make_shared<InstallRecord>(InstallToken{});
    auto request                  = std::make_shared<PublicationRequest>();
    request->transaction.record   = record;
    request->transaction.pages.push_back({.page_id = 0, .expected = {}, .replacement = page});
    request->transaction.root_page_id = 0;
    request->transaction.metadata     = reservation;
    bool completed                    = false;
    request->on_done                  = [&](PublicationResult result) {
        EXPECT_EQ(result.status.code(), Code::kResourceExhausted);
        completed = true;
    };
    ASSERT_TRUE(publisher.try_submit(request));
    EXPECT_TRUE(completed);
    EXPECT_EQ(publisher.capture(), before);
    EXPECT_FALSE(record->committed.load());
    EXPECT_EQ(reservation->available(), 64U);
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}

TEST(AllocationReservation, OverlayKeepsMetadataChargedAndSharesLargePayload)
{
    ResourceBudget budget(limits(4096), 4);
    auto           reservation = reserve(budget, 4096);
    auto           table       = std::make_shared<MemTable>();
    ASSERT_TRUE(table->upsert("key", 1, encode_cell_buf(1, OpKind::kPut, std::string(16 * 1024, 'x'))));
    std::shared_ptr<const OverlayBatch> overlay;
    const void                         *payload;
    {
        auto guard  = table->read_guard();
        auto cursor = table->prefix_cursor(1);
        auto owner  = cursor.cell_owner();
        payload     = owner->cell.data() + kCellHeaderSize;
        OverlayBuilder builder({.max_entries = 16, .max_key_bytes = 1024, .key_block_bytes = 128}, reservation);
        ASSERT_TRUE(builder.append(cursor.key(), owner).ok());
        ASSERT_TRUE(builder.finish(1, &overlay).ok());
    }
    table.reset();
    reservation->seal();
    EXPECT_GT(budget.snapshot().allocated[kMetadata], 128U);
    EXPECT_LT(budget.snapshot().allocated[kMetadata], 4096U);
    EXPECT_EQ(overlay->entries().front().value().data(), payload);
    EXPECT_EQ(overlay->entries().front().value().size(), 16 * 1024U);
    auto page = std::make_shared<PageState>();
    page->overlays.push_back(overlay);
    auto copy = std::make_shared<PageState>(*page);
    EXPECT_EQ(copy->overlays.front(), overlay);
    overlay.reset();
    page.reset();
    copy.reset();
    EXPECT_EQ(budget.snapshot().allocated[kMetadata], 0U);
}
} // namespace
} // namespace crowdb::tree
