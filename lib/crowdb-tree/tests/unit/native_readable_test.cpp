// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "runtime/tree_runtime_binding.h"
#include "snapshot/native_frame_read.h"
#include "snapshot/native_layout.h"
#include "snapshot/operation_wait.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
struct TestNativeReadable
{
    std::shared_ptr<TreeRuntime>          runtime;
    std::shared_ptr<TreeRuntimeBinding>   binding;
    std::shared_ptr<std::vector<uint8_t>> raw     = std::make_shared<std::vector<uint8_t>>(4096);
    std::shared_ptr<BackingRef>           backing = std::make_shared<BackingRef>();
    OverflowBackingPage                   descriptor;
    PageLoadLimits                        limits;
    std::shared_ptr<TreeWorkMetrics>      metrics = std::make_shared<TreeWorkMetrics>();

    TestNativeReadable()
    {
        EXPECT_TRUE(TreeRuntime::create({}, &runtime).ok());
        EXPECT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
        const std::string value(100, 'r');
        overflow_frame_build(raw->data(), raw->size(), 1, kInvalidPageId,
                             reinterpret_cast<const uint8_t *>(value.data()), value.size());
        backing->storage        = BackingStorage::kResidentFrame;
        backing->durability     = BackingDurability::kResident;
        backing->resident_frame = *raw;
        backing->owner          = raw;
        descriptor              = {
            .backing = backing, .next = kInvalidPageId, .bytes = 100, .frame_bytes = 4096, .logical_bytes = 4096};
        limits.max_blob_bytes  = 8192;
        limits.max_frame_bytes = 4096;
    }

    ~TestNativeReadable()
    {
        binding->drain();
    }

    [[nodiscard]] Status create(std::shared_ptr<NativeFrameRead> *out, bool seal_credit = false) const
    {
        const auto bound = NativeFrameRead::allocation_bound(limits);
        if (!bound) {
            return Status::resource_exhausted("native read test bound overflow");
        }
        const auto bytes = *bound + detail::shared_allocation_bound<detail::AllocationReservation>();
        auto credit = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kRouting, bytes);
        if (!credit) {
            return Status::resource_exhausted();
        }
        auto metadata = detail::AllocationReservation::create(std::move(*credit));
        if (seal_credit) {
            metadata->seal();
        }
        return NativeFrameRead::create(1, descriptor, {}, binding->cpu(), binding->lane(), limits,
                                       {.bytes = std::move(metadata), .io = {}, .work_metrics = metrics}, out);
    }
};

TEST(NativeReadable, RawOverflowNeedsNoBackendOrIoCreditAndRetainsExactOwner)
{
    TestNativeReadable               test;
    std::shared_ptr<NativeFrameRead> read;
    auto                             status = test.create(&read);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = detail::wait_snapshot_operation(read);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto &frame = read->result().frame;
    if (!frame.has_value()) {
        FAIL() << "native reader omitted its successful frame";
    }
    auto retained = frame.value();
    EXPECT_EQ(retained.bytes.data(), test.raw->data());
    EXPECT_EQ(retained.durable_addr, kNoAddr);
    EXPECT_FALSE(retained.inherited);
    std::weak_ptr<std::vector<uint8_t>> weak = test.raw;
    test.descriptor                          = {};
    test.backing.reset();
    test.raw.reset();
    read.reset();
    test.binding->drain();
    EXPECT_FALSE(weak.expired());
    EXPECT_TRUE(frame_validate(retained.bytes.data(), retained.bytes.size()));
    EXPECT_EQ(test.runtime->snapshot().reserved.io_operations, 0U);
    EXPECT_EQ(test.runtime->snapshot().allocated.io_operations, 0U);
    retained = {};
    EXPECT_TRUE(weak.expired());
}

TEST(NativeReadable, RawOverflowChecksCrcSelfPidAndPhysicalTopology)
{
    for (unsigned fault = 0; fault != 3; ++fault) {
        TestNativeReadable test;
        if (fault == 0) {
            test.raw->at(100) ^= 0xff;
        }
        if (fault == 1) {
            const std::string value(100, 'r');
            overflow_frame_build(test.raw->data(), test.raw->size(), 2, kInvalidPageId,
                                 reinterpret_cast<const uint8_t *>(value.data()), value.size());
        }
        if (fault == 2) {
            test.descriptor.next = 2;
        }
        std::shared_ptr<NativeFrameRead> read;
        auto                             status = test.create(&read);
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(detail::wait_snapshot_operation(read).code(), Code::kCorruption) << fault;
        EXPECT_FALSE(read->result().frame);
    }
}

TEST(NativeReadable, LayoutAcceptsResidentCatalogWithoutDurableDomainIdentity)
{
    TestNativeReadable test;
    auto               view = std::make_shared<PublishedTreeState>();
    view->next_page_id      = 2;
    auto catalog            = std::make_shared<OverflowBackingSet>(nullptr);
    catalog->pages.emplace(1, test.descriptor);
    const std::vector<ReusedOverflowDependency> references{
        {.head = 1, .bytes = 100, .owner = catalog, .backing = catalog}
    };
    NativeLayoutPlanner planner(view, {}, {}, 7, 9);
    ASSERT_TRUE(planner.accept({.page_id = 0, .summary = {}, .reused_overflow = references, .owner = catalog}).ok());
    while (planner.pending()) {
        auto status = planner.step();
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    ASSERT_TRUE(planner.finish().ok());
    EXPECT_EQ(planner.frame_count(), 2U);
    EXPECT_EQ(planner.new_overflow_frames(), 0U);
}

TEST(NativeReadable, MetricsIncludeValidationAdmissionAndAbandonedRead)
{
    TestNativeReadable               test;
    std::shared_ptr<NativeFrameRead> read;
    test.descriptor.next = 1;
    EXPECT_EQ(test.create(&read).code(), Code::kInvalidArgument);
    test.descriptor.next = kInvalidPageId;
    EXPECT_EQ(test.create(&read, true).code(), Code::kResourceExhausted);
    auto totals = test.metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)];
    EXPECT_EQ(totals.attempts, 2U);
    EXPECT_EQ(totals.inflight, 0U);
    EXPECT_EQ(totals.outcomes[static_cast<size_t>(TreeOutcome::Error)], 2U);
    ASSERT_TRUE(test.create(&read).ok());
    EXPECT_EQ(test.metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].inflight, 1U);
    read.reset();
    totals = test.metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)];
    EXPECT_EQ(totals.count, 3U);
    EXPECT_EQ(totals.inflight, 0U);
    EXPECT_EQ(totals.outcomes[static_cast<size_t>(TreeOutcome::Cancelled)], 1U);
}
} // namespace
} // namespace crowdb::tree
