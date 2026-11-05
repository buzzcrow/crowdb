// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "btree/page_cache.h"
#include "btree/tree_work_registry.h"
#include "captured_write_test_support.h"
#include "common/test_chunk_physical.h"
#include "runtime/allocation_memory_resource.h"
#include "runtime/tree_runtime_binding.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <sstream>

namespace crowdb::tree
{
namespace
{
uint64_t quantity(const std::shared_ptr<TreeWorkMetrics> &work, TreeWorkCounter counter,
                  TreeWorkCause cause = TreeWorkCause::Other)
{
    return work->counters.snapshot()[static_cast<size_t>(cause)][static_cast<size_t>(counter)];
}

struct TestBinding
{
    std::shared_ptr<TreeRuntime>        runtime;
    std::shared_ptr<TreeRuntimeBinding> binding;

    TestBinding()
    {
        EXPECT_TRUE(TreeRuntime::create({}, &runtime).ok());
        EXPECT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
    }

    ~TestBinding()
    {
        binding->drain();
    }
};

std::string rendered(common::metrics::MetricsRegistry &registry)
{
    std::unique_ptr<FILE, decltype(&std::fclose)> file(std::tmpfile(), &std::fclose);
    EXPECT_NE(file, nullptr);
    if (!file) {
        return {};
    }
    registry.flush_to(file.get(), 1, "work");
    EXPECT_EQ(std::fseek(file.get(), 0, SEEK_SET), 0);
    std::array<char, 2048> buffer{};
    std::string            result;
    while (const auto bytes = std::fread(buffer.data(), 1, buffer.size(), file.get())) {
        result.append(buffer.data(), bytes);
    }
    return result;
}

uint64_t metric_value(const std::string &text, const std::string &name)
{
    std::istringstream lines(text);
    std::string        line;
    while (std::getline(lines, line)) {
        std::istringstream fields(line);
        std::string        key;
        uint64_t           value = 0;
        if (fields >> key >> value; key == name) {
            return value;
        }
    }
    ADD_FAILURE() << "missing metric " << name;
    return UINT64_MAX;
}

TEST(TreeWorkCounters, ActualFreeIsCountedOnceAndSameHostTransferDoesNotCount)
{
    TestBinding                         test;
    std::shared_ptr<TreeRuntimeBinding> next;
    ASSERT_TRUE(TreeRuntimeBinding::create(test.runtime, {}, &next).ok());
    std::shared_ptr<detail::AllocationAccount> owner;
    ASSERT_TRUE(detail::AllocationAccount::create(test.binding->account(), detail::BudgetClass::kRouting, &owner).ok());
    BudgetAllocator<uint8_t, AllocationBudget, AllocationKind::kPayload> allocator(owner);
    auto                                                                *bytes    = allocator.allocate(137);
    auto                                                                 old_work = test.binding->work_metrics();
    auto                                                                 new_work = next->work_metrics();
    const auto old_before = quantity(old_work, TreeWorkCounter::MemoryReclaimedBytes, TreeWorkCause::Allocation);
    const auto new_before = quantity(new_work, TreeWorkCounter::MemoryReclaimedBytes, TreeWorkCause::Allocation);
    ASSERT_TRUE(owner->rebind(next->account()).ok());
    EXPECT_EQ(quantity(old_work, TreeWorkCounter::MemoryReclaimedBytes, TreeWorkCause::Allocation), old_before);
    EXPECT_EQ(quantity(new_work, TreeWorkCounter::MemoryReclaimedBytes, TreeWorkCause::Allocation), new_before);
    allocator.deallocate(bytes, 137);
    EXPECT_EQ(quantity(old_work, TreeWorkCounter::MemoryReclaimedBytes, TreeWorkCause::Allocation), old_before);
    EXPECT_EQ(quantity(new_work, TreeWorkCounter::MemoryReclaimedBytes, TreeWorkCause::Allocation), new_before + 137);
    next->drain();
}

TEST(TreeWorkCounters, EncoderCountsOnlyEmittedFramesAndActualCopiesAcrossFailuresAndCompletion)
{
    auto work     = std::make_shared<TreeWorkMetrics>();
    auto capture  = test_write::captured_leaf(test_read::page({
        {.key = "key", .slot = 3, .value = std::string(1000, 'v'), .tombstone = false}
    }));
    auto prepared = prepare_final_page(
        capture, 0, {},
        {.frame_bytes = 256, .inline_value_bytes = 16, .work_metrics = work, .cause = TreeWorkCause::Checkpoint});
    ASSERT_TRUE(prepared.status.ok()) << prepared.status.to_string();
    ASSERT_TRUE(prepared.encoder);
    const auto               frames = prepared.encoder->summary().new_overflow_frames;
    std::array<uint8_t, 256> frame{};
    EXPECT_EQ(prepared.encoder->next(frame).status.code(), Code::kUnavailable);
    EXPECT_EQ(quantity(work, TreeWorkCounter::EncodedFrameBytes, TreeWorkCause::Checkpoint), 0U);
    ASSERT_TRUE(prepared.encoder->bind_overflow({.first = 1, .count = frames, .owner = capture}).ok());
    EXPECT_EQ(prepared.encoder->next(std::span(frame).first(255)).status.code(), Code::kInvalidArgument);
    for (uint64_t i = 0; i <= frames; ++i) {
        ASSERT_TRUE(prepared.encoder->next(frame).status.ok());
    }
    EXPECT_FALSE(prepared.encoder->next(frame).frame.has_value());
    EXPECT_EQ(quantity(work, TreeWorkCounter::EncodedFrameBytes, TreeWorkCause::Checkpoint), (frames + 1) * 256);
    EXPECT_EQ(quantity(work, TreeWorkCounter::EncodedPrimaryFrames, TreeWorkCause::Checkpoint), 1U);
    EXPECT_EQ(quantity(work, TreeWorkCounter::EncodedOverflowFrames, TreeWorkCause::Checkpoint), frames);
    EXPECT_EQ(quantity(work, TreeWorkCounter::EncodingKeyCopyBytes, TreeWorkCause::Checkpoint), 3U);
    EXPECT_EQ(quantity(work, TreeWorkCounter::EncodingValueCopyBytes, TreeWorkCause::Checkpoint), 1000U);
    EXPECT_EQ(quantity(work, TreeWorkCounter::HandoffValueCopyBytes, TreeWorkCause::Flush), 0U);
}

TEST(TreeWorkCounters, CacheDeduplicatesLoadsAndSeparatesHitMissJoinAndEviction)
{
    TestBinding     test;
    auto            credit = test.binding->lifecycle_budget();
    SharedPageCache cache({.max_pages = 2, .max_waiters = 8}, credit);
    auto            upstream = std::make_shared<test_read::TestLoader>();
    upstream->deferred       = true;
    upstream->frames[1]      = test_read::base({
        {.key = "a", .value = "v"}
    });
    auto   input             = test_read::cold(1);
    auto   loader            = cache.bind(upstream, credit);
    size_t completions       = 0;
    auto   complete          = [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(leaf);
        ++completions;
    };
    loader->load_leaf(input, complete);
    loader->load_leaf(input, complete);
    EXPECT_EQ(upstream->calls, 1U);
    upstream->finish();
    loader->load_leaf(input, complete);
    EXPECT_EQ(completions, 3U);
    auto work = test.binding->work_metrics();
    EXPECT_EQ(quantity(work, TreeWorkCounter::CacheMisses, TreeWorkCause::Read), 1U);
    EXPECT_EQ(quantity(work, TreeWorkCounter::CacheJoinedLoads, TreeWorkCause::Read), 1U);
    EXPECT_EQ(quantity(work, TreeWorkCounter::CacheHits, TreeWorkCause::Read), 1U);
    cache.evict();
    EXPECT_EQ(quantity(work, TreeWorkCounter::CacheEvictions, TreeWorkCause::CachePressure), 1U);
}

TEST(TreeWorkCounters, PhysicalDeletionCountsTheObservedMirrorCapacityOnlyOnce)
{
    TestBinding                            test;
    auto                                   credit = test.binding->lifecycle_budget();
    std::shared_ptr<ChunkPhysicalRegistry> registry;
    ASSERT_TRUE(ChunkPhysicalRegistry::create(credit, &registry).ok());
    using Host = test_chunk::TestPhysicalHost;
    std::shared_ptr<ChunkPhysicalPack> pack;
    ASSERT_TRUE(registry->prepare(Host::stage, Host::chunk, credit, &pack).ok());
    ASSERT_TRUE(registry->observe(pack, Host::receipt, credit).ok());
    auto wrong = Host::receipt;
    ++wrong.created_ms;
    EXPECT_EQ(registry->deleted(Host::stage, Host::chunk, wrong, credit).code(), Code::kCorruption);
    auto work = test.binding->work_metrics();
    EXPECT_EQ(quantity(work, TreeWorkCounter::BackingDeletedBytes, TreeWorkCause::GarbageCollection), 0U);
    ASSERT_TRUE(registry->deleted(Host::stage, Host::chunk, Host::receipt, credit).ok());
    ASSERT_TRUE(registry->deleted(Host::stage, Host::chunk, Host::receipt, credit).ok());
    EXPECT_EQ(quantity(work, TreeWorkCounter::BackingDeletedBytes, TreeWorkCause::GarbageCollection),
              Host::receipt.physical_bytes);
}

TEST(TreeWorkCounters, ProxySlotProofCountsOnlyTheColdLoadItAvoided)
{
    TestBinding test;
    auto        logical = std::make_shared<PageState>(*test_read::page({
        {.key = "a", .slot = 7, .value = "new"}
    }));
    logical->bases.push_back(test_read::cold(1, 2));
    auto                          loader = std::make_shared<test_read::TestLoader>();
    std::shared_ptr<PublishedGet> get;
    ASSERT_TRUE(PublishedGet::create(test_read::view(logical), loader, "a",
                                     {.metadata = test.binding->lifecycle_budget()}, &get)
                    .ok());
    test_read::Completion done;
    auto                  connected = stdexec::connect(get->sender(), test_read::Receiver{.done = &done});
    stdexec::start(connected);
    ASSERT_EQ(done.values.load(), 1U) << done.status.to_string();
    EXPECT_EQ(get->result().cell.value.to_string(), "new");
    EXPECT_EQ(loader->calls, 0U);
    EXPECT_EQ(quantity(test.binding->work_metrics(), TreeWorkCounter::ProxyAvoidedLoads, TreeWorkCause::Read), 1U);
}

TEST(TreeWorkCounters, AdmissionRejectAndOneQueuedWaitAreDifferentEvents)
{
    detail::ResourceBudget::Limits limits;
    limits.fill(128);
    detail::ResourceBudget budget(limits, 2);
    auto                   account = budget.make_account(limits);
    auto                   work    = std::make_shared<TreeWorkMetrics>();
    account->set_work_metrics(work);
    auto full = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, 128);
    ASSERT_TRUE(full);
    EXPECT_FALSE(detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, 1));
    EXPECT_EQ(quantity(work, TreeWorkCounter::AdmissionRejected, TreeWorkCause::Allocation), 1U);
    detail::ResourceBudget::Request request(account, detail::BudgetClass::kRouting, 1);
    test_read::Completion           done;
    auto                            connected = stdexec::connect(request.sender(), test_read::Receiver{.done = &done});
    stdexec::start(connected);
    EXPECT_EQ(done.values.load(), 0U);
    EXPECT_EQ(quantity(work, TreeWorkCounter::AdmissionWaits, TreeWorkCause::Allocation), 1U);
    full.reset();
    EXPECT_EQ(done.values.load(), 1U);
    EXPECT_EQ(quantity(work, TreeWorkCounter::AdmissionWaits, TreeWorkCause::Allocation), 1U);
}

TEST(TreeWorkCounters, RegistrySamplesOnceBeforeCallbacksAndNeverDrainsTheCollector)
{
    TestBinding test;
    auto        work      = test.binding->work_metrics();
    auto        collector = work->claim_collector(test.binding->lifecycle_budget());
    ASSERT_TRUE(collector);
    (void)collector->capture_window();
    const auto &credit   = test.binding->lifecycle_budget();
    const auto  baseline = test.binding->account()->snapshot().physical[static_cast<size_t>(AllocationKind::kMetadata)];
    auto        resource = detail::reserved_shared<detail::AllocationMemoryResource>(credit, credit);
    auto        registry = create_tree_work_registry(resource, "test", work, credit);
    TreeMetricsOperation operation(work, TreeStage::PageSplit, 1);
    ASSERT_TRUE(operation.finish(TreeOutcome::Success, 12));
    work->counters.add(TreeWorkCounter::SplitOutputPages, 4, TreeWorkCause::Flush);
    // Executed last, even though its name sorts first. Every quantity gauge
    // must keep the first snapshot until the next registry flush.
    registry->register_callback_gauge_inline("a.mutator", [target = work.get()] {
        target->counters.add(TreeWorkCounter::SplitOutputPages, 4, TreeWorkCause::Flush);
        return 0;
    });
    auto first = rendered(*registry);
    EXPECT_EQ(metric_value(first, "test.work.flush.split.output.pages"), 4U);
    EXPECT_EQ(metric_value(first, "test.split.output.pages.c"), 4U);
    EXPECT_NE(first.find("test.work.page.split.age.complete.g"), std::string::npos);
    EXPECT_EQ(quantity(work, TreeWorkCounter::SplitOutputPages, TreeWorkCause::Flush), 8U);
    const auto window = collector->capture_window();
    EXPECT_EQ(window[static_cast<size_t>(TreeStage::PageSplit)].count, 1U);
    EXPECT_EQ(window[static_cast<size_t>(TreeStage::PageSplit)].sum_ns, 11U);
    EXPECT_EQ(collector->capture_window()[static_cast<size_t>(TreeStage::PageSplit)].count, 0U);
    registry.reset();
    resource.reset();
    test.binding->drain();
    EXPECT_EQ(test.binding->account()->snapshot().physical[static_cast<size_t>(AllocationKind::kMetadata)], baseline);
}
} // namespace
} // namespace crowdb::tree
