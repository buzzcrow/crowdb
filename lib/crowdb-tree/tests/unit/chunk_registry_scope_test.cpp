// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_chunk_physical.h"
#include "common/test_chunk_root.h"

#include <thread>

namespace crowdb::tree
{
TEST(ChunkRegistryScope, HostAndLogicalTreeScopeRemainDistinctAcrossClosedHostQueues)
{
    test_chunk::TestRootBackend backend;
    uint64_t                    first_scope = 0;
    {
        test_chunk::TestPhysicalHost           first;
        std::shared_ptr<ChunkPhysicalRegistry> one;
        std::shared_ptr<ChunkPhysicalRegistry> same;
        std::shared_ptr<ChunkPhysicalRegistry> other_tree;
        ASSERT_TRUE(backend.transport->physical_registry(19, first.budget, &one).ok());
        ASSERT_TRUE(backend.transport->physical_registry(19, first.budget, &same).ok());
        ASSERT_TRUE(backend.transport->physical_registry(20, first.budget, &other_tree).ok());
        EXPECT_EQ(one, same);
        EXPECT_NE(one->backing_stats().scope_id, other_tree->backing_stats().scope_id);
        first_scope = one->backing_stats().scope_id;
    }
    test_chunk::TestPhysicalHost           second;
    std::shared_ptr<ChunkPhysicalRegistry> next_host;
    ASSERT_TRUE(backend.transport->physical_registry(19, second.budget, &next_host).ok());
    EXPECT_NE(next_host->backing_stats().scope_id, first_scope);
    EXPECT_EQ(next_host->backing_stats().allocated_bytes, 0U);
}

TEST(ChunkRegistryScope, ConcurrentFacadesShareOneCanonicalLedger)
{
    test_chunk::TestPhysicalHost           host;
    test_chunk::TestRootBackend            backend;
    std::shared_ptr<ChunkPhysicalRegistry> first;
    std::shared_ptr<ChunkPhysicalRegistry> second;
    Status                                 one;
    Status                                 two;
    std::jthread a([&] { one = backend.transport->physical_registry(19, host.budget, &first); });
    std::jthread b([&] { two = backend.transport->physical_registry(19, host.budget, &second); });
    a.join();
    b.join();
    ASSERT_TRUE(one.ok());
    ASSERT_TRUE(two.ok());
    EXPECT_EQ(first, second);
}

TEST(ChunkRegistryScope, EveryStartupAllocationFailureReturnsItsPaidMetadataWithoutExternalAllocation)
{
    test_chunk::TestPhysicalHost host;
    bool                         accepted = false;
    for (int allowance = 0; allowance < 16; ++allowance) {
        test_chunk::TestRootBackend backend;
        auto                        credit = std::make_shared<test_chunk::TestPhysicalBudget>();
        credit->queue                      = host.queue;
        credit->allowances                 = allowance;
        std::shared_ptr<ChunkPhysicalRegistry> registry;
        const auto                             status = backend.transport->physical_registry(19, credit, &registry);
        if (status.ok()) {
            accepted = true;
            ASSERT_TRUE(registry);
        }
        else {
            EXPECT_EQ(status.code(), Code::kResourceExhausted);
            EXPECT_FALSE(registry);
        }
        EXPECT_TRUE(backend.transport->receipts.empty());
        credit->reject = true;
        registry.reset();
        backend.domain->transport.reset();
        backend.transport.reset();
        host.cpu.drain();
        EXPECT_EQ(credit->claimed.load(), 0U) << allowance;
    }
    EXPECT_TRUE(accepted);
}
} // namespace crowdb::tree
