// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/metrics/metrics.h"
#include "runtime/allocation_memory_resource.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <memory>
#include <string>

namespace crowdb::tree::detail
{
namespace
{
using common::metrics::MetricsRegistry;

class TestRegistryBudget final : public AllocationBudget
{
  public:
    bool claim(size_t bytes) noexcept override
    {
        if (calls++ == fail_at) {
            return false;
        }
        used += bytes;
        return true;
    }

    void release(size_t bytes) noexcept override
    {
        used -= bytes;
    }

    void record_allocation(AllocationKind kind, size_t bytes, bool acquired) noexcept override
    {
        EXPECT_EQ(kind, AllocationKind::kMetadata);
        if (acquired) {
            physical += bytes;
        }
        else {
            physical -= bytes;
        }
    }

    size_t used     = 0;
    size_t physical = 0;
    size_t calls    = 0;
    size_t fail_at  = SIZE_MAX;
};

template <class F> bool throws_bad_alloc(F function)
{
    try {
        function();
    }
    catch (const std::bad_alloc &) {
        return true;
    }
    return false;
}

std::string output(MetricsRegistry &registry)
{
    std::unique_ptr<FILE, decltype(&std::fclose)> file(std::tmpfile(), &std::fclose);
    if (!file) {
        return {};
    }
    registry.flush_to(file.get(), 1, "test");
    EXPECT_EQ(std::fseek(file.get(), 0, SEEK_SET), 0);
    std::string           result;
    std::array<char, 512> bytes{};
    while (const auto count = std::fread(bytes.data(), 1, bytes.size(), file.get())) {
        result.append(bytes.data(), count);
    }
    return result;
}

TEST(MetricsRegistryAllocation, NamesNodesRegistryAndSortWindowUseActualBudget)
{
    auto budget = std::make_shared<TestRegistryBudget>();
    {
        auto resource =
            std::allocate_shared<AllocationMemoryResource>(MetadataAllocator<AllocationMemoryResource>(budget), budget);
        auto registry     = std::allocate_shared<MetricsRegistry>(MetadataAllocator<MetricsRegistry>(budget), resource);
        const auto before = budget->used;
        uint64_t   value  = 41;
        auto      *first =
            registry->register_callback_gauge_inline("a.long.tree.instance.prefix", ".z", [&value] { return value; });
        const auto with_first = budget->used;
        EXPECT_GT(with_first, before);
        registry->register_callback_gauge_inline("a.long.tree.instance.prefix", ".a", [] { return 7; });
        EXPECT_EQ(first->get(), 41U);
        const auto retained = budget->used;
        const auto calls    = budget->calls;
        const auto rendered = output(*registry);
        EXPECT_LT(rendered.find("prefix.a"), rendered.find("prefix.z"));
        EXPECT_NE(rendered.find("41"), std::string::npos);
        EXPECT_GT(budget->calls, calls);
        EXPECT_EQ(budget->used, retained);
        EXPECT_EQ(budget->physical, retained);
        const auto *same = registry->register_callback_gauge_inline("a.long.tree.instance.prefix.z", [] { return 99; });
        EXPECT_EQ(same, first);
        EXPECT_EQ(first->get(), 99U);
        EXPECT_EQ(budget->used, retained);
        resource.reset();
        EXPECT_EQ(first->get(), 99U);
    }
    EXPECT_EQ(budget->used, 0U);
    EXPECT_EQ(budget->physical, 0U);
}

TEST(MetricsRegistryAllocation, RegistrationFailureAtEveryAllocationLeavesExistingGaugeUsable)
{
    for (size_t fail = 0; fail < 2; ++fail) {
        auto budget = std::make_shared<TestRegistryBudget>();
        {
            auto            resource = std::make_shared<AllocationMemoryResource>(budget);
            MetricsRegistry registry(resource);
            auto           *existing = registry.register_callback_gauge_inline("live", [] { return 5; });
            const auto      retained = budget->used;
            budget->fail_at          = budget->calls + fail;
            EXPECT_TRUE(throws_bad_alloc(
                [&] { registry.register_callback_gauge_inline("another.long.paid.tree.name", [] { return 17; }); }));
            EXPECT_EQ(existing->get(), 5U);
            EXPECT_EQ(registry.max_name_len(), 4U);
            EXPECT_EQ(budget->used, retained);
            EXPECT_EQ(budget->physical, retained);
        }
        EXPECT_EQ(budget->used, 0U);
    }
}

TEST(MetricsRegistryAllocation, FlushScratchRefusalRetainsCallbacksWithoutUnchargedFallback)
{
    auto budget = std::make_shared<TestRegistryBudget>();
    {
        auto            resource = std::make_shared<AllocationMemoryResource>(budget);
        MetricsRegistry registry(resource);
        registry.register_callback_gauge_inline("retained", [] { return 73; });
        const auto retained = budget->used;
        budget->fail_at     = budget->calls;
        EXPECT_TRUE(throws_bad_alloc([&] { (void)output(registry); }));
        EXPECT_EQ(budget->used, retained);
        EXPECT_NE(output(registry).find("73"), std::string::npos);
    }
    EXPECT_EQ(budget->used, 0U);
    EXPECT_EQ(budget->physical, 0U);
}

TEST(MetricsRegistryAllocation, InlineGaugeCoexistsWithLegacyGaugeAndCallback)
{
    MetricsRegistry registry;
    registry.register_gauge("b.regular")->set(8);
    registry.register_callback_gauge("c.callback", [] { return 9; });
    registry.register_callback_gauge_inline("a.inline", [] { return 7; });
    const auto rendered = output(registry);
    EXPECT_LT(rendered.find("a.inline"), rendered.find("b.regular"));
    EXPECT_LT(rendered.find("b.regular"), rendered.find("c.callback"));
    EXPECT_EQ(registry.max_name_len(), 10U);
}
} // namespace
} // namespace crowdb::tree::detail
