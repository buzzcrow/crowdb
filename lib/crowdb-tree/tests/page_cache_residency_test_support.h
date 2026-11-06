// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_cache.h"
#include "published_read_test_support.h"

namespace crowdb::tree::test_cache_residency
{
using namespace test_read;

inline std::shared_ptr<PublishedPage> published(std::initializer_list<BaseRef> refs)
{
    auto leaf = std::make_shared<PageState>();
    leaf->bases.assign(refs.begin(), refs.end());
    auto page  = std::make_shared<PublishedPage>();
    page->leaf = std::move(leaf);
    return page;
}

struct TestCache
{
    detail::FlushRuntime                        runtime{1, 8};
    std::shared_ptr<detail::FlushRuntime::Lane> lane        = std::make_shared<detail::FlushRuntime::Lane>(8);
    bool                                        synchronize = true;
    std::shared_ptr<TreeResourceMetrics>        resources   = std::make_shared<TreeResourceMetrics>();
    std::shared_ptr<SharedPageCache>            cache =
        std::make_shared<SharedPageCache>(PageCacheLimits{}, nullptr, resources, &runtime, lane);
    PublicationCoordinator      publisher{64, 1, {}, std::pmr::get_default_resource(), cache};
    std::shared_ptr<TestLoader> upstream = std::make_shared<TestLoader>();
    std::shared_ptr<PageLoader> loader   = cache->bind(upstream);
    uint64_t                    sequence = 0;

    PublicationResult publish(PublicationTransaction::Pages pages, std::optional<uint64_t> root = {},
                              std::shared_ptr<detail::AllocationReservation> metadata = {})
    {
        auto request         = std::make_shared<PublicationRequest>();
        request->transaction = {.record       = std::make_shared<InstallRecord>(InstallToken{.sequence = ++sequence}),
                                .pages        = std::move(pages),
                                .root_page_id = root,
                                .metadata     = std::move(metadata)};
        PublicationResult result;
        size_t            calls = 0;
        request->on_done        = [&](PublicationResult value) {
            result = std::move(value);
            ++calls;
        };
        EXPECT_TRUE(publisher.try_submit(request));
        EXPECT_EQ(calls, 1U);
        if (synchronize) {
            runtime.drain();
        }
        return result;
    }

    std::shared_ptr<const LeafBase> load(BaseRef input)
    {
        std::shared_ptr<const LeafBase> result;
        loader->load_leaf(std::move(input), [&](Status status, auto leaf) {
            EXPECT_TRUE(status.ok()) << status.to_string();
            result = std::move(leaf);
        });
        runtime.drain();
        return result;
    }

    void frame(uint64_t identity)
    {
        upstream->frames[identity] = base({
            {.key = "a", .value = "value"}
        });
    }
};
} // namespace crowdb::tree::test_cache_residency
