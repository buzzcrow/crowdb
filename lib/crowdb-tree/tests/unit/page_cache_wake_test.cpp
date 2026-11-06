// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_runtime.h"
#include "btree/page_cache.h"
#include "btree/page_load_internal.h"
#include "host_completion.h"
#include "published_read_test_support.h"

namespace crowdb::tree
{
namespace
{
class TestAsyncLoader final : public PageLoader
{
  public:
    void load_leaf(BaseRef /*base*/, LeafComplete complete) override
    {
        auto pending =
            std::make_shared<Pending>(Pending{.complete = std::move(complete), .leaf = leaf_, .owner = nullptr});
        pending->owner = pending;
        auto status    = runtime_.try_submit(lane_, &Pending::run, pending.get());
        if (!status.ok()) {
            pending->owner.reset();
            complete_page_load(pending->complete, std::move(status), std::shared_ptr<const LeafBase>{});
        }
    }

  private:
    struct Pending
    {
        LeafComplete                    complete;
        std::shared_ptr<const LeafBase> leaf;
        std::shared_ptr<Pending>        owner;

        static void run(void *context) noexcept
        {
            auto self = std::move(static_cast<Pending *>(context)->owner);
            complete_page_load(self->complete, Status::Ok(), std::move(self->leaf));
        }
    };

    detail::FlushRuntime                        runtime_{1, 8};
    std::shared_ptr<detail::FlushRuntime::Lane> lane_ = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<const LeafBase>             leaf_ = test_read::base({
        {.key = "a", .value = "value"}
    });
};

TEST(SharedPageCacheWake, CrossThreadCompletionAndNextColdRequestCannotLeaveIdleIncomingWork)
{
    SharedPageCache              cache({.max_pages = 1, .max_waiters = 4});
    auto                         upstream = std::make_shared<TestAsyncLoader>();
    auto                         binding  = cache.bind(upstream);
    const std::array<BaseRef, 2> inputs{test_read::cold(1), test_read::cold(2)};
    constexpr size_t             iterations = 50000;
    for (size_t index = 0; index < iterations; ++index) {
        detail::HostCompletion          done;
        Status                          status;
        std::shared_ptr<const LeafBase> leaf;
        binding->load_leaf(inputs[index % inputs.size()], [&](Status outcome, auto result) {
            status = std::move(outcome);
            leaf   = std::move(result);
            done.signal();
        });
        done.wait();
        ASSERT_TRUE(status.ok()) << index << ' ' << status.to_string();
        ASSERT_TRUE(leaf) << index;
        ASSERT_EQ(leaf->count(), 1U);
    }
    EXPECT_EQ(cache.snapshot().misses, iterations);
    EXPECT_EQ(cache.snapshot().waiters, 0U);
}
} // namespace
} // namespace crowdb::tree
