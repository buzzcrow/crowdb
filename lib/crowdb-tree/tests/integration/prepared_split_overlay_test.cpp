// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/prepared_split_overlay.h"
#include "btree/tree_core_access.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <semaphore>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct CreditReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::binary_semaphore *entered;
    std::binary_semaphore *release;

    void set_value() const noexcept
    {
        entered->release();
        release->acquire();
    }

    void set_error(Status /*unused*/) const noexcept
    {
    }

    void set_stopped() const noexcept
    {
    }
};

class PreparedSplit : public testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions runtime;
        runtime.workers = 1;
        ASSERT_TRUE(TreeRuntime::create(runtime, &options.runtime).ok());
        parent = std::make_unique<Crowdbtree>(options);
        child  = std::make_unique<Crowdbtree>(options);
    }

    static Status put(Crowdbtree &tree, uint64_t slot, const std::string &key, const std::string &value)
    {
        return tree.apply(slot, Batch{.ops = {{.key = key, .kind = OpKind::kPut, .value = value}}});
    }

    Status arm(size_t records = 64, size_t bytes = 64 * 1024)
    {
        return PreparedSplitOverlay::arm(*TreeCoreAccess::get(*parent), *TreeCoreAccess::get(*child),
                                         {.max_records = records, .max_bytes = bytes}, &token, &start);
    }

    Config                                options;
    std::unique_ptr<Crowdbtree>           parent, child;
    std::shared_ptr<PreparedSplitOverlay> token;
    uint64_t                              start = UINT64_MAX;
};

TEST_F(PreparedSplit, ContinuousParentRotationAndFlushCannotLoseThePreparedTail)
{
    ASSERT_TRUE(put(*parent, 1, "base", "one").ok());
    ASSERT_TRUE(put(*child, 1, "base", "one").ok());
    auto status = arm();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(start, 1U);
    ASSERT_TRUE(put(*parent, 2, "replayed", "two").ok());
    ASSERT_TRUE(put(*child, 2, "replayed", "two").ok());
    ASSERT_TRUE(parent->flush().ok());
    status = token->prepare(2);
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (uint64_t slot = 3; slot <= 8; ++slot) {
        ASSERT_TRUE(put(*parent, slot, "key" + std::to_string(slot), std::to_string(slot)).ok());
        ASSERT_TRUE(parent->flush().ok());
    }
    const auto active = TreeCoreAccess::get(*parent)->capture()->sources->tables.back();
    ASSERT_FALSE(active->closed());
    InstalledSplitOverlay installed;
    status = token->try_install(8, &installed);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(active->closed());
    EXPECT_EQ(installed.frontier, 8U);
    EXPECT_EQ(child->get_view("base").value().to_string(), "one");
    for (uint64_t slot = 3; slot <= 8; ++slot) {
        auto value = child->get_view("key" + std::to_string(slot));
        ASSERT_TRUE(value.found());
        EXPECT_EQ(value.slot(), slot);
    }
    ASSERT_TRUE(put(*parent, 9, "late", "parent").ok());
    ASSERT_TRUE(put(*child, 9, "late", "child").ok());
    EXPECT_EQ(child->get_view("late").value().to_string(), "child");
    status = parent->publish_split_memtable_view(installed.generation, 8, *child, KeyRange::unbounded());
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(child->clear_split_memtable_overlay(*parent).ok());
    ASSERT_TRUE(parent->release_split_memtable_view(installed.generation).ok());
    token.reset();
    parent.reset();
    EXPECT_EQ(child->get_view("late").value().to_string(), "child");
    ASSERT_TRUE(child->flush().ok());
}

TEST_F(PreparedSplit, InstallDoesNotWaitForCpuAndIsIdempotentOnlyForItsExactFrontier)
{
    auto status = arm();
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = token->prepare(0);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(put(*parent, 1, "tail", "one").ok());
    auto *core = TreeCoreAccess::get(*parent);

    struct Blocked
    {
        std::binary_semaphore started{0}, release{0};
    } blocked;

    ASSERT_TRUE(core->binding()
                    ->cpu()
                    ->try_submit(
                        core->binding()->lane(),
                        [](void *context) noexcept {
                            auto &wait = *static_cast<Blocked *>(context);
                            wait.started.release();
                            wait.release.acquire();
                        },
                        &blocked)
                    .ok());
    blocked.started.acquire();
    InstalledSplitOverlay installed;
    std::thread           caller([&] { status = token->try_install(1, &installed); });
    caller.join(); // must not need the blocked native worker
    blocked.release.release();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(token->committed_frontier(), 1U);
    EXPECT_TRUE(token->try_install(1, &installed).ok());
    EXPECT_FALSE(token->try_install(2, &installed).ok());
    EXPECT_EQ(child->get_view("tail").value().to_string(), "one");
}

TEST_F(PreparedSplit, InstallDoesNotRunCreditWaitCallbacks)
{
    auto status = arm();
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = token->prepare(0);
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto *core = TreeCoreAccess::get(*child);
    core->binding()->account()->drain_retirements();
    core->binding()->drain();

    struct Blocked
    {
        std::binary_semaphore entered{0}, release{0};
    } cpu, callback;

    ASSERT_TRUE(core->binding()
                    ->cpu()
                    ->try_submit(
                        core->binding()->lane(),
                        [](void *context) noexcept {
                            auto &wait = *static_cast<Blocked *>(context);
                            wait.entered.release();
                            wait.release.acquire();
                        },
                        &cpu)
                    .ok());
    cpu.entered.acquire();
    constexpr auto category = detail::BudgetClass::kRouting;
    constexpr auto index    = static_cast<size_t>(category);
    const auto     account  = core->binding()->account();
    const auto     snapshot = account->snapshot();
    auto occupied = detail::ResourceBudget::try_reserve(account, category,
                                                        detail::ResourceBudget::capacity(account, category) -
                                                            snapshot.allocated[index] - snapshot.reserved[index]);

    detail::ResourceBudget::Request request(account, category, 1);
    auto                            operation =
        stdexec::connect(request.sender(), CreditReceiver{.entered = &callback.entered, .release = &callback.release});
    stdexec::start(operation);
    std::promise<Status> result;
    auto                 done = result.get_future();
    std::thread          installer([&] {
        InstalledSplitOverlay installed;
        result.set_value(token->try_install(0, &installed));
    });
    const bool           returned = done.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    // Always unblock both participants before reporting an installation regression.
    if (!returned) {
        callback.release.release();
        cpu.release.release();
    }
    installer.join();
    EXPECT_TRUE(returned);
    EXPECT_TRUE(done.get().ok());
    if (returned) {
        std::thread cleanup([&] { token.reset(); });
        callback.entered.acquire();
        callback.release.release();
        cpu.release.release();
        cleanup.join();
    }
    else {
        token.reset();
    }
}

TEST_F(PreparedSplit, RecordOverflowExpiresTheTokenWithoutRejectingParentWrites)
{
    auto status = arm(1);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = token->prepare(0);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto before = TreeCoreAccess::get(*child)->capture();
    ASSERT_TRUE(put(*parent, 1, "a", "one").ok());
    ASSERT_TRUE(put(*parent, 2, "b", "two").ok());
    InstalledSplitOverlay installed;
    EXPECT_FALSE(token->try_install(2, &installed).ok());
    EXPECT_EQ(TreeCoreAccess::get(*child)->capture(), before);
    token.reset();
    EXPECT_TRUE(put(*child, 1, "own", "works").ok());
    EXPECT_EQ(parent->get_view("b").value().to_string(), "two");
}

TEST_F(PreparedSplit, ByteOverflowAndExplicitChildCloseLeaveItsRootUnchanged)
{
    auto status = arm(8, 16);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = token->prepare(0);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto before = TreeCoreAccess::get(*child)->capture();
    ASSERT_TRUE(put(*parent, 1, "a", std::string(64, 'v')).ok());
    InstalledSplitOverlay installed;
    EXPECT_FALSE(token->try_install(1, &installed).ok());
    EXPECT_EQ(TreeCoreAccess::get(*child)->capture(), before);
    token.reset();
    status = arm();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(put(*child, 1, "a", std::string(64, 'v')).ok());
    status = token->prepare(1);
    ASSERT_TRUE(status.ok()) << status.to_string();
    TreeCoreAccess::get(*child)->close_and_drain();
    EXPECT_FALSE(token->try_install(1, &installed).ok());
    token.reset();
    ASSERT_TRUE(put(*parent, 2, "alive", "yes").ok());
}

TEST_F(PreparedSplit, EmptyTailAndFacadeClosureKeepTheCapabilityMemorySafe)
{
    auto status = arm();
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = token->prepare(0);
    ASSERT_TRUE(status.ok()) << status.to_string();
    InstalledSplitOverlay installed;
    status = token->try_install(0, &installed);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(child->contiguous_slot(), 0U);
    EXPECT_FALSE(TreeCoreAccess::get(*child)->capture()->sources->inheritance);
    EXPECT_TRUE(token->try_install(0, &installed).ok());
    token.reset();
    ASSERT_TRUE(parent->release_split_memtable_view(installed.generation).ok());
    status = arm();
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = token->prepare(0);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::weak_ptr<TreeCore> parent_owner = TreeCoreAccess::get(*parent)->shared_from_this();
    std::weak_ptr<TreeCore> child_owner  = TreeCoreAccess::get(*child)->shared_from_this();
    parent.reset();
    child.reset();
    EXPECT_FALSE(parent_owner.expired());
    EXPECT_FALSE(child_owner.expired());
    EXPECT_FALSE(token->try_install(0, &installed).ok());
    token.reset();
    EXPECT_TRUE(parent_owner.expired());
    EXPECT_TRUE(child_owner.expired());
}

TEST_F(PreparedSplit, RegistrationHighWaterCoversPreexistingOutOfOrderVersions)
{
    ASSERT_TRUE(put(*parent, 3, "future", "three").ok());
    ASSERT_TRUE(put(*parent, 1, "first", "one").ok());
    ASSERT_EQ(parent->contiguous_slot(), 1U);
    auto status = arm();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(start, 3U);
    ASSERT_TRUE(put(*parent, 2, "gap", "two").ok());
    ASSERT_TRUE(put(*child, 1, "first", "one").ok());
    ASSERT_TRUE(put(*child, 2, "gap", "two").ok());
    ASSERT_TRUE(put(*child, 3, "future", "three").ok());
    status = token->prepare(3);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(put(*parent, 4, "tail", "four").ok());
    InstalledSplitOverlay installed;
    status = token->try_install(4, &installed);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(child->get_view("future").value().to_string(), "three");
    EXPECT_EQ(child->get_view("future").slot(), 3U);
    EXPECT_EQ(child->get_view("tail").slot(), 4U);
}
} // namespace
} // namespace crowdb::tree
