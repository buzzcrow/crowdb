// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_bound.h"
#include "btree/apply_dispatcher.h"
#include "btree/page_split_operation.h"
#include "published_read_test_support.h"

#include <future>

namespace crowdb::tree::test_structure
{
struct TestCheckpointStructure
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        TestCheckpointStructure *owner;

        void finish(Status status) const noexcept
        {
            owner->terminal = std::move(status);
            owner->done.store(true, std::memory_order_release);
        }

        void set_value() const noexcept
        {
            finish(Status::Ok());
        }

        void set_error(Status status) const noexcept
        {
            finish(std::move(status));
        }

        void set_stopped() const noexcept
        {
            finish(Status::unavailable());
        }
    };

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, TestCheckpointStructure *owner)
            : connected(stdexec::connect(sender, Receiver{owner}))
        {
        }

        stdexec::connect_result_t<detail::CallbackSender, Receiver> connected;
    };

    std::optional<Attempt> attempt;

    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits value;
        value.fill(64 * 1024 * 1024);
        return value;
    }

    detail::ResourceBudget                           budget{limits(), 8};
    std::shared_ptr<detail::ResourceBudget::Account> account = budget.make_account(limits());
    PublicationCoordinator                           publisher;
    std::shared_ptr<detail::FlushRuntime>            runtime = std::make_shared<detail::FlushRuntime>(2, 16);
    std::shared_ptr<detail::FlushRuntime::Lane>      lane    = std::make_shared<detail::FlushRuntime::Lane>(16);
    std::shared_ptr<ApplyDispatcher>                 progress;
    std::shared_ptr<test_read::TestLoader>           loader = std::make_shared<test_read::TestLoader>();
    std::shared_ptr<PageSplitOperation>              operation;
    std::atomic<bool>                                done{false};
    Status                                           terminal;
    uint64_t                                         sequence = 0;

    TestCheckpointStructure()
    {
        auto reserved = detail::ResourceBudget::try_reserve(
            account, detail::BudgetClass::kRouting,
            ApplyDispatcher::allocation_bound() + detail::shared_allocation_bound<detail::AllocationReservation>());
        EXPECT_TRUE(reserved.has_value());
        if (reserved) {
            auto credit = detail::AllocationReservation::create(std::move(*reserved));
            EXPECT_TRUE(ApplyDispatcher::create(runtime, lane, 8, credit, &progress).ok());
            credit->seal();
        }
    }

    ~TestCheckpointStructure()
    {
        try {
            if (operation) {
                operation->request_stop();
            }
            if (!loader->pending.empty()) {
                loader->finish(Status::unavailable());
            }
            runtime->drain();
            operation.reset();
            if (progress) {
                progress->close_and_drain();
            }
            runtime->drain();
        }
        catch (const std::exception &error) {
            ADD_FAILURE() << error.what();
        }
        catch (...) {
            ADD_FAILURE() << "checkpoint fixture cleanup failed";
        }
    }

    Status publish(std::shared_ptr<const PageState> logical, bool split_candidate = false)
    {
        const auto old                       = publisher.capture()->mapping.get(0);
        auto       replacement               = std::make_shared<PublishedPage>();
        replacement->leaf                    = std::move(logical);
        replacement->revision                = old ? old->revision + 1 : 1;
        replacement->metrics.dirty           = true;
        replacement->metrics.split_candidate = split_candidate;
        auto sources                         = std::make_shared<PublishedSources>();
        sources->floor                       = 10;
        auto request                         = std::make_shared<PublicationRequest>();
        request->transaction = {.record                = std::make_shared<InstallRecord>(InstallToken{
                                    .generation = publisher.capture()->generation, .sequence = ++sequence}),
                                .pages                 = {{.page_id = 0, .expected = old, .replacement = replacement}},
                                .root_page_id          = 0,
                                .coverage              = 10,
                                .expected_sources      = publisher.capture()->sources,
                                .sources               = std::move(sources),
                                .expected_next_page_id = publisher.capture()->next_page_id,
                                .next_page_id          = 1};
        std::promise<Status> completed;
        auto                 future = completed.get_future();
        request->on_done            = [&](PublicationResult result) { completed.set_value(result.status); };
        auto status                 = publisher.try_submit(request) ? future.get() : Status::unavailable();
        if (!status.ok()) {
            ADD_FAILURE() << status.to_string();
        }
        return status;
    }

    Status prepare(std::shared_ptr<const PublishedTreeState> view, size_t max_output_pages = 4,
                   size_t max_work_entries = 16)
    {
        PageMaintenanceOperationOptions options;
        options.max_candidates = options.max_steps = options.max_visits = options.visits_per_step = 1;
        options.publication.pages.primary_page_bytes                                              = 512;
        options.publication.pages.target_page_bytes                                               = 256;
        options.publication.pages.merge_trigger_bytes                                             = 64;
        options.publication.pages.merge_target_bytes                                              = 128;
        options.publication.pages.max_work_entries                                                = max_work_entries;
        options.publication.pages.max_output_pages                                                = max_output_pages;
        options.publication.pages.max_metadata_bytes                                              = 1024 * 1024;
        options.publication.max_inner_bytes                                                       = 512;
        const auto bound = page_maintenance_reserve_bound(view->mapping, 0, options);
        if (!bound) {
            return Status::resource_exhausted();
        }
        const auto complete = detail::AllocationBound(*bound)
                                  .add(PageSplitOperation::control_allocation_bound())
                                  .add(detail::shared_allocation_bound<detail::AllocationReservation>())
                                  .value();
        if (!complete) {
            return Status::resource_exhausted();
        }
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim, *complete);
        if (!credit) {
            return Status::resource_exhausted();
        }
        options.prepaid = detail::AllocationReservation::create(std::move(*credit));
        return PageSplitOperation::create(publisher, *runtime, lane, std::move(view), 0, std::move(options), loader,
                                          progress, {}, TreeWorkCause::Checkpoint, &operation);
    }

    void begin()
    {
        attempt.emplace(operation->sender(), this);
        stdexec::start(attempt->connected);
        runtime->drain();
    }
};

inline std::shared_ptr<const PageState> structural_input(bool cold = false)
{
    auto leaf = std::make_shared<PageState>(*test_read::page({
        {.key = "a", .value = std::string(80, 'a')},
        {.key = "c", .value = std::string(80, 'c')},
        {.key = "d", .value = std::string(80, 'd')},
        {.key = "e", .value = std::string(80, 'e')},
        {.key = "f", .value = std::string(80, 'f')},
        {.key = "g", .value = std::string(80, 'g')}
    }));
    if (cold) {
        leaf->bases.push_back(test_read::cold(1));
    }
    return leaf;
}

} // namespace crowdb::tree::test_structure
