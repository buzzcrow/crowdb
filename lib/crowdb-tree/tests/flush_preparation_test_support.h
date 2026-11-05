// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/allocation_bound.h"
#include "btree/apply_progress.h"
#include "btree/flush_operation.h"
#include "btree/page_cache.h"
#include "inner_load_test_support.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
struct TestPreparedFlushTree
{
    static detail::ResourceBudget::Limits limits()
    {
        detail::ResourceBudget::Limits result;
        result.fill(16 * 1024 * 1024);
        result[static_cast<size_t>(detail::BudgetClass::kIo)] = 1;
        return result;
    }

    static std::shared_ptr<detail::AllocationReservation>
    reserve_lifecycle(const std::shared_ptr<detail::ResourceBudget::Account> &account)
    {
        const auto bytes = SourceAdmission::allocation_bound();
        if (!bytes) {
            throw std::bad_alloc();
        }
        auto credit = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kAdmittedWriter, *bytes);
        if (!credit) {
            throw std::bad_alloc();
        }
        return detail::AllocationReservation::create(std::move(*credit));
    }

    detail::ResourceBudget                           budget{limits(), 4};
    std::shared_ptr<detail::ResourceBudget::Account> account   = budget.make_account(limits());
    std::shared_ptr<detail::AllocationReservation>   lifecycle = reserve_lifecycle(account);
    SourceAdmission                                  sources{
        0,
        1,
        {.account           = account,
          .max_entries       = 64,
          .max_key_bytes     = 4096,
          .max_gap_entries   = 8,
          .max_gap_key_bytes = 256,
          .max_key_size      = 256},
        lifecycle
    };
    std::shared_ptr<detail::FlushRuntime>       runtime;
    std::shared_ptr<detail::FlushRuntime::Lane> lane = std::make_shared<detail::FlushRuntime::Lane>(32);
    std::shared_ptr<TreeResourceMetrics>        resources;
    std::shared_ptr<SharedPageCache>            cache;
    PublicationCoordinator                      publisher;
    std::shared_ptr<test_read::TestInnerStore>  store = std::make_shared<test_read::TestInnerStore>();
    std::atomic<uint64_t>                       frontier{0};
    FlushBatchOptions                           batch;
    FlushPreparationOptions                     preparation;
    std::unique_ptr<FlushController>            controller;

    explicit TestPreparedFlushTree(size_t workers = 1, bool cold = false, bool metrics = false)
        : runtime(std::make_shared<detail::FlushRuntime>(workers, 32)),
          resources(metrics ? std::make_shared<TreeResourceMetrics>() : nullptr),
          cache(metrics ? std::make_shared<SharedPageCache>(PageCacheLimits{.max_pages = 2, .max_waiters = 1}, nullptr,
                                                            resources, runtime.get(), lane)
                        : nullptr),
          publisher(64, 1, {}, std::pmr::get_default_resource(), cache)
    {
        batch.run_limits                          = {.max_entries = 4, .max_key_bytes = 256, .key_block_bytes = 64};
        batch.max_sources                         = 8;
        batch.parallelism                         = 4;
        batch.routing_leaves_per_step             = 1;
        preparation.runtime                       = runtime;
        preparation.store                         = store;
        preparation.cache                         = cache;
        preparation.loads.max_blob_bytes          = 128 * 1024;
        preparation.loads.max_frame_bytes         = 128 * 1024;
        preparation.loads.max_inner_children      = 64;
        preparation.loads.max_inner_key_bytes     = 4096;
        auto &maintenance                         = preparation.maintenance;
        maintenance.max_candidates                = 8;
        maintenance.max_visits                    = 16;
        maintenance.max_steps                     = 32;
        maintenance.visits_per_step               = 2;
        maintenance.publication.max_changed_pages = 16;
        maintenance.publication.max_path_pages    = 8;
        auto &pages                               = maintenance.publication.pages;
        pages.max_metadata_bytes                  = 4096;
        pages.max_work_entries                    = 128;
        pages.max_output_pages                    = 8;
        pages.primary_page_bytes                  = 512;
        pages.target_page_bytes                   = 384;
        pages.merge_trigger_bytes                 = 64;
        pages.merge_target_bytes                  = 128;

        auto left           = std::make_shared<PageState>();
        left->range.upper   = "m";
        left->right_sibling = 1;
        auto right          = std::make_shared<PageState>();
        right->range.lower  = "m";
        if (metrics) {
            left->bases.push_back(cold_leaf(0, 10));
            right->bases.push_back(cold_leaf(1, 11));
        }
        auto first         = std::make_shared<PublishedPage>();
        first->leaf        = left;
        auto second        = std::make_shared<PublishedPage>();
        second->leaf       = right;
        auto root          = std::make_shared<PublishedPage>();
        root->level        = 1;
        root->metrics.leaf = false;
        const auto inner   = test_read::inner({"m"}, {0, 1});
        if (cold) {
            root->inner_backing = test_read::inner_backing(store, 0, *inner);
        }
        else {
            root->inner = inner;
        }
        auto catalog = std::make_shared<PublishedSources>();
        catalog->tables.assign(sources.catalog()->tables.begin(), sources.catalog()->tables.end());
        auto request         = std::make_shared<PublicationRequest>();
        request->transaction = {
            .record           = std::make_shared<InstallRecord>(InstallToken{}
                        ),
            .pages            = {{.page_id = 0, .expected = {}, .replacement = first},
                                                                             {.page_id = 1, .expected = {}, .replacement = second},
                                                                             {.page_id = 2, .expected = {}, .replacement = root}},
            .root_page_id     = 2,
            .expected_sources = publisher.capture()->sources,
            .sources          = catalog
        };
        request->on_done = [](PublicationResult result) { EXPECT_TRUE(result.status.ok()); };
        EXPECT_TRUE(publisher.try_submit(request));
        runtime->drain();
        controller = std::make_unique<FlushController>(publisher, *runtime, sources, frontier, lane, batch);
    }

    BaseRef cold_leaf(uint64_t identity, uint64_t address)
    {
        std::array<uint8_t, 512> frame{};
        LeafFrameBuilder         builder(frame.data(), frame.size());
        builder.finish(identity, kInvalidPageId);
        PageSummary summary;
        summary.exact                         = true;
        summary.reachable_leaf_pages          = 1;
        summary.reachable_page_capacity_bytes = frame.size();
        frame_set_summary(frame.data(), frame.size(), summary);
        auto &blob = store->blobs[address];
        encode_durable_page(frame.data(), frame.size(), compress_algo::kNone, &blob);
        auto backing     = std::make_shared<BackingRef>();
        backing->address = address;
        backing->length  = blob.size();
        backing->owner   = store;
        return {.identity = identity, .backing = std::move(backing)};
    }

    [[nodiscard]] detail::ResourceBudget::Limits lifecycle_reservations() const noexcept
    {
        detail::ResourceBudget::Limits result{};
        result[static_cast<size_t>(lifecycle->category())] = lifecycle->available();
        return result;
    }

    std::array<std::optional<detail::ResourceBudget::Credit>, detail::ResourceBudget::kClasses> exhaust_unused() const
    {
        std::array<std::optional<detail::ResourceBudget::Credit>, detail::ResourceBudget::kClasses> blockers;
        const auto snapshot = budget.snapshot();
        for (size_t category = 0; category < blockers.size(); ++category) {
            const auto remaining = limits()[category] - snapshot.allocated[category] - snapshot.reserved[category];
            blockers[category] =
                detail::ResourceBudget::try_reserve(account, static_cast<detail::BudgetClass>(category), remaining);
            EXPECT_TRUE(blockers[category]);
        }
        return blockers;
    }
};
} // namespace crowdb::tree
