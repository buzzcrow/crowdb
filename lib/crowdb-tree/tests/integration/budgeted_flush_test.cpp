// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_operation.h"

#include <gtest/gtest.h>

namespace crowdb::tree
{
namespace
{
TEST(BudgetedFlush, WriterPayloadRemainsChargedAfterHandoffAndReleasesWithLastMappingOwner)
{
    detail::ResourceBudget::Limits limits;
    limits.fill(4 * 1024 * 1024);
    detail::ResourceBudget budget(limits, 4);
    auto                   account      = budget.make_account(limits);
    constexpr auto         writer_class = static_cast<size_t>(detail::BudgetClass::kAdmittedWriter);
    {
        const auto lifecycle_bound = SourceAdmission::allocation_bound();
        if (!lifecycle_bound) {
            FAIL() << "source lifecycle bound overflow";
        }
        auto lifecycle_credit =
            detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kAdmittedWriter, *lifecycle_bound);
        if (!lifecycle_credit) {
            FAIL() << "source lifecycle credit unavailable";
        }
        auto                   lifecycle = detail::AllocationReservation::create(std::move(*lifecycle_credit));
        SourceAdmission        sources(0, 1,
                                       {.account           = account,
                                        .max_entries       = 16,
                                        .max_key_bytes     = 1024,
                                        .max_gap_entries   = 2,
                                        .max_gap_key_bytes = 128},
                                       lifecycle);
        PublicationCoordinator publisher;
        auto                   published_sources = std::make_shared<PublishedSources>();
        published_sources->tables.push_back(sources.catalog()->active);
        auto page                   = std::make_shared<PublishedPage>();
        page->leaf                  = std::make_shared<PageState>();
        auto request                = std::make_shared<PublicationRequest>();
        request->transaction.record = std::make_shared<InstallRecord>(InstallToken{});
        request->transaction.pages.push_back({.page_id = 0, .expected = {}, .replacement = page});
        request->transaction.root_page_id     = 0;
        request->transaction.expected_sources = publisher.capture()->sources;
        request->transaction.sources          = std::move(published_sources);
        request->on_done                      = [](PublicationResult result) {
            ASSERT_TRUE(result.status.ok()) << result.status.to_string();
        };
        ASSERT_TRUE(publisher.try_submit(request));
        request.reset();
        SourceAdmission::Writer writer;
        constexpr size_t        payload_bytes = 8192;
        ASSERT_TRUE(sources.try_admit(0, {8, 8, 8 * (payload_bytes + kCellHeaderSize)}, &writer).ok());
        std::weak_ptr<MemTable> old_source = writer.table();
        for (uint64_t slot = 1; slot <= 8; ++slot) {
            ASSERT_TRUE(writer
                            .upsert(std::to_string(slot), slot,
                                    encode_cell_buf(slot, OpKind::kPut, std::string(payload_bytes, 'v')))
                            .ok());
        }
        writer.release();
        EXPECT_EQ(budget.snapshot().reserved[writer_class], lifecycle->available());
        detail::FlushRuntime  runtime(1, 8);
        auto                  lane = std::make_shared<detail::FlushRuntime::Lane>(8);
        std::atomic<uint64_t> frontier{8};
        FlushController       controller(publisher, runtime, sources, frontier, lane);
        auto reserved = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kHandoff, 1024 * 1024);
        if (!reserved) {
            FAIL() << "test handoff credit unavailable";
        }
        auto metadata = std::make_shared<detail::AllocationReservation>(std::move(*reserved));
        std::shared_ptr<FlushOperation> flush;
        ASSERT_TRUE(controller.prepare(metadata, &flush).ok());
        ASSERT_TRUE(stdexec::sync_wait(flush->sender()));
        runtime.drain();
        ASSERT_TRUE(flush->result().handoff_status.ok()) << flush->result().handoff_status.to_string();
        EXPECT_EQ(flush->result().published_coverage, 8U);
        publisher.reclaim();
        sources.reclaim();
        EXPECT_TRUE(old_source.expired());
        EXPECT_GE(budget.snapshot().allocated[writer_class], 8 * payload_bytes);
        // Retired source infrastructure returns its bytes to the still-live
        // lifecycle owner. All separate writer reservations must be sealed.
        EXPECT_EQ(budget.snapshot().reserved[writer_class], lifecycle->available());
        auto view = publisher.capture();
        EXPECT_EQ(view->mapping.get(0)->leaf->lookup("1").cell.value.size(), payload_bytes);
    }
    for (auto allocated : budget.snapshot().allocated) {
        EXPECT_EQ(allocated, 0U);
    }
    for (auto reserved : budget.snapshot().reserved) {
        EXPECT_EQ(reserved, 0U);
    }
}
} // namespace
} // namespace crowdb::tree
