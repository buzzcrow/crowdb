// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_backing.h"
#include "backend/local/scheduled_page_store.h"
#include "common/test_chunk_stage_backend.h"
#include "load_budget_test_support.h"

namespace crowdb::tree::test_chunk
{
class TestProtectedCatalog final : public detail::TestStageCatalog, public detail::ChunkProtectionCatalog
{
  public:
    std::shared_ptr<detail::ChunkPinReleaseQueue> queue;
    mutable size_t                                reader_checks = 0;
    size_t                                        fence_at      = SIZE_MAX;
    size_t                                        unpins        = 0;
    bool                                          fail_unpin    = false;

    detail::ChunkProtectionCatalog *protection() noexcept override
    {
        return this;
    }

    std::shared_ptr<detail::ChunkPinReleaseQueue> pin_release_queue() const noexcept override
    {
        return queue;
    }

    Status check_read_owner() const override
    {
        return ++reader_checks >= fence_at ? Status::unavailable() : Status::Ok();
    }

    Status change_pin(uint64_t /*tree*/, detail::ChunkStageId /*stage*/, detail::ChunkStageId /*pin*/,
                      bool acquire) override
    {
        if (!acquire) {
            ++unpins;
        }
        return !acquire && fail_unpin ? Status::unavailable() : Status::Ok();
    }

    Status writer_quiesced(uint64_t /*tree*/, uint64_t /*epoch*/) override
    {
        return Status::Ok();
    }

    Status load_root_bytes(uint64_t /*tree*/, uint64_t /*generation*/, std::vector<uint8_t> * /*out*/) const override
    {
        return Status::not_supported();
    }

    Status begin_candidate(uint64_t /*tree*/, const detail::ChunkManifestCandidate & /*unused*/) override
    {
        return Status::not_supported();
    }

    Status load_candidate(uint64_t /*tree*/, detail::ChunkStageId /*unused*/,
                          std::optional<detail::ChunkCandidateStatus> * /*out*/) const override
    {
        return Status::not_supported();
    }

    Status list_candidates(uint64_t /*tree*/, std::optional<detail::ChunkStageId> /*after*/, uint32_t /*limit*/,
                           std::vector<detail::ChunkCandidateStatus> * /*out*/) const override
    {
        return Status::not_supported();
    }

    Status prepare_references(uint64_t /*tree*/, detail::ChunkStageId /*unused*/, uint64_t /*window*/,
                              std::span<const detail::ChunkStageId> /*unused*/) override
    {
        return Status::not_supported();
    }

    Status finish_candidate(uint64_t /*tree*/, detail::ChunkStageId /*unused*/, uint32_t /*action*/) override
    {
        return Status::not_supported();
    }

    Status release_references(uint64_t /*tree*/, detail::ChunkStageId /*unused*/, uint64_t /*window*/) override
    {
        return Status::not_supported();
    }

    Status prune_pins(uint64_t /*tree*/, detail::ChunkStageId /*unused*/, std::optional<detail::ChunkStageId> /*after*/,
                      uint32_t /*limit*/, detail::ChunkPinProgress * /*out*/) override
    {
        return Status::not_supported();
    }
};

struct TestChunkBackend
{
    test_read::LoadCredits                      credits;
    std::shared_ptr<TestProtectedCatalog>       catalog   = std::make_shared<TestProtectedCatalog>();
    std::shared_ptr<detail::TestFixedTransport> transport = std::make_shared<detail::TestFixedTransport>(catalog);
    std::shared_ptr<ChunkBackingDomain>         domain    = std::make_shared<ChunkBackingDomain>();
    std::shared_ptr<TreeRuntime>                runtime;
    std::shared_ptr<TreeRuntimeBinding>         binding;
    std::shared_ptr<AsyncPageStore>             scheduled;
    MemPageStore                                empty_device{4096};

    TestChunkBackend()
    {
        EXPECT_TRUE(detail::ChunkPinReleaseQueue::create(
                        credits.credits.reservation(), 16, std::make_shared<int>(1), [](void *) noexcept {}, nullptr,
                        &catalog->queue)
                        .ok());
        domain->backend_id = 17;
        domain->tree       = 19;
        domain->epoch      = 1;
        domain->mirrors    = 2;
        domain->catalog    = catalog;
        domain->transport  = transport;
        TreeRuntimeOptions options;
        options.workers         = 1;
        options.backend_workers = 1;
        EXPECT_TRUE(TreeRuntime::create(options, &runtime).ok());
        EXPECT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
        EXPECT_TRUE(ScheduledPageStore::create(&empty_device, nullptr, {}, binding, binding->lifecycle_budget(),
                                               binding->backend_lane(), &scheduled)
                        .ok());
    }

    ~TestChunkBackend()
    {
        binding->drain();
        catalog->fail_unpin = false;
        catalog->queue->stop_admission();
        (void)catalog->queue->pump(16);
        EXPECT_TRUE(catalog->queue->drained());
        catalog->record.reset();
    }

    Status prepare(uint32_t bytes, std::shared_ptr<ChunkPackBacking> *out) const
    {
        return prepare_chunk_backing(domain, {.owner_epoch = 1, .operation_high = 4, .operation_low = 5},
                                     {uint64_t{2} << 56U, 1}, bytes, PagePurpose::kBtreePage,
                                     credits.credits.reservation(), out);
    }
};
} // namespace crowdb::tree::test_chunk
