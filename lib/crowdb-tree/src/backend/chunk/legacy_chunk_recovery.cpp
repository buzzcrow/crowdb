// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "legacy_chunk_recovery.h"

#include "backend/local/local_backend_state.h"
#include "backend/local/scheduled_page_store.h"
#include "btree/backing_identity.h"
#include "chunk_frame.h"
#include "chunk_runtime_page_store.h"
#include "snapshot/local_recovery.h"
#include "snapshot/operation_wait.h"
#include "snapshot/recovered_state.h"

namespace crowdb::tree::detail
{
namespace
{
struct RecoveryCredits
{
    PageLoadBudget budget;

    ~RecoveryCredits()
    {
        if (budget.io) {
            budget.io->seal();
        }
        if (budget.reservation()) {
            budget.reservation()->seal();
        }
    }
};

Status recover(const Config &config, const std::shared_ptr<TreeRuntimeBinding> &binding,
               const std::shared_ptr<LegacyChunkReadStore> &store, size_t reader_bytes, std::shared_ptr<TreeCore> *out)
{
    const auto available = binding->options().limits.routing_bytes;
    const auto lifecycle = binding->options().lifecycle_bytes;
    if (lifecycle >= available || reader_bytes >= available - lifecycle) {
        return Status::resource_exhausted("legacy Chunk metadata leaves no recovery workspace");
    }
    RecoveryCredits credits;
    auto            bytes =
        ResourceBudget::try_reserve(binding->account(), BudgetClass::kRouting, available - lifecycle - reader_bytes);
    auto io = ResourceBudget::try_reserve(binding->account(), BudgetClass::kIo, 1);
    if (!bytes || !io) {
        return Status::resource_exhausted();
    }
    credits.budget.bytes = AllocationReservation::create(std::move(*bytes));
    credits.budget.io    = reserved_shared<AllocationReservation>(credits.budget.reservation(), std::move(*io));
    auto local           = reserved_shared<LocalBackendState>(binding->lifecycle_budget());
    auto status          = LocalBackendDomain::claim(store.get(), store, binding->lifecycle_budget(), &local->domain);
    if (status.ok()) {
        status = ScheduledPageStore::create(store.get(), nullptr, store, binding, binding->lifecycle_budget(),
                                            binding->backend_lane(), &local->store);
    }
    if (!status.ok()) {
        return status;
    }
    LocalRecoveryLimits limits;
    limits.accept_legacy_chunk_refs = true;
    limits.max_frame_bytes          = std::max(limits.max_frame_bytes, size_t{config.frame_bytes});
    limits.max_blob_bytes           = round_up_to_iu(limits.max_frame_bytes + kDurableBlobHeader, store->iu_size());
    ExtentLedgerLimits                      domain{.backend_id   = allocate_backend_identity(),
                                                   .region_begin = snapshot::region_base_for(store->iu_size()),
                                                   .file_size    = store->size(),
                                                   .iu_size      = store->iu_size()};
    std::shared_ptr<LocalRecoveryOperation> recovered;
    status = LocalRecoveryOperation::create(local->store, binding->cpu(), binding->lane(), domain, limits,
                                            credits.budget, &recovered);
    if (status.ok()) {
        status = wait_snapshot_operation(recovered);
    }
    if (!status.ok()) {
        return status;
    }
    local->ledger      = recovered->result().ledger;
    local->checkpoints = recovered->result().registry;
    local->inherited   = recovered->result().latest;
    if (!local->inherited) {
        return Status::corruption("legacy Chunk root has no committed Tree anchor");
    }
    PageLoadLimits loads;
    loads.max_blob_bytes  = limits.max_blob_bytes;
    loads.max_frame_bytes = limits.max_frame_bytes;
    auto loader =
        reserved_shared<AsyncStorePageLoader>(credits.budget.reservation(), local->store, binding->cpu(),
                                              binding->lane(), loads, OverflowBackingResolver{}, credits.budget);
    std::shared_ptr<RecoveredStateOperation> graph;
    status = RecoveredStateOperation::create(local->inherited, loader, binding->cpu(), binding->lane(), {},
                                             credits.budget.reservation(), &graph);
    if (status.ok()) {
        status = wait_snapshot_operation(graph);
    }
    if (!status.ok()) {
        return status;
    }
    const auto initial = graph->result().state;
    graph.reset();
    loader.reset();
    recovered.reset();
    credits.budget.io->seal();
    credits.budget.reservation()->seal();
    binding->drain();
    return TreeCore::create(config, binding, {.initial = initial, .local = std::move(local)}, out);
}
} // namespace

Status recover_legacy_chunk_source(const Config &destination, uint64_t generation,
                                   std::shared_ptr<LegacyChunkReadStore> *store, std::shared_ptr<TreeCore> *source)
{
    auto *facade = dynamic_cast<ChunkRuntimePageStore *>(destination.page_store);
    if (facade == nullptr || !destination.runtime || store == nullptr || *store || source == nullptr || *source) {
        return Status::invalid_argument();
    }
    std::shared_ptr<TreeRuntimeBinding> binding;
    auto status = TreeRuntimeBinding::create(destination.runtime, destination.runtime_binding, &binding);
    if (!status.ok()) {
        return status;
    }
    const auto reader_bytes = (2 * kMaxCandidateManifestBytes) + ((256 * 28) + 24) +
                              (destination.runtime->options().backend_workers * protocol::kMaxFrameBytes) +
                              shared_allocation_bound<LegacyChunkReadStore>() + 4096;
    auto       reserved     = ResourceBudget::try_reserve(binding->account(), BudgetClass::kRouting, reader_bytes);
    if (!reserved) {
        return Status::resource_exhausted();
    }
    auto        metadata = AllocationReservation::create(std::move(*reserved));
    const auto &options  = facade->options();
    *store = reserved_shared<LegacyChunkReadStore>(metadata, facade->catalog(), facade->transport(), metadata,
                                                   options.tree, options.owner_epoch, options.iu_size, options.mirrors);
    bool committed = false;
    status         = (*store)->resolve_migration(generation, &committed);
    if (!status.ok() || committed) {
        return status;
    }
    status = (*store)->open(generation);
    if (!status.ok()) {
        return status;
    }
    Config config           = destination;
    config.page_store       = store->get();
    config.backend_owner    = *store;
    config.async_page_store = nullptr;
    config.backend_label    = "chunk-migration-source";
    return recover(config, binding, *store, reader_bytes, source);
}
} // namespace crowdb::tree::detail
