// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_backend_state.h"
#include "backend/chunk/chunk_protocol_operation.h"
#include "backend/chunk/legacy_chunk_read_store.h"
#include "btree/backing_identity.h"
#include "btree/tree_core.h"
#include "btree/tree_core_geometry.h"
#include "runtime/tree_runtime_internal.h"
#include "snapshot/chunk_root_recovery.h"
#include "snapshot/operation_wait.h"
#include "snapshot/recovered_state.h"

namespace crowdb::tree
{
namespace
{
struct ChunkOpen
{
    ChunkOpen(std::shared_ptr<TreeRuntimeBinding> binding, std::shared_ptr<ChunkBackendState> backend)
        : binding(std::move(binding)),
          backend(std::move(backend)),
          lifetime(std::in_place, 1, this->binding->lifecycle_budget(), this->backend)
    {
    }

    ~ChunkOpen()
    {
        finish();
    }

    void finish()
    {
        if (finished) {
            return;
        }
        finished = true;
        store.reset();
        ticket.reset();
        if (lifetime) {
            lifetime->close_and_drain();
        }
        lifetime.reset();
        backend.reset();
        binding->drain();
        if (budget.io) {
            budget.io->seal();
        }
        if (budget.reservation()) {
            budget.reservation()->seal();
        }
        budget = {};
    }

    bool                                           finished = false;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<ChunkBackendState>             backend;
    std::optional<TreeOperationLifetime>           lifetime;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<AsyncPageStore>                store;
    PageLoadBudget                                 budget;
};

struct RootLoad
{
    explicit RootLoad(const std::shared_ptr<detail::AllocationReservation> &metadata)
        : bytes(MetadataAllocator<uint8_t>(metadata)),
          root(metadata)
    {
    }

    std::shared_ptr<const ChunkBackingDomain> domain;
    detail::ChunkRootBytes                    bytes;
    ChunkRootManifest                         root;
    uint64_t                                  generation        = 0;
    uint64_t                                  latest_generation = 0;
    bool                                      loaded_current    = false;
    bool                                      absent            = false;
    bool                                      done              = false;

    Status step(bool stopped)
    {
        if (stopped) {
            return Status::unavailable();
        }
        auto *catalog = domain->catalog->protection();
        auto  status  = catalog->check_read_owner();
        if (!status.ok()) {
            return status;
        }
        const auto requested = loaded_current ? generation : 0;
        status               = catalog->load_root_owned(domain->tree, requested, &bytes);
        if (status.code() == Code::kNotFound && generation == 0) {
            absent = true;
            done   = true;
            return Status::Ok();
        }
        if (!status.ok()) {
            return status;
        }
        root.indexes.clear();
        if (bytes.size() >= 4 && frame_u32(bytes.data(), 0) == detail::kChunkManifestFormat) {
            return Status::not_supported(
                "legacy Chunk root requires explicit migrate_legacy_chunk while writes are stopped");
        }
        status = decode_chunk_root(bytes, domain->tree, requested, &root);
        if (status.ok() && !loaded_current) {
            latest_generation = root.identity.root_generation;
            loaded_current    = true;
            if (generation != 0 && generation != latest_generation) {
                return Status::Ok();
            }
        }
        if (status.ok()) {
            status = catalog->check_read_owner();
        }
        done = status.ok();
        return status;
    }
};

Status open_credit(ChunkOpen &open)
{
    using namespace detail;
    const auto &options = open.binding->options();
    if (options.limits.routing_bytes <= options.lifecycle_bytes) {
        return Status::resource_exhausted();
    }
    auto bytes = ResourceBudget::try_reserve(open.binding->account(), BudgetClass::kRouting,
                                             ResourceBudget::available(open.binding->account(), BudgetClass::kRouting));
    auto io    = ResourceBudget::try_reserve(open.binding->account(), BudgetClass::kIo, 1);
    if (!bytes || !io) {
        return Status::resource_exhausted();
    }
    open.budget.bytes = AllocationReservation::create(std::move(*bytes));
    open.budget.io    = reserved_shared<AllocationReservation>(open.budget.reservation(), std::move(*io));
    if (!open.lifetime) {
        return Status::internal_error();
    }
    auto status = open.lifetime->acquire(open.budget.reservation(), &open.ticket);
    if (status.ok()) {
        open.store = open.ticket->backend(open.backend->store.get());
    }
    return status;
}

template <class Protocol> Status run_protocol(ChunkOpen &open, const std::shared_ptr<Protocol> &protocol)
{
    std::shared_ptr<ChunkProtocolOperation> operation;
    auto                                    status = ChunkProtocolOperation::create(
        open.store, open.binding->cpu(), open.binding->lane(), open.budget, protocol,
        [protocol](bool stopped) { return protocol->step(stopped); }, [protocol] { return protocol->done(); },
        &operation);
    if (status.ok()) {
        status = detail::wait_snapshot_operation(operation);
    }
    return status;
}

Status load_root(ChunkOpen &open, std::shared_ptr<RootLoad> *out)
{
    auto root        = detail::reserved_shared<RootLoad>(open.budget.reservation(), open.budget.reservation());
    root->domain     = open.backend->domain;
    root->generation = open.backend->facade->options().open_generation;
    std::shared_ptr<ChunkProtocolOperation> operation;
    auto                                    status = ChunkProtocolOperation::create(
        open.store, open.binding->cpu(), open.binding->lane(), open.budget, root,
        [root](bool stopped) { return root->step(stopped); }, [root] { return root->done; }, &operation);
    if (status.ok()) {
        status = detail::wait_snapshot_operation(operation);
    }
    if (status.ok()) {
        *out = std::move(root);
    }
    return status;
}

Status recover_chunk(ChunkOpen &open, const Config &config, std::shared_ptr<const PublishedTreeState> *initial)
{
    auto status = open_credit(open);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<RootLoad> loaded;
    status = load_root(open, &loaded);
    if (!status.ok() || loaded->absent) {
        return status;
    }
    const auto          identity = loaded->root.identity;
    ChunkRecoveryLimits limits;
    limits.max_frame_bytes = std::max(limits.max_frame_bytes, physical_frame_capacity(config));
    limits.max_blob_bytes  = round_up_to_iu(limits.max_frame_bytes + kDurableBlobHeader, open.backend->iu_size);
    const auto metadata    = ChunkRootRecovery::allocation_bound(loaded->root);
    const auto window      = ChunkRootRecovery::window_allocation_bound(limits);
    if (!metadata || !window || *metadata > open.budget.reservation()->available() ||
        *window > open.budget.reservation()->available() - *metadata) {
        return Status::resource_exhausted();
    }
    std::shared_ptr<ChunkRootRecovery> recovery;
    status = ChunkRootRecovery::create(std::move(loaded->root), open.backend->domain, open.backend->ids, limits,
                                       open.budget.reservation(), open.budget.reservation(), &recovery);
    loaded.reset();
    if (!status.ok()) {
        return status;
    }
    status = run_protocol(open, recovery);
    if (!status.ok()) {
        return status;
    }
    PageLoadLimits loads;
    loads.max_frame_bytes      = limits.max_frame_bytes;
    loads.max_blob_bytes       = limits.max_blob_bytes;
    loads.max_chunk_blob_bytes = limits.max_blob_bytes;
    auto loader = detail::reserved_shared<AsyncStorePageLoader>(open.budget.reservation(), open.store,
                                                                open.binding->cpu(), open.binding->lane(), loads,
                                                                OverflowBackingResolver{}, open.budget);
    std::shared_ptr<RecoveredStateOperation> recovered;
    status = RecoveredStateOperation::create(recovery->pages(), loader, open.binding->cpu(), open.binding->lane(), {},
                                             open.budget.reservation(), &recovered);
    if (!status.ok()) {
        return status;
    }
    status = detail::wait_snapshot_operation(recovered);
    if (status.ok()) {
        *initial                      = recovered->result().state;
        open.backend->max_frame_bytes = recovery->max_frame_bytes();
        open.backend->max_blob_bytes  = recovery->max_blob_bytes();
        open.backend->recovered(identity, recovery->take_manifest());
    }
    return status;
}
} // namespace

Status TreeCore::open_chunk(const Config &config, std::shared_ptr<TreeCore> *out)
{
    return open_chunk_migration(config, {}, 0, out);
}

Status TreeCore::open_chunk_migration(const Config &config, const std::shared_ptr<detail::LegacyChunkReadStore> &legacy,
                                      uint64_t snapshot_sequence, std::shared_ptr<TreeCore> *out)
{
    auto *facade = dynamic_cast<detail::ChunkRuntimePageStore *>(config.page_store);
    if (facade == nullptr || !facade->supported() || !config.backend_owner || config.async_page_store != nullptr) {
        return Status::invalid_argument();
    }
    if (!config.runtime || !facade->matches_host(TreeRuntimeAccess::lifecycle(*config.runtime).get())) {
        return Status::invalid_argument();
    }
    if (legacy && (legacy->catalog() != facade->catalog() || legacy->transport() != facade->transport() ||
                   facade->options().open_generation != 0)) {
        return Status::invalid_argument("migration must replace the same current Chunk root");
    }
    auto status = config.key_range.validate();
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<TreeRuntimeBinding> binding;
    status = TreeRuntimeBinding::create(config.runtime, config.runtime_binding, &binding);
    if (!status.ok()) {
        return status;
    }
    auto chunk           = detail::reserved_shared<ChunkBackendState>(binding->lifecycle_budget());
    chunk->facade        = facade;
    chunk->backend_owner = config.backend_owner;
    chunk->iu_size       = facade->iu_size();
    if (!facade->try_claim_runtime_domain()) {
        return Status::unavailable();
    }
    chunk->owns_writer_domain = true;
    status = facade->make_domain(detail::allocate_backend_identity(), binding->lifecycle_budget(), &chunk->domain);
    if (!status.ok()) {
        return status;
    }
    status = facade->prepare_writer(binding->lifecycle_budget());
    if (!status.ok()) {
        return status;
    }
    chunk->writer_session = facade->take_writer();
    status = detail::ChunkRuntimeIds::create(chunk->domain->epoch, binding->lifecycle_budget(), &chunk->ids);
    if (!status.ok()) {
        return status;
    }
    status = ScheduledPageStore::create(facade, nullptr, config.backend_owner, binding, binding->lifecycle_budget(),
                                        binding->backend_lane(), &chunk->store);
    if (!status.ok()) {
        return status;
    }
    ChunkOpen                                 open(binding, chunk);
    std::shared_ptr<const PublishedTreeState> initial;
    if (legacy) {
        chunk->root_generation.store(legacy->generation(), std::memory_order_relaxed);
        chunk->snapshot_sequence.store(snapshot_sequence, std::memory_order_relaxed);
        facade->request_wal_offset(legacy->wal_offset());
    }
    else {
        status = recover_chunk(open, config, &initial);
    }
    if (!status.ok()) {
        return status;
    }
    // Recovery owns its completed mapping allocations, but its unused startup
    // reserve must be returned before the live replay/control window is prepaid.
    open.finish();
    return create(config, binding, {.initial = std::move(initial), .local = {}, .chunk = std::move(chunk)}, out);
}
} // namespace crowdb::tree
