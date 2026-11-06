// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/block_account.h"
#include "backend/local/local_sparse_gc.h"
#include "btree/allocation_bound.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "crowdb-common/log.h"

#include <cassert>

namespace crowdb::tree::checkpoint_detail
{
namespace
{
struct Cleanup : std::enable_shared_from_this<Cleanup>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<Cleanup> owner;

        void set_value() noexcept
        {
            auto keep = std::move(owner);
            keep->completed(Status::Ok());
        }

        void set_error(Status status) noexcept
        {
            auto keep = std::move(owner);
            keep->completed(std::move(status));
        }

        void set_stopped() noexcept
        {
            auto keep = std::move(owner);
            keep->completed(Status::unavailable());
        }
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<Cleanup> owner)
            : connected(stdexec::connect(sender, Receiver{.owner = std::move(owner)}))
        {
        }

        Connected connected;
    };

    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<AllocationBudget>              backend;
    std::shared_ptr<LocalBackendState>             local;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    TreeCore                                      *core = nullptr;
    std::shared_ptr<Cleanup>                       self;
    bool                                           owns_gate = false;
    ProtectedExtent                                exclusion;
    std::shared_ptr<CheckpointOperation>           operation;
    std::optional<Attempt>                         attempt;
    Status                                         outcome;
    size_t                                         remaining = 2;

    void completed(Status status) noexcept
    {
        outcome = std::move(status);
        self    = weak_from_this().lock();
        assert(self);
        Status submitted;
        try {
            submitted = binding->backend_cpu()->try_submit(binding->backend_lane(), &Cleanup::run, this);
        }
        catch (...) {
            submitted = Status::resource_exhausted();
        }
        if (!submitted.ok()) {
            self.reset();
        }
    }

    void advance()
    {
        if (auto *memory = dynamic_cast<MemPageStore *>(core->config().page_store)) {
            if (!outcome.ok()) {
                return;
            }
            const auto result =
                collect_free_local_blocks(*memory, *local->ledger, local->gc_cursor.load(), {}, metadata);
            local->gc_cursor.store(result.complete ? 0 : result.next_block, std::memory_order_relaxed);
            if (result.status.ok() && !result.complete) {
                completed(Status::Ok());
            }
            return;
        }
        auto *store = dynamic_cast<BlockPageStore *>(core->config().page_store);
        if (store == nullptr || store->block_size() == 0 || !outcome.ok()) {
            return;
        }
        attempt.reset();
        operation.reset();
        if (!exclusion.owner) {
            if (local->checkpoints->needs_anchor_retirement()) {
                auto options                            = defaults(*core);
                options.cause                           = TreeWorkCause::GarbageCollection;
                options.metadata.retire_previous_anchor = true;
                const auto status                       = core->prepare_checkpoint(options, &operation);
                if (!status.ok()) {
                    return;
                }
                attempt.emplace(operation->sender(), shared_from_this());
                stdexec::start(attempt->connected);
                return;
            }
            core->publisher().reclaim();
            core->binding()->account()->drain_retirements();
            reclaim_free_blocks(*core, metadata, backend);
            auto candidate = select_sparse_local_block(
                *store, *local->ledger, local->gc_cursor.load(std::memory_order_relaxed), metadata, 0.5, backend);
            local->gc_cursor.store(candidate.next_block, std::memory_order_relaxed);
            if (!candidate.status.ok()) {
                return;
            }
            exclusion = std::move(candidate.exclusion);
        }
        if (remaining == 0) {
            core->cache()->evict();
            // Quiescent publication roots can otherwise keep old extents
            // until the epoch batch reaches its normal collection threshold.
            core->publisher().reclaim();
            const LocalBlockGcLimits limits{.max_blocks = 1, .max_deleted_bytes = store->block_size()};
            (void)collect_free_local_blocks(*store, *local->ledger, exclusion.address / store->block_size(), limits,
                                            metadata, backend);
            return;
        }
        --remaining;
        auto options                   = defaults(*core);
        options.cause                  = TreeWorkCause::GarbageCollection;
        options.write.frames.exclusion = {.address = exclusion.address, .length = exclusion.length};
        auto status                    = core->prepare_checkpoint(options, &operation);
        if (!status.ok()) {
            return;
        }
        attempt.emplace(operation->sender(), shared_from_this());
        stdexec::start(attempt->connected);
    }

    ~Cleanup()
    {
        detail::seal_backend_budget(backend);
        if (owns_gate) {
            local->gc_active.store(false, std::memory_order_release);
        }
        if (metadata) {
            metadata->seal();
        }
    }

    static void run(void *context) noexcept
    {
        auto owner = std::move(static_cast<Cleanup *>(context)->self);
        // The accepted checkpoint's Ticket keeps Core/backend lifetime valid
        // while close or generation replacement drains this continuation.
        try {
            owner->advance();
        }
        catch (const std::bad_alloc &) {
            // The existing anchors and live backing still protect their bytes.
            return;
        }
        catch (...) {
            CRB_LOG_WARN("sparse block cleanup deferred after an exception");
        }
    }
};
} // namespace

void schedule_free_blocks(TreeCore &core, std::shared_ptr<TreeOperationLifetime::Ticket> ticket) noexcept
try {
    auto                     local = core.local_backend();
    auto                    *store = core.config().page_store;
    const LocalBlockGcLimits limits;
    const auto               unit = store != nullptr ? local_block_gc_unit(*store) : 0;
    if (!ticket || !local || !local->ledger || unit == 0 || unit > limits.max_deleted_bytes ||
        local->gc_active.exchange(true, std::memory_order_acq_rel)) {
        return;
    }

    struct Gate
    {
        LocalBackendState *local;

        ~Gate()
        {
            if (local != nullptr) {
                local->gc_active.store(false, std::memory_order_release);
            }
        }
    } gate{local.get()};

    using namespace detail;
    const auto bound = AllocationBound(local_block_gc_metadata_bound(limits).value_or(SIZE_MAX))
                           .add(shared_allocation_bound<Cleanup>() + shared_allocation_bound<AllocationReservation>())
                           .value();
    if (!bound) {
        return;
    }
    auto reserved = ResourceBudget::try_reserve(core.binding()->account(), BudgetClass::kReclaim, *bound);
    if (!reserved) {
        return;
    }
    auto metadata     = AllocationReservation::create(std::move(*reserved));
    auto cleanup      = reserved_shared<Cleanup>(metadata);
    cleanup->metadata = std::move(metadata);
    auto backend_status =
        reserve_backend_operation(store, core.binding()->account(),
                                  {.workers = core.binding()->backend_cpu()->worker_count()}, &cleanup->backend);
    if (!backend_status.ok()) {
        return;
    }
    cleanup->local     = std::move(local);
    cleanup->binding   = core.binding();
    cleanup->ticket    = std::move(ticket);
    cleanup->core      = &core;
    cleanup->owns_gate = true;
    gate.local         = nullptr;
    cleanup->self      = cleanup;
    Status status;
    try {
        status =
            cleanup->binding->backend_cpu()->try_submit(cleanup->binding->backend_lane(), &Cleanup::run, cleanup.get());
    }
    catch (...) {
        cleanup->self.reset();
        throw;
    }
    if (!status.ok()) {
        cleanup->self.reset();
    }
}
catch (const std::bad_alloc &) {
    // This checkpoint is already durable; a later pass can retry admission.
    return;
}
catch (const std::exception &error) {
    CRB_LOG_WARN("post-checkpoint cleanup submission deferred: {}", error.what());
}
catch (...) {
    CRB_LOG_WARN("post-checkpoint cleanup submission deferred after unknown exception");
}
} // namespace crowdb::tree::checkpoint_detail
