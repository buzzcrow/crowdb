// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/apply_dispatcher.h"
#include "btree/apply_frontier.h"
#include "btree/apply_progress.h"
#include "btree/apply_waiters.h"
#include "btree/replay_controls.h"
#include "btree/source_rotation.h"
#include "btree/tree_core.h"
#include "btree/tree_core_split_internal.h"
#include "snapshot/checkpoint_operation.h"

namespace crowdb::tree
{
struct TreeCore::State
{
    static constexpr uint64_t kClosed       = uint64_t{1} << 63;
    static constexpr uint64_t kReplacing    = uint64_t{1} << 62;
    static constexpr uint64_t kPrepared     = uint64_t{1} << 61;
    static constexpr uint64_t kParticipants = ~(kClosed | kReplacing | kPrepared);

    State(Config options, std::shared_ptr<TreeRuntimeBinding> host);
    ~State();
    Status initialize(TreeCoreBootstrap bootstrap);
    Status capture_split_sources(TreeCore &core, uint64_t target);
    Status install_split_sources(const SplitSourceSession &session, uint64_t identity, uint64_t end);
    Status prepare_split_handoff(const SplitSourceSession &session, uint64_t identity, uint64_t end);
    bool   enter() noexcept;
    void   leave() noexcept;
    void   join_ingress() noexcept;
    bool   begin_replacement() noexcept;
    void   finish_replacement() noexcept;
    void   drain_ingress() noexcept;
    static std::shared_ptr<PublishedTreeState>
    empty_state(const std::shared_ptr<detail::AllocationReservation> &credit);

    struct Ingress
    {
        explicit Ingress(State &state) : owner(state.enter() ? &state : nullptr)
        {
        }

        ~Ingress()
        {
            if (owner != nullptr) {
                owner->leave();
            }
        }

        State *owner;
    };

    CheckpointDiagnostics                          checkpoint_diagnostics;
    Config                                         config;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<detail::AllocationReservation> source_lifecycle;
    std::shared_ptr<TreeResourceMetrics>           resources;
    std::shared_ptr<TreeWorkMetrics>               work;
    std::shared_ptr<SharedPageCache>               cache;
    std::shared_ptr<LocalBackendState>             local;
    std::shared_ptr<CheckpointBackend>             chunk;
    std::shared_ptr<AsyncPageStore>                backend;
    std::shared_ptr<ApplyFrontier>                 frontier;
    detail::ReservedUnique<TreeOperationLifetime>  lifetime;
    detail::ReservedUnique<TreeOperationLifetime>  apply_lifetime;
    detail::ReservedUnique<TreeOperationLifetime>  replay_lifetime;
    std::shared_ptr<ReplayControls>                replay_controls;
    std::shared_ptr<ApplyWaiters>                  replay_waiters;
    std::shared_ptr<ApplyProgress>                 apply_progress;
    std::shared_ptr<ApplyDispatcher>               apply_dispatcher;
    std::shared_ptr<SourceRotation>                source_rotation;
    detail::ReservedUnique<SourceAdmission>        sources;
    detail::ReservedUnique<PublicationCoordinator> publisher;
    detail::ReservedUnique<FlushController>        flush;
    PageLoadLimits                                 loads;
    PublishedReadLimits                            reads;
    FlushPreparationOptions                        preparation;
    std::atomic<uint64_t>                          ingress{0};
    std::atomic<uint64_t>                          automatic_slot{0};
    std::atomic<uint8_t>                           close_state{0};
    std::atomic_flag                               split_busy = ATOMIC_FLAG_INIT;
    std::shared_ptr<SplitSourceSession>            split_sources;
    std::shared_ptr<SplitHandoff>                  split_handoff;
    uint64_t                                       split_sequence = 0;
#ifdef CROWDB_TREE_TEST_UTIL
    std::function<void()> before_generation_install_for_tests;
#endif
};
} // namespace crowdb::tree
