// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/local_checkpoint.h"

namespace crowdb::tree
{
struct LocalRecoveryLimits
{
    bool   accept_legacy_chunk_refs = false;
    size_t max_segments             = 1U << 16;
    size_t max_frames               = 1024 * 1024;
    size_t max_blob_bytes           = 1024 * 1024;
    size_t max_frame_bytes          = 1024 * 1024;
    size_t max_overflow_references  = 1024 * 1024;
    size_t max_dependency_steps     = 1024 * 1024;
};

struct LocalRecoveryResult
{
    Status                                                    status;
    std::shared_ptr<ExtentLedger>                             ledger;
    std::shared_ptr<LocalCheckpointRegistry>                  registry;
    std::array<std::shared_ptr<const ProtectedCheckpoint>, 2> anchors;
    std::shared_ptr<const CapturedDurablePages>               latest;
};

// Startup-only bootstrap of an exclusive local allocation domain. Every
// retained anchor's metadata and page/overflow extents are protected before
// enabling allocation. Frames are checked through bounded windows to rebuild
// precise overflow dependencies; raw payloads are not retained. Routing/live
// publication construction is a separate consumer of the recovered mapping.
class LocalRecoveryOperation
{
    struct State;

  public:
    static Status create(std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, ExtentLedgerLimits domain,
                         LocalRecoveryLimits limits, PageLoadBudget budget,
                         std::shared_ptr<LocalRecoveryOperation> *out);
    // Valid only when the exclusive domain is shorter than the first anchor
    // slot. The normal recovery still validates and initializes that domain.
    [[nodiscard]] static std::optional<size_t> empty_allocation_bound(LocalRecoveryLimits limits, uint32_t iu) noexcept;
    [[nodiscard]] detail::CallbackSender       sender() noexcept;
    [[nodiscard]] const LocalRecoveryResult   &result() const noexcept;
    void                                       request_stop() noexcept;

  private:
    explicit LocalRecoveryOperation(std::shared_ptr<State> state);

    static void                          submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State>               state_;
    std::shared_ptr<LocalRecoveryResult> result_;
};
} // namespace crowdb::tree
