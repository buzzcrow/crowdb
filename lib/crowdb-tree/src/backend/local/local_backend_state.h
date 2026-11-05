// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/local_checkpoint.h"

namespace crowdb::tree
{
class PageStore;

// The claim outlives registry/Core owners when a detached snapshot still
// reads protected extents. The backend owner is released after the claim.
class LocalBackendDomain
{
  public:
    static Status claim(PageStore *store, std::shared_ptr<const void> backend_owner,
                        const std::shared_ptr<detail::AllocationReservation> &metadata,
                        std::shared_ptr<LocalBackendDomain>                  *out);
    ~LocalBackendDomain();
    LocalBackendDomain(const LocalBackendDomain &)            = delete;
    LocalBackendDomain &operator=(const LocalBackendDomain &) = delete;

  private:
    explicit LocalBackendDomain(std::shared_ptr<const void> owner) : backend_owner_(std::move(owner))
    {
    }

    std::shared_ptr<const void> backend_owner_;
    PageStore                  *store_ = nullptr;
};

// One allocation authority for a local Tree lifetime. The registry retains
// both anchors and an uncertain candidate; live backing and I/O owners retain
// their own extents independently. Legacy allocation must not use this domain.
struct LocalBackendState
{
    LocalBackendState()                                                           = default;
    ~LocalBackendState()                                                          = default;
    LocalBackendState(const LocalBackendState &)                                  = delete;
    LocalBackendState                       &operator=(const LocalBackendState &) = delete;
    std::shared_ptr<AsyncPageStore>          store;
    std::shared_ptr<ExtentLedger>            ledger;
    std::shared_ptr<LocalCheckpointRegistry> checkpoints;
    // Startup-only recovered descriptors, consumed while sizing Core loaders.
    // Live primary backings retain their exact typed overflow dependencies.
    std::shared_ptr<const CapturedDurablePages> inherited;
    // Set only after claiming the PageStore runtime allocation domain.
    std::shared_ptr<LocalBackendDomain> domain;
    std::atomic<uint64_t>               gc_cursor{0};
    std::atomic<bool>                   gc_active{false};
};
} // namespace crowdb::tree
