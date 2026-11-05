// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "flush_reservation.h"

namespace crowdb::tree::detail
{
struct ReclaimAdmission
{
    PublicationCoordinator                  *publisher;
    std::shared_ptr<ResourceBudget::Account> account;
    PageMaintenanceOperationOptions          options;
    Status acquire_window(const MappingView &mapping, MaintenancePublicationOptions *publication,
                          std::shared_ptr<AllocationReservation> *out) const;
};
} // namespace crowdb::tree::detail
