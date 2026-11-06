// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/apply_dispatcher.h"
#include "page_writeback.h"
#include "resource_observation.h"

namespace crowdb::tree
{
struct PageWriteOperation::State : std::enable_shared_from_this<State>
{
    struct Protection
    {
        std::shared_ptr<const void> extent;
        std::shared_ptr<const void> dependencies;
    };

    State(PageWriteInput source, PageWriteLimits bounds, PageLoadBudget credits);
    void        start(detail::CallbackComplete callback, void *context) noexcept;
    static void encode(void *context) noexcept;
    static void write(void *context) noexcept;
    static void written(void *context, Status status) noexcept;
    void        prepare();
    void        reserved(Status status, ReservedWriteExtent extent) noexcept;
    void        queue_write() noexcept;
    void        submit_write();
    void        finish(Status status) noexcept;

    ApplyDispatcher::Node                                     encode_node{&State::encode, this};
    ApplyDispatcher::Node                                     write_node{&State::write, this};
    PageWriteInput                                            input;
    PageWriteLimits                                           limits;
    PageLoadBudget                                            budget;
    std::shared_ptr<detail::AllocationReservation>            chunk_credit;
    std::shared_ptr<AsyncPageStore>                           store;
    std::shared_ptr<detail::FlushRuntime>                     runtime;
    std::shared_ptr<detail::FlushRuntime::Lane>               lane;
    ReserveExtent                                             reserve;
    AllocationLease                                           io_credit;
    ResourceObservation                                       io_resources;
    detail::ReservedVector<uint8_t, AllocationKind::kIo>      blob;
    detail::ReservedVector<uint8_t, AllocationKind::kScratch> scratch;
    std::span<uint8_t>                                        aligned;
    std::shared_ptr<BackingRef>                               backing;
    std::shared_ptr<Protection>                               protection;
    PageWriteResult                                           report;
    ReservedWriteExtent                                       reserved_extent;
    Status                                                    reservation_status;
    std::atomic<bool>                                         reservation_claimed{false};
    std::atomic<unsigned>                                     reservation_events{0};
    std::shared_ptr<State>                                    pending;
    detail::CallbackComplete                                  complete = nullptr;
    void                                                     *receiver = nullptr;
    std::atomic<bool>                                         started{false};
    std::atomic<bool>                                         stopped{false};
    std::atomic<bool>                                         finished{false};
    bool                                                      io_submitted = false;
    std::atomic<bool>                                         io_complete{false};
};
} // namespace crowdb::tree
