// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/apply_dispatcher.h"
#include "btree/overflow_backing.h"
#include "snapshot/captured_write.h"
#include "snapshot/native_frame_read.h"

namespace crowdb::tree
{
namespace captured_write_detail
{
[[nodiscard]] bool same_durable_frame(const DurableCapturedFrame &left, const DurableCapturedFrame &right) noexcept;
}

struct CapturedWriteOperation::State : std::enable_shared_from_this<State>
{
    enum class Stage : uint8_t { Next, Frame, Dependencies, Write, ResidentWrite, RelocateRead };

    struct Dependencies
    {
        bool retire_step() noexcept
        {
            for (size_t i = 0; i < 64 && !owners.empty(); ++i) {
                owners.pop_back();
            }
            return owners.empty();
        }

        explicit Dependencies(const std::shared_ptr<detail::AllocationReservation> &metadata)
            : owners(detail::ReservationAllocator<std::shared_ptr<const void>>(metadata))
        {
        }

        std::shared_ptr<const void>                         ids;
        detail::ReservedVector<std::shared_ptr<const void>> owners;
    };

    struct ResidentSource
    {
        std::shared_ptr<const BackingRef> backing;
        std::shared_ptr<Dependencies>     dependencies;
    };

    using ResidentEntry = std::pair<const uint64_t, ResidentSource>;
    using ResidentSources =
        std::map<uint64_t, ResidentSource, std::less<>, detail::ReservationAllocator<ResidentEntry>>;

    explicit State(const std::shared_ptr<detail::AllocationReservation> &metadata)
        : resident_sources(std::less<>{}, detail::ReservationAllocator<ResidentEntry>(metadata))
    {
    }

    struct Reservation
    {
        PageWriteOperation::ExtentComplete complete;
        std::shared_ptr<Dependencies>      dependencies;
        std::atomic<bool>                  claimed{false};
    };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<State> owner;
        void                   set_value() noexcept;
        void                   set_error(Status status) noexcept;
        void                   set_stopped() noexcept;
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<State> owner)
            : connected(stdexec::connect(sender, Receiver{.owner = std::move(owner)}))
        {
        }

        Connected connected;
    };

    void                              start(detail::CallbackComplete callback, void *argument) noexcept;
    void                              wake() noexcept;
    static void                       run(void *context) noexcept;
    void                              pump();
    void                              begin_child(detail::CallbackSender sender, Stage next);
    void                              child_done(Status status) noexcept;
    void                              cancel_child() noexcept;
    void                              received_frame();
    void                              reuse_step();
    void                              begin_write();
    PageWriteOperation::ReserveExtent reserve_page(bool protect_dependency);
    void                              use_resident_dependency(DurableCapturedFrame value);
    void                              begin_resident_write(DurableCapturedFrame value);
    void                              received_resident_write();
    void                              begin_relocated_read(DurableCapturedFrame value);
    void                              received_relocated_read();
    void                              use_relocated_dependency(DurableCapturedFrame value);
    void                              reuse_durable_dependency(DurableCapturedFrame value);
    Status                            rebind_primary();
    void                              attach_dependency(const DurableCapturedFrame &value, bool retain_owner = true);
    void                              received_write();
    void                              finish_frame();
    Status                            insert(DurableCapturedFrame value, bool reused);
    void                              finish(Status status) noexcept;

    std::shared_ptr<AsyncPageStore>             store;
    std::shared_ptr<detail::FlushRuntime>       runtime;
    std::shared_ptr<detail::FlushRuntime::Lane> lane;
    CapturedWriteLimits                         limits;
    PageLoadBudget                              budget;
    PageWriteOperation::ReserveExtent           reserve;
    std::shared_ptr<const CapturedDurablePages> inherited;
    std::shared_ptr<CapturedDurablePages>       pages;
    std::shared_ptr<CapturedFrameStream>        stream;
    std::shared_ptr<PageWriteOperation>         write;
    std::shared_ptr<NativeFrameRead>            relocation_read;
    std::shared_ptr<Dependencies>               dependencies;
    std::shared_ptr<OverflowBackingSet>         overflow_backing;
    std::optional<CapturedFrame>                frame;
    DurableCapturedFrame                        descriptor;
    DurableCapturedFrame                        resident_dependency;
    ResidentSources                             resident_sources;
    size_t                                      dependency_index = 0;
    uint64_t                                    dependency_pid   = kInvalidPageId;
    uint64_t                                    dependency_bytes = 0;
    size_t                                      dependency_steps = 0;
    bool                                        last_frame       = false;
    Stage                                       stage            = Stage::Next;
    bool                                        waiting          = false;
    Status                                      dispatch_failure;
    Status                                      child_status;
    std::atomic<bool>                           child_ready{false};
    std::optional<Attempt>                      attempt;
    std::shared_ptr<State>                      queued_owner;
    std::atomic<uint8_t>                        lifecycle{0};
    std::atomic<bool>                           stopped{false};
    std::atomic<bool>                           scheduled{false};
    std::atomic<bool>                           wake_requested{false};
    detail::CallbackComplete                    complete = nullptr;
    void                                       *context  = nullptr;
    ApplyDispatcher::Node                       cpu_node{&State::run, this};
    CapturedWriteResult                         report;
};
} // namespace crowdb::tree
