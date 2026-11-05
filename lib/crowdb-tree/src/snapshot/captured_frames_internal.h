// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/apply_dispatcher.h"
#include "captured_frames.h"

namespace crowdb::tree
{
struct CapturedFrameStream::State : std::enable_shared_from_this<State>
{
    struct Path
    {
        uint64_t                             pid;
        std::shared_ptr<const PublishedPage> page;
        PageKeyRange                         range;
        size_t                               child = 0;
        InnerEncodeFences                    fences;
        bool                                 validated = false;
    };

    struct Load
    {
        std::atomic<bool>                     claimed{false};
        std::atomic<bool>                     ready{false};
        Status                                status;
        size_t                                base     = 0;
        bool                                  is_inner = false;
        std::shared_ptr<const LeafBase>       leaf;
        std::shared_ptr<const InnerPageState> inner;
    };

    State(std::shared_ptr<const PublishedTreeState> captured, std::shared_ptr<PageLoader> transport,
          std::shared_ptr<detail::FlushRuntime> cpu, std::shared_ptr<detail::FlushRuntime::Lane> queue,
          CapturedFrameLimits bounds, ReserveOverflow reserve);
    void        start(detail::CallbackComplete callback, void *receiver) noexcept;
    void        wake() noexcept;
    static void run(void *context) noexcept;
    void        pump();
    void        step();
    bool        initialize();
    void        push(uint64_t pid, PageKeyRange range, std::optional<uint32_t> expected_level);
    void        validate(Path &path);
    void        prepare();
    void        prepare_encoder();
    void        emit();
    void        plan();
    void        completed_page(bool nonempty_leaf);
    bool        reuse_primary();
    void        load_base(const BaseLoadRequest &request);
    void        load_inner();
    void        loaded();
    void        finish(Status status, bool fatal = false) noexcept;

    std::shared_ptr<const PublishedTreeState>   view;
    std::optional<PageEncodeInput>              single_page;
    std::shared_ptr<PageLoader>                 loader;
    std::shared_ptr<detail::FlushRuntime>       runtime;
    std::shared_ptr<detail::FlushRuntime::Lane> lane;
    CapturedFrameLimits                         limits;
    ReserveOverflow                             reserve_overflow;
    detail::ReservedVector<Path>                stack;
    std::shared_ptr<CapturedFrameBuffer>        buffer;
    detail::ReservedUnique<FinalPageEncoder>    encoder;
    std::shared_ptr<Load>                       load;
    std::shared_ptr<State>                      lifetime;
    std::shared_ptr<State>                      queued_owner;
    ApplyDispatcher::Node                       cpu_node{&State::run, this};
    CapturedFrameResult                         report;
    CapturedFrameStats                          counters;
    uint64_t                                    overflow_end = 0;
    Status                                      failure;
    bool                                        initialized   = false;
    bool                                        encoder_ready = false;
    bool                                        begin_pending = false;
    std::atomic<bool>                           stopped{false};
    std::atomic<bool>                           scheduled{false};
    std::atomic<bool>                           wake_requested{false};
    std::atomic<uint8_t>                        lifecycle{0};
    detail::CallbackComplete                    complete = nullptr;
    void                                       *context  = nullptr;
};
} // namespace crowdb::tree
