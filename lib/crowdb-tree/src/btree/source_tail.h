// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "crowdb-tree/memtable/memtable.h"

namespace crowdb::tree
{
// A finite, independent source for a prepared split. Payloads retain their
// original allocation owner; all new nodes and cross-Tree reasons are paid by
// the destination before observation starts.
class SourceTail
{
  public:
    struct Write
    {
        std::shared_ptr<SourceTail> owner;
        MemTable::PreparedWrite     prepared;
        bool                        entered = false;
        Write()                             = default;
        Write(Write &&) noexcept;
        Write &operator=(Write &&) noexcept;
        ~Write();
        void complete(bool changed, uint64_t slot) noexcept;
    };

    SourceTail(std::shared_ptr<detail::AllocationReservation> metadata, size_t records, size_t bytes);
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t records, size_t bytes) noexcept;
    static Write prepare(const std::shared_ptr<SourceTail> &owner, const MemTable::PreparedWrite &parent) noexcept;
    // Only zero active writers may freeze the tail. Failed attempts never
    // change source admission or the destination's published generation.
    bool               freeze() noexcept;
    void               expire() noexcept;
    [[nodiscard]] bool expired() const noexcept;

    [[nodiscard]] const std::shared_ptr<MemTable> &table() const noexcept
    {
        return table_;
    }

  private:
    void                                           leave() noexcept;
    static constexpr size_t                        kStopped = size_t{1} << ((sizeof(size_t) * 8) - 1);
    static constexpr size_t                        kExpired = kStopped >> 1;
    std::atomic<size_t>                            state_{0};
    std::atomic<size_t>                            records_{0};
    std::atomic<size_t>                            bytes_{0};
    std::shared_ptr<detail::AllocationReservation> metadata_;
    std::shared_ptr<MemTable>                      table_;
    size_t                                         max_records_;
    size_t                                         max_bytes_;
};

// The source lifetime owns this domain across active-table rotations. Guards
// span a complete admitted Writer, including its frontier acknowledgement.
// Registering a tail then retiring a barrier covers pre-registration writers:
// all pre-registration, unobserved writers have acknowledged before their
// max-seen high-watermark A is read. Child replay must cover A, including gaps.
class SourceTailDomain : public std::enable_shared_from_this<SourceTailDomain>
{
    struct Registration;

  public:
    struct Observation
    {
        std::shared_ptr<SourceTailDomain> domain;
        EpochManager::Guard               guard;
        std::shared_ptr<SourceTail>       tail;
        Observation()                        = default;
        Observation(Observation &&) noexcept = default;

        Observation &operator=(Observation &&other) noexcept
        {
            if (this != &other) {
                tail.reset();
                guard = {};
                domain.reset();
                domain = std::move(other.domain);
                guard  = std::move(other.guard);
                tail   = std::move(other.tail);
            }
            return *this;
        }
    };

    struct Barrier : EpochManager::Deferred
    {
        std::shared_ptr<Barrier>      owner;
        std::shared_ptr<Registration> registration;
        std::atomic<bool>             ready{false};
        void                          wait() const noexcept;
    };

    explicit SourceTailDomain(std::shared_ptr<detail::AllocationReservation> metadata);
    ~SourceTailDomain();
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t threads) noexcept;
    [[nodiscard]] static size_t                registration_bound() noexcept;
    Observation                                observe();
    Status arm(const std::shared_ptr<SourceTail> &tail, const std::shared_ptr<detail::AllocationReservation> &metadata,
               std::shared_ptr<Barrier> *out);
    bool   detach(const std::shared_ptr<Barrier> &subscription) noexcept;
    void   close() noexcept;

  private:
    void                                           retire(Registration *entry) noexcept;
    std::shared_ptr<detail::AllocationReservation> metadata_;
    EpochManager                                   epoch_;
    std::atomic<Registration *>                    registration_{nullptr};
    std::atomic<bool>                              closed_{false};
};
} // namespace crowdb::tree
