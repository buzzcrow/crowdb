// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"
#include "crowdb-tree/memtable/memtable.h"
#include "source_tail.h"
#include "source_write_budget.h"
#include "stdexec_adapter.h"

#include <memory>

namespace crowdb::tree
{
struct PublishedTreeState;

struct SourceCatalog
{

    detail::RetirementObserver retirement;
    static constexpr bool      observe_retirement = true;

    bool retire_step() noexcept
    {
        for (size_t i = 0; i < 64 && !tables.empty(); ++i) {
            tables.pop_back();
        }
        return tables.empty();
    }

    using TableAllocator = detail::ReservationAllocator<std::shared_ptr<MemTable>>;
    using Tables         = std::vector<std::shared_ptr<MemTable>, TableAllocator>;

    [[nodiscard]] static size_t allocation_bound() noexcept
    {
        return detail::shared_allocation_bound<SourceCatalog>();
    }

    explicit SourceCatalog(std::shared_ptr<detail::AllocationReservation> credit = {})
        : tables(TableAllocator(std::move(credit)))
    {
    }

    std::shared_ptr<SourceMemoryLedger> memory;
    Tables                              tables;
    std::shared_ptr<MemTable>           active;
    std::shared_ptr<SourceWriteBudget>  active_budget;
    uint64_t                            floor      = 0;
    uint64_t                            generation = 1;
    uint64_t                            serial     = 0;
};

enum class SourceCapturePhase : uint8_t { kCaptured, kDraining, kReady, kRepairRequired, kCovered };

class SourceCapture
{
    struct State;

  public:
    ~SourceCapture();
    SourceCapture(const SourceCapture &)            = delete;
    SourceCapture &operator=(const SourceCapture &) = delete;

    [[nodiscard]] uint64_t                                              batch_id() const noexcept;
    [[nodiscard]] uint64_t                                              floor() const noexcept;
    [[nodiscard]] uint64_t                                              frontier() const noexcept;
    [[nodiscard]] SourceCapturePhase                                    phase() const noexcept;
    [[nodiscard]] bool                                                  drained() const noexcept;
    [[nodiscard]] bool                                                  successor_active() const noexcept;
    [[nodiscard]] const std::shared_ptr<const SourceCatalog>           &expected_catalog() const noexcept;
    [[nodiscard]] const std::shared_ptr<const SourceCatalog>           &next_catalog() const noexcept;
    [[nodiscard]] const std::shared_ptr<MemTable>                      &successor() const noexcept;
    [[nodiscard]] const std::shared_ptr<detail::AllocationReservation> &admission_owner() const noexcept;

    // A stopped attempt still drains every admitted writer before terminal.
    // Reconnecting after terminal retries against the same fixed P/F/sources.
    [[nodiscard]] detail::CallbackSender drain_sender() noexcept;
    void                                 request_stop() noexcept;

  private:
    friend class SourceAdmission;

    explicit SourceCapture(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static void            submit(void *source, detail::CallbackComplete complete, void *context);
    std::shared_ptr<State> state_;
};

// Host-side write source selection. It deliberately does not publish Tree
// mapping/coverage: those belong to the common publication transaction.
class SourceAdmission
{
    struct State;

  public:
    class Rotation
    {
        struct Data;

      public:
        ~Rotation();
        Rotation(const Rotation &)                                                            = delete;
        Rotation                                                 &operator=(const Rotation &) = delete;
        [[nodiscard]] const std::shared_ptr<const SourceCatalog> &expected() const noexcept;
        [[nodiscard]] const std::shared_ptr<SourceCatalog>       &next() const noexcept;

      private:
        friend class SourceAdmission;
        explicit Rotation(detail::ReservedUnique<Data> data);
        detail::ReservedUnique<Data> data_;
    };

    class Coverage
    {
        struct Data;

      public:
        ~Coverage();
        Coverage(const Coverage &)            = delete;
        Coverage &operator=(const Coverage &) = delete;

      private:
        friend class SourceAdmission;
        explicit Coverage(detail::ReservedUnique<Data> data);
        detail::ReservedUnique<Data> data_;
    };

    // Binding a reservation enters thread-bound epoch guards. Keep the Writer
    // on that apply thread; transfer SourceWriteReservation between threads.
    class Writer
    {
      public:
        class Prepared
        {
          public:
            Prepared()                                = default;
            Prepared(Prepared &&) noexcept            = default;
            Prepared &operator=(Prepared &&) noexcept = default;
            Prepared(const Prepared &)                = delete;
            Prepared &operator=(const Prepared &)     = delete;

          private:
            friend class Writer;
            std::shared_ptr<MemTable> table_;
            MemTable::PreparedWrite   write_;
            WriteFootprint            footprint_;
        };

        Writer() = default;
        ~Writer();
        Writer(Writer &&other) noexcept;
        Writer &operator=(Writer &&other) noexcept;
        Writer(const Writer &)            = delete;
        Writer &operator=(const Writer &) = delete;

        [[nodiscard]] const std::shared_ptr<MemTable> &table() const noexcept
        {
            return table_;
        }

        [[nodiscard]] uint64_t bound() const noexcept
        {
            return bound_;
        }

        // Whole-batch payload staging consumes the already admitted credit.
        // Return its temporary lease before prepare_write adopts those bytes.
        [[nodiscard]] const std::shared_ptr<detail::AllocationReservation> &allocation_budget() const noexcept
        {
            return credit_;
        }

        void release() noexcept;

        // A permit is used by one apply caller. It validates the admitted
        // footprint before moving an input or changing the source.
        Status upsert(Slice key, uint64_t slot, buffer value, bool *changed = nullptr);
        Status upsert_external(Slice key, uint64_t slot, uint8_t flags, buffer value, bool *changed = nullptr);

        // Prepare the complete batch before assigning an automatic slot. A
        // failure abandons the batch and its permit, returning unused source
        // capacity. The buffer is an encoded owned cell or a charged split
        // external value; all preparation is thread-bound.
        Status prepare_write(Slice key, uint8_t flags, buffer value, Prepared *out);
        bool   commit_write(Prepared write, uint64_t slot) noexcept;

      private:
        friend class SourceAdmission;
        Status apply(Slice key, uint64_t slot, uint8_t flags, buffer value, bool split, bool *changed);

        Writer(std::shared_ptr<MemTable> table, uint64_t bound) : table_(std::move(table)), bound_(bound)
        {
        }

        SourceTailDomain::Observation                  observation_;
        std::shared_ptr<MemTable>                      table_;
        uint64_t                                       bound_ = 0;
        std::shared_ptr<SourceWriteBudget>             source_budget_;
        std::shared_ptr<detail::AllocationReservation> credit_;
        WriteFootprint                                 remaining_;
        WriteFootprint                                 planned_;
        detail::BudgetClass                            category_ = detail::BudgetClass::kAdmittedWriter;
    };

    // Keep lifecycle credit unsealed for bounded participant registration on
    // host threads. Exhaustion rejects ingress before a source is modified.
    explicit SourceAdmission(uint64_t floor = 0, uint64_t generation = 1, SourceWriteOptions options = {},
                             std::shared_ptr<detail::AllocationReservation> lifecycle = {});
    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t max_threads = 64) noexcept;
    ~SourceAdmission();
    SourceAdmission(const SourceAdmission &)            = delete;
    SourceAdmission &operator=(const SourceAdmission &) = delete;

    [[nodiscard]] std::shared_ptr<const SourceCatalog>     catalog() const;
    [[nodiscard]] uint64_t                                 identity() const noexcept;
    [[nodiscard]] const std::shared_ptr<SourceTailDomain> &tail_domain() const noexcept;
    [[nodiscard]] static std::optional<size_t>             capture_allocation_bound(size_t sources) noexcept;
    // bound is the host's completed contiguous frontier. Completion tracking
    // and generation fencing remain host responsibilities. An unavailable
    // result needs async admission/retry, never a worker spin loop.
    [[nodiscard]] Status try_admit(uint64_t bound, Writer *out);
    // A client can become a gap by the time consensus chooses it. Select the
    // current source counters independently of its prepaid memory partition;
    // moving the permit never reserves or charges its allocation bytes again.
    [[nodiscard]] Status try_admit_reserved(uint64_t bound, SourceWriteReservation *reservation, Writer *out,
                                            std::optional<detail::BudgetClass> source_category = {});
    [[nodiscard]] Status try_admit(uint64_t bound, WriteFootprint footprint, Writer *out, detail::BudgetClass category,
                                   bool *paused);
    [[nodiscard]] Status try_admit(uint64_t bound, WriteFootprint footprint, Writer *out,
                                   detail::BudgetClass category = detail::BudgetClass::kAdmittedWriter);

    // Requires the already-authorized whole-batch metadata credit owner.
    // All allocations precede closure. Existing repair captures are returned
    // unchanged, rather than closing another active table or widening F.
    [[nodiscard]] Status try_capture(const std::atomic<uint64_t>                   &contiguous,
                                     std::shared_ptr<detail::AllocationReservation> admission_owner,
                                     std::shared_ptr<SourceCapture>                *out,
                                     std::shared_ptr<const SourceCatalog>           expected_catalog = {});
    // Call only after the Tree publication makes next_catalog's complete
    // source membership visible to readers. Before this acknowledgement,
    // successor writes are rejected, so no read can miss a new source.
    // An accepted capture can still activate after close(); new writers remain
    // rejected, while the host can finish draining its accepted flush.
    [[nodiscard]] Status activate_successor(const std::shared_ptr<SourceCapture> &capture, uint64_t batch_id);
    [[nodiscard]] Status mark_repair(const std::shared_ptr<SourceCapture> &capture, uint64_t batch_id,
                                     uint64_t actual_coverage);
    // Call only after the atomic mapping/source coverage commit. It accepts
    // exactly this capture's F, never a caller-supplied larger frontier.
    [[nodiscard]] Status finish_covered(const std::shared_ptr<SourceCapture> &capture, uint64_t batch_id,
                                        uint64_t actual_coverage);
    // Split handoff covers inherited sources while the child's own active
    // table keeps later slots. Allocate its new catalog before publication;
    // installation only acknowledges the matching immutable covered state.
    [[nodiscard]] static std::optional<size_t> coverage_allocation_bound(size_t sources) noexcept;
    Status prepare_coverage(uint64_t frontier, const std::shared_ptr<detail::AllocationReservation> &metadata,
                            std::shared_ptr<Coverage> *out);
    Status install_coverage(const std::shared_ptr<Coverage> &prepared, const PublishedTreeState &published);
    [[nodiscard]] static std::optional<size_t> rotation_allocation_bound(size_t sources) noexcept;
    // Prepares all successor owners without closing the active source. The
    // host publishes next() membership before acknowledging installation.
    Status prepare_rotation(const std::shared_ptr<detail::AllocationReservation> &metadata,
                            const std::shared_ptr<const SourceCatalog> &expected, std::shared_ptr<Rotation> *out);
    Status activate_rotation(const std::shared_ptr<Rotation> &rotation);
    Status abort_rotation(const std::shared_ptr<Rotation> &rotation);
    // Close ingress. Accepted Writer/Capture owners remain valid and must be
    // drained by the host before it tears down external epoch/runtime state.
    void   close() noexcept;
    size_t reclaim();

  private:
    detail::ReservedUnique<State> state_;
};

} // namespace crowdb::tree
