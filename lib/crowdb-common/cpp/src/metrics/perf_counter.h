// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#ifdef __linux__
#    include <linux/perf_event.h>
#    include <sys/ioctl.h>
#    include <sys/syscall.h>
#    include <unistd.h>

#    include <cstdint>
#    include <optional>
#    include <utility>

namespace crowdb::common::metrics::detail
{

struct PerfSample
{
    uint64_t value        = 0;
    uint64_t time_enabled = 0;
    uint64_t time_running = 0;
};

class PerfCounter
{
  public:
    explicit PerfCounter(int fd) : fd_(fd)
    {
    }

    ~PerfCounter()
    {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    PerfCounter(const PerfCounter &)            = delete;
    PerfCounter &operator=(const PerfCounter &) = delete;

    PerfCounter(PerfCounter &&other) noexcept : fd_(std::exchange(other.fd_, -1))
    {
    }

    PerfCounter &operator=(PerfCounter &&other) noexcept
    {
        if (this != &other) {
            if (fd_ >= 0) {
                close(fd_);
            }
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    // Shared by DRAM and C-state collectors. Enabled only after open succeeds.
    static int open_fd(uint32_t type, uint64_t config, int cpu)
    {
        perf_event_attr attr{};
        attr.type        = type;
        attr.size        = sizeof(attr);
        attr.config      = config;
        attr.disabled    = 1;
        attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
        int fd           = static_cast<int>(syscall(SYS_perf_event_open, &attr, -1, cpu, -1, PERF_FLAG_FD_CLOEXEC));
        if (fd >= 0 && ioctl(fd, PERF_EVENT_IOC_ENABLE, 0) != 0) {
            close(fd);
            return -1;
        }
        return fd;
    }

    [[nodiscard]] static std::optional<PerfSample> read_fd(int fd)
    {
        PerfSample sample;
        if (::read(fd, &sample, sizeof(sample)) != sizeof(sample)) {
            return std::nullopt;
        }
        return sample;
    }

    [[nodiscard]] std::optional<PerfSample> read() const
    {
        return read_fd(fd_);
    }

  private:
    int fd_;
};

inline std::optional<double> scaled_delta(const PerfSample &current, const PerfSample &previous)
{
    if (current.value < previous.value || current.time_enabled < previous.time_enabled ||
        current.time_running < previous.time_running) {
        return std::nullopt;
    }
    uint64_t enabled = current.time_enabled - previous.time_enabled;
    uint64_t running = current.time_running - previous.time_running;
    if (running == 0 || enabled < running) {
        return std::nullopt;
    }
    return static_cast<double>(current.value - previous.value) * static_cast<double>(enabled) /
           static_cast<double>(running);
}

} // namespace crowdb::common::metrics::detail
#endif
