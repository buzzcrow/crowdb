// Copyright 2026-present Gian <crow.db@outlook.com>

#include "crowdb-common/metrics/system_metrics.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#ifdef __linux__
#    include <fcntl.h>
#    include <sys/ioctl.h>
#    include <sys/syscall.h>
#    include <unistd.h>
#endif

namespace crowdb::common::metrics
{

// ── Time helper ─────────────────────────────────────────────────

static uint64_t now_us()
{
    auto now = std::chrono::steady_clock::now();
    auto us  = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());
    return static_cast<uint64_t>(us.count());
}

// ── Platform-specific readers ───────────────────────────────────

#ifdef __linux__

static std::pair<uint64_t, uint64_t> read_cpu_times()
{
    // /proc/self/stat: utime at field 14, stime at field 15 (1-based).
    // The comm field is in parentheses and may contain spaces.
    std::ifstream f("/proc/self/stat");
    std::string   line;
    if (!std::getline(f, line)) {
        return {0, 0};
    }

    auto rp = line.rfind(')');
    if (rp == std::string::npos) {
        return {0, 0};
    }

    std::istringstream       iss(line.substr(rp + 2));
    std::vector<std::string> fields;
    std::string              field;
    while ((iss >> field) && !iss.fail()) { // NOLINT(bugprone-signed-bitwise) — stream extraction, not bitwise
        fields.push_back(field);
    }
    // fields[0]=state, ..., fields[11]=utime, fields[12]=stime
    if (fields.size() <= 12) {
        return {0, 0};
    }

    uint64_t           utime         = std::stoull(fields[11]);
    uint64_t           stime         = std::stoull(fields[12]);
    constexpr uint64_t ticks_per_sec = 100;
    return {utime * 1'000'000 / ticks_per_sec, stime * 1'000'000 / ticks_per_sec};
}

static uint64_t read_rss_kb()
{
    std::ifstream f("/proc/self/status");
    std::string   line;
    while (std::getline(f, line)) {
        if (line.starts_with("VmRSS:")) {
            std::istringstream iss(line.substr(6));
            uint64_t           kb;
            iss >> kb; // NOLINT(bugprone-signed-bitwise) — stream extraction, not bitwise
            return kb;
        }
    }
    return 0;
}

static std::pair<uint64_t, uint64_t> read_tcp_stats()
{
    std::ifstream            f("/proc/net/snmp");
    std::string              line;
    std::vector<std::string> labels;
    std::vector<std::string> values;
    while (std::getline(f, line)) {
        if (!line.starts_with("Tcp:")) {
            continue;
        }
        std::istringstream        iss(line);
        std::string               tok;
        std::vector<std::string> *target = labels.empty() ? &labels : &values;
        while ((iss >> tok) && !iss.fail()) { // NOLINT(bugprone-signed-bitwise) — stream extraction, not bitwise
            target->push_back(tok);
        }
        if (!values.empty()) {
            break;
        }
    }
    if (labels.empty() || values.empty()) {
        return {0, 0};
    }

    uint64_t retransmits = 0;
    uint64_t lost        = 0;
    for (size_t i = 0; i < labels.size() && i < values.size(); ++i) {
        if (labels[i] == "RetransSegs") {
            retransmits = std::stoull(values[i]);
        }
        if (labels[i] == "InErrs" || labels[i] == "OutRsts") {
            lost = std::stoull(values[i]);
        }
    }
    return {retransmits, lost};
}

// ── DRAM bandwidth via perf_event_open ──────────────────────────

struct perf_event_attr_min
{
    uint32_t type;
    uint32_t size;
    uint64_t config;
    uint64_t sample_period_or_freq;
    uint64_t sample_type;
    uint64_t read_format;
    uint64_t flags;
    uint32_t wakeup_events_or_watermark;
    uint32_t bp_type;
    uint64_t bp_addr_or_config1;
    uint64_t bp_len_or_config2;
    uint64_t branch_sample_type;
    uint64_t sample_regs_user;
    uint32_t sample_stack_user;
    int32_t  clockid;
    uint64_t sample_regs_intr;
    uint32_t aux_watermark;
    uint16_t sample_max_stack;
    uint16_t reserved2;
    uint32_t aux_sample_size;
    uint32_t reserved3;
    uint64_t sig_data;
};

static constexpr uint64_t PERF_FLAG_DISABLED             = 1ULL;
static constexpr uint64_t PERF_IOC_ENABLE                = 0x2400ULL;
static constexpr uint64_t PERF_FLAG_FD_CLOEXEC           = 8ULL;
static constexpr uint64_t PERF_FORMAT_TOTAL_TIME_ENABLED = 1ULL;
static constexpr uint64_t PERF_FORMAT_TOTAL_TIME_RUNNING = 2ULL;

struct SystemCollector::DramBwImpl
{
    struct Fd
    {
        struct Sample
        {
            uint64_t value        = 0;
            uint64_t time_enabled = 0;
            uint64_t time_running = 0;
        };

        int    fd = -1;
        Sample prev;
    };

    std::vector<Fd> read_fds;
    std::vector<Fd> write_fds;
    std::vector<Fd> total_fds;
    double          scale        = 0.0; // bytes per tick
    uint64_t        prev_time_us = 0;

    DramBwImpl() : prev_time_us(now_us())
    {
        init();
    }

    ~DramBwImpl() // NOLINT(modernize-use-equals-default) — closes fds, not trivial
    {
        for (auto &f : read_fds) {
            if (f.fd >= 0) {
                close(f.fd);
            }
        }
        for (auto &f : write_fds) {
            if (f.fd >= 0) {
                close(f.fd);
            }
        }
        for (auto &f : total_fds) {
            if (f.fd >= 0) {
                close(f.fd);
            }
        }
    }

    DramBwImpl(const DramBwImpl &)            = delete;
    DramBwImpl &operator=(const DramBwImpl &) = delete;

    static int perf_open(uint32_t pmu_type, uint64_t config, int cpu)
    {
        perf_event_attr_min attr{};
        attr.type        = pmu_type;
        attr.size        = sizeof(attr);
        attr.config      = config;
        attr.flags       = PERF_FLAG_DISABLED;
        attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
        return static_cast<int>(syscall(SYS_perf_event_open, &attr, -1, cpu, -1, PERF_FLAG_FD_CLOEXEC));
    }

    static void perf_enable(int fd)
    {
        ioctl(fd, PERF_IOC_ENABLE, 0);
    }

    static std::optional<Fd::Sample> read_fd(int fd)
    {
        Fd::Sample sample;
        if (read(fd, &sample, sizeof(sample)) != sizeof(sample)) {
            return std::nullopt;
        }
        return sample;
    }

    static std::optional<uint32_t> read_pmu_type(const std::string &name)
    {
        std::string   path = "/sys/bus/event_source/devices/" + name + "/type";
        std::ifstream f(path);
        uint32_t      v;
        if ((f >> v) && !f.fail()) { // NOLINT(bugprone-signed-bitwise) — stream extraction, not bitwise
            return v;
        }
        return std::nullopt;
    }

    // Read a PMU cpumask. Uncore PMUs (amd_df, uncore_imc) require a
    // specific CPU from this mask rather than cpu=-1. Returns 0 if
    // the file is missing.
    static int read_pmu_cpumask(const std::string &name)
    {
        std::string   path = "/sys/bus/event_source/devices/" + name + "/cpumask";
        std::ifstream f(path);
        int           v = 0;
        f >> v;
        return v;
    }

    static std::optional<uint64_t> read_pmu_event(const std::string &pmu, const std::string &event)
    {
        std::string   path = "/sys/bus/event_source/devices/" + pmu + "/events/" + event;
        std::ifstream f(path);
        std::string   content;
        if (!std::getline(f, content)) {
            return std::nullopt;
        }

        uint64_t           event_val = 0;
        uint64_t           umask_val = 0;
        std::istringstream iss(content);
        std::string        part;
        while (std::getline(iss, part, ',')) { // NOLINT(bugprone-infinite-loop) — getline advances iss
            auto eq = part.find('=');
            if (eq == std::string::npos) {
                continue;
            }
            std::string key     = part.substr(0, eq);
            std::string val_str = part.substr(eq + 1);
            if (val_str.starts_with("0x") || val_str.starts_with("0X")) {
                val_str = val_str.substr(2);
            }
            uint64_t v = std::stoull(val_str, nullptr, 16);
            if (key == "event") {
                event_val = v;
            }
            else if (key == "umask") {
                umask_val = v;
            }
        }
        return (umask_val << 8U) | event_val; // NOLINT(bugprone-signed-bitwise) — uint64_t operands
    }

    static std::vector<std::string> intel_imc_names()
    {
        std::vector<std::string> names;
        std::error_code          error;
        for (const auto &entry : std::filesystem::directory_iterator("/sys/bus/event_source/devices", error)) {
            std::string name = entry.path().filename().string();
            if (name == "uncore_imc" || name.starts_with("uncore_imc_")) {
                names.push_back(std::move(name));
            }
        }
        std::sort(names.begin(), names.end());
        return names;
    }

    // Aggregate request-with-data events exported by Linux perf for Zen 3.
    static constexpr std::array<uint64_t, 8> AMD_DF_TOTAL_EVENTS = {
        0x3807, 0x3847, 0x3887, 0x38c7, 0x100003807ULL, 0x100003847ULL, 0x100003887ULL, 0x1000038c7ULL,
    };

    void init()
    {
        if (try_amd()) {
            return;
        }
        try_intel();
    }

    bool try_amd()
    {
        auto pmu_type = read_pmu_type("amd_df");
        if (!pmu_type) {
            return false;
        }
        int             cpu = read_pmu_cpumask("amd_df");
        std::vector<Fd> totals;
        totals.reserve(AMD_DF_TOTAL_EVENTS.size());
        for (uint64_t config : AMD_DF_TOTAL_EVENTS) {
            int fd = perf_open(*pmu_type, config, cpu);
            if (fd < 0) {
                close_fds(totals);
                return false;
            }
            perf_enable(fd);
            totals.push_back({fd, {}});
        }
        total_fds = std::move(totals);
        // Each tick = 64 bytes of DRAM data.
        scale = 64.0;
        return true;
    }

    bool try_intel()
    {
        auto names = intel_imc_names();
        if (names.empty()) {
            return false;
        }
        std::vector<Fd> reads;
        std::vector<Fd> writes;
        reads.reserve(names.size());
        writes.reserve(names.size());
        for (const auto &name : names) {
            auto pmu_type  = read_pmu_type(name);
            auto read_cfg  = read_pmu_event(name, "cas_count_read");
            auto write_cfg = read_pmu_event(name, "cas_count_write");
            if (!pmu_type || !read_cfg || !write_cfg) {
                close_fds(reads);
                close_fds(writes);
                return false;
            }
            int cpu = read_pmu_cpumask(name);
            int rfd = perf_open(*pmu_type, *read_cfg, cpu);
            int wfd = perf_open(*pmu_type, *write_cfg, cpu);
            if (rfd < 0 || wfd < 0) {
                if (rfd >= 0)
                    close(rfd);
                if (wfd >= 0)
                    close(wfd);
                close_fds(reads);
                close_fds(writes);
                return false;
            }
            perf_enable(rfd);
            perf_enable(wfd);
            reads.push_back({rfd, {}});
            writes.push_back({wfd, {}});
        }
        read_fds  = std::move(reads);
        write_fds = std::move(writes);
        scale     = 64.0; // 64 bytes per tick
        return true;
    }

    struct Bandwidth
    {
        std::optional<double> read;
        std::optional<double> write;
        double                total = 0.0;
    };

    std::optional<Bandwidth> read_bytes_per_sec()
    {
        if (!total_fds.empty()) {
            auto sample = sample_deltas(total_fds);
            if (!sample) {
                return std::nullopt;
            }
            uint64_t cur_time   = now_us();
            uint64_t elapsed_us = cur_time - prev_time_us;
            if (elapsed_us == 0) {
                return std::nullopt;
            }
            commit_samples(total_fds, sample->second);
            prev_time_us   = cur_time;
            double elapsed = static_cast<double>(elapsed_us) / 1'000'000.0;
            return Bandwidth{std::nullopt, std::nullopt, sample->first * scale / elapsed};
        }
        if (read_fds.empty() || write_fds.empty()) {
            return std::nullopt;
        }

        auto read_sample  = sample_deltas(read_fds);
        auto write_sample = sample_deltas(write_fds);
        if (!read_sample || !write_sample) {
            return std::nullopt;
        }

        uint64_t cur_time   = now_us();
        uint64_t elapsed_us = cur_time - prev_time_us;
        if (elapsed_us == 0) {
            elapsed_us = 1;
        }
        commit_samples(read_fds, read_sample->second);
        commit_samples(write_fds, write_sample->second);
        prev_time_us        = cur_time;
        double elapsed_secs = static_cast<double>(elapsed_us) / 1'000'000.0;
        double read_bytes   = read_sample->first * scale;
        double write_bytes  = write_sample->first * scale;
        double read_bw      = read_bytes / elapsed_secs;
        double write_bw     = write_bytes / elapsed_secs;
        return Bandwidth{read_bw, write_bw, read_bw + write_bw};
    }

  private:
    static std::optional<std::pair<double, std::vector<Fd::Sample>>> sample_deltas(const std::vector<Fd> &fds)
    {
        std::vector<Fd::Sample> samples;
        samples.reserve(fds.size());
        double total = 0.0;
        for (const auto &f : fds) {
            auto sample = read_fd(f.fd);
            if (!sample || sample->value < f.prev.value || sample->time_enabled < f.prev.time_enabled ||
                sample->time_running < f.prev.time_running) {
                return std::nullopt;
            }
            uint64_t running = sample->time_running - f.prev.time_running;
            if (running == 0) {
                return std::nullopt;
            }
            uint64_t value   = sample->value - f.prev.value;
            uint64_t enabled = sample->time_enabled - f.prev.time_enabled;
            total += static_cast<double>(value) * static_cast<double>(enabled) / static_cast<double>(running);
            samples.push_back(*sample);
        }
        return std::pair{total, std::move(samples)};
    }

    static void commit_samples(std::vector<Fd> &fds, const std::vector<Fd::Sample> &samples)
    {
        for (size_t index = 0; index < fds.size(); ++index) {
            fds[index].prev = samples[index];
        }
    }

    static void close_fds(std::vector<Fd> &fds)
    {
        for (auto &fd : fds) {
            if (fd.fd >= 0)
                close(fd.fd);
            fd.fd = -1;
        }
    }
};

#else // non-Linux

struct SystemCollector::DramBwImpl
{
};

static std::pair<uint64_t, uint64_t> read_cpu_times()
{
    return {0, 0};
}

static uint64_t read_rss_kb()
{
    return 0;
}

static std::pair<uint64_t, uint64_t> read_tcp_stats()
{
    return {0, 0};
}

#endif

// ── SystemCollector ─────────────────────────────────────────────

SystemCollector::SystemCollector()
{
#ifdef __linux__
    auto [user_us, sys_us]   = read_cpu_times();
    auto [retransmits, lost] = read_tcp_stats();
    prev_cpu_user_us_        = user_us;
    prev_cpu_sys_us_         = sys_us;
    prev_tcp_retransmits_    = retransmits;
    prev_tcp_lost_           = lost;
    prev_time_us_            = now_us();
    dram_bw_                 = new DramBwImpl();
    if (dram_bw_->read_fds.empty() && dram_bw_->write_fds.empty() && dram_bw_->total_fds.empty()) {
        delete dram_bw_;
        dram_bw_ = nullptr;
    }
#else
    prev_time_us_ = now_us();
#endif
}

SystemCollector::~SystemCollector() = default;

SystemCollector::SystemCollector(SystemCollector &&other) noexcept
    : prev_cpu_user_us_(other.prev_cpu_user_us_),
      prev_cpu_sys_us_(other.prev_cpu_sys_us_),
      prev_tcp_retransmits_(other.prev_tcp_retransmits_),
      prev_tcp_lost_(other.prev_tcp_lost_),
      prev_time_us_(other.prev_time_us_),
      dram_bw_(std::exchange(other.dram_bw_, nullptr))
{
}

SystemCollector &SystemCollector::operator=(SystemCollector &&other) noexcept
{
    if (this != &other) {
        delete dram_bw_;
        prev_cpu_user_us_     = other.prev_cpu_user_us_;
        prev_cpu_sys_us_      = other.prev_cpu_sys_us_;
        prev_tcp_retransmits_ = other.prev_tcp_retransmits_;
        prev_tcp_lost_        = other.prev_tcp_lost_;
        prev_time_us_         = other.prev_time_us_;
        dram_bw_              = std::exchange(other.dram_bw_, nullptr);
    }
    return *this;
}

SystemMetricsSnapshot SystemCollector::collect()
{
    auto [user_us, sys_us]   = read_cpu_times();
    auto [retransmits, lost] = read_tcp_stats();

    uint64_t cur_time   = now_us();
    uint64_t elapsed_us = cur_time - prev_time_us_;
    if (elapsed_us == 0) {
        elapsed_us = 1;
    }
    prev_time_us_ = cur_time;

    uint64_t delta_user      = user_us - prev_cpu_user_us_;
    uint64_t delta_sys       = sys_us - prev_cpu_sys_us_;
    uint64_t tcp_retransmits = retransmits - prev_tcp_retransmits_;
    uint64_t tcp_lost        = lost - prev_tcp_lost_;

    prev_cpu_user_us_     = user_us;
    prev_cpu_sys_us_      = sys_us;
    prev_tcp_retransmits_ = retransmits;
    prev_tcp_lost_        = lost;

    uint64_t cpu_user_pct = delta_user * 100 / elapsed_us;
    uint64_t cpu_sys_pct  = delta_sys * 100 / elapsed_us;

    SystemMetricsSnapshot snap;
    snap.cpu_user_pct    = cpu_user_pct;
    snap.cpu_sys_pct     = cpu_sys_pct;
    snap.rss_kb          = read_rss_kb();
    snap.tcp_retransmits = tcp_retransmits;
    snap.tcp_lost        = tcp_lost;

#ifdef __linux__
    if (dram_bw_ != nullptr) {
        auto bw = dram_bw_->read_bytes_per_sec();
        if (bw) {
            if (bw->read) {
                snap.dram_read_mib = *bw->read / 1024.0 / 1024.0;
            }
            if (bw->write) {
                snap.dram_write_mib = *bw->write / 1024.0 / 1024.0;
            }
            snap.dram_total_mib = bw->total / 1024.0 / 1024.0;
        }
    }
#endif

    return snap;
}

void flush_system(FILE *fp, const SystemMetricsSnapshot &snap)
{
    double rss_gb   = static_cast<double>(snap.rss_kb) / 1024.0 / 1024.0;
    auto   print_bw = [fp](const std::optional<double> &value) {
        value ? std::fprintf(fp, "%.1f", *value) : std::fprintf(fp, "unsupported");
    };
    std::fprintf(fp, "sys  cpu.user=%llu%% cpu.sys=%llu%% rss_gb=%.2f tcp_retrans=%llu tcp_lost=%llu bw_read_mib=",
                 static_cast<unsigned long long>(snap.cpu_user_pct), static_cast<unsigned long long>(snap.cpu_sys_pct),
                 rss_gb, static_cast<unsigned long long>(snap.tcp_retransmits),
                 static_cast<unsigned long long>(snap.tcp_lost));
    print_bw(snap.dram_read_mib);
    std::fprintf(fp, " bw_write_mib=");
    print_bw(snap.dram_write_mib);
    std::fprintf(fp, " bw_total_mib=");
    print_bw(snap.dram_total_mib);
    std::fprintf(fp, "\n");
}

} // namespace crowdb::common::metrics
