#pragma once

#if defined(_WIN32) && defined(_MSC_VER)
  #pragma warning(push)
  #pragma warning(disable : 4996) // getenv deprecation
#endif

#if defined(_WIN32) && defined(__clang__)
  #pragma clang diagnostic push
  #pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdarg>
#include <mutex>
#include <string>
#include <thread>
#include <array>

// Auto-enable via env var:
// VIRTUALSHELL_DEBUG=1
// VIRTUALSHELL_DEBUG_PATH=C:\temp\vshell.log
// VIRTUALSHELL_DEBUG_EXCLUDE=IO,PARSE

static constexpr size_t MAX_EXCLUDED_TAGS = 16;

namespace virtualshell {
namespace dev {

/**
 * @brief Thread-safe, lazily-opened file logger for development diagnostics.
 *
 * Controlled through environment variables (VIRTUALSHELL_DEBUG, etc.) and safe
 * to call from multiple threads. Designed for lightweight instrumentation
 * without pulling in an external logging dependency.
 */
class Logger {
public:
    /**
     * @brief Obtain the singleton logger instance.
     */
    static Logger& instance() {
        static Logger inst;
        return inst;
    }

    /**
     * @brief Enable or disable logging at runtime.
     *
     * When enabling, optionally override the output path. If left empty the
     * default "virtualshell_debug.log" in the current working directory is used.
     */
    void enable(bool on, std::string path = {}) {
        std::lock_guard<std::mutex> lk(mx_);
        enabled_.store(on, std::memory_order_relaxed);
        if (on) {
            if (!path.empty()) path_ = std::move(path);
            if (!fh_) open_nolock_();
        } else {
            close_nolock_();
        }
    }

    /**
     * @brief Query whether logging is currently enabled.
     */
    bool enabled() const { return enabled_.load(std::memory_order_relaxed); }

    /**
     * @brief Log a formatted message with UTC timestamp and thread id.
     */
    void logf(const char* tag, const char* fmt, ...) {
        if (!enabled()) return;
        std::lock_guard<std::mutex> lk(mx_);
        if (!fh_) open_nolock_();

        if (!fh_) return; // give up if open failed

        if (is_excluded_(tag)) {
            return;
        }

        // Timestamp (UTC), thread id
        auto now   = std::chrono::system_clock::now();
        auto secs  = std::chrono::time_point_cast<std::chrono::seconds>(now);
        auto micros= std::chrono::duration_cast<std::chrono::microseconds>(now - secs).count();
        std::time_t t = std::chrono::system_clock::to_time_t(secs);
        std::tm tm{};
#if defined(_WIN32)
        gmtime_s(&tm, &t);
#else
        gmtime_r(&t, &tm);
#endif
        char ts[64];
        std::snprintf(ts, sizeof(ts), "%04d-%02d-%02dT%02d:%02d:%02d.%06lldZ",
                      tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
                      tm.tm_hour, tm.tm_min, tm.tm_sec,
                      static_cast<long long>(micros));

        // Message body
        char buf[2048];
        va_list ap;
        va_start(ap, fmt);
#if defined(_WIN32)
        _vsnprintf_s(buf, sizeof(buf), _TRUNCATE, fmt, ap);
#else
        vsnprintf(buf, sizeof(buf), fmt, ap);
#endif
        va_end(ap);

        // Thread id (hash)
        auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());

        std::fprintf(fh_, "[%s] [%s] [tid=%llu] %s\n",
                     ts, tag ? tag : "-", static_cast<unsigned long long>(tid), buf);
        std::fflush(fh_);
    }

private:
    Logger() {
        const char* env_on   = std::getenv("VIRTUALSHELL_DEBUG");
        const char* env_path = std::getenv("VIRTUALSHELL_DEBUG_PATH");
        const char* env_excl = std::getenv("VIRTUALSHELL_DEBUG_EXCLUDE");
        set_excluded_tags_(env_excl);


        if (env_path && *env_path) path_ = env_path;
        if (env_on && *env_on == '1') {
            enabled_.store(true);
            open_nolock_();
            logf("LOGGER", "VirtualShell debug is ENABLED via environment variable. Set VIRTUALSHELL_DEBUG=0 to disable.");
            logf("LOGGER", "VIRTUALSHELL_DEBUG_PATH=%s", path_.empty() ? "(default)" : path_.c_str());
            logf("LOGGER", "VIRTUALSHELL_DEBUG_EXCLUDE=%s", env_excl ? env_excl : "(none)");
        }
    }
    ~Logger() { close_nolock_(); }
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    std::array<std::string, MAX_EXCLUDED_TAGS> excluded_tags_{};
    size_t excluded_count_{0};

    /**
     * @brief Append a tag to the exclusion list, bounded by MAX_EXCLUDED_TAGS.
     */
    void add_excluded_tag_(const char* start, const char* end) {
        if (excluded_count_ >= excluded_tags_.size()) return;
        const size_t len = static_cast<size_t>(end - start);
        if (len == 0) return;
        excluded_tags_[excluded_count_++] = std::string(start, len);
    }


    /**
     * @brief Parse comma-separated exclusions from the environment variable.
     */
    bool set_excluded_tags_(const char* env_excl) {
        excluded_tags_.fill({});
        excluded_count_ = 0;
        if (!env_excl || *env_excl == '\0') return false;

        const char* start = env_excl;
        const char* p = env_excl;
        while (*p != '\0') {
            if (*p == ',') {
                add_excluded_tag_(start, p);
                start = p + 1;
            }
            ++p;
        }
        add_excluded_tag_(start, p);
        return excluded_count_ > 0;
    }

    /**
     * @brief Check whether a given tag should be skipped.
     */
    bool is_excluded_(const char* tag) const {
        if (!tag) return false;
        for (size_t i = 0; i < excluded_count_; ++i) {
            if (excluded_tags_[i] == tag) return true;
        }
        return false;
    }

    /**
     * @brief Open the log file using the current path (mutex must be held).
     */
    void open_nolock_() {
        if (fh_) return;
        if (path_.empty()) path_ = "virtualshell_debug.log";
#if defined(_WIN32)
        fopen_s(&fh_, path_.c_str(), "ab");
#else
        fh_ = std::fopen(path_.c_str(), "ab");
#endif
        if (fh_) {
            std::fprintf(fh_, "----- VirtualShell debug start -----\n");
            std::fflush(fh_);
        }
    }
    /**
     * @brief Close the log file and reset state (mutex must be held).
     */
    void close_nolock_() {
        if (fh_) {
            std::fprintf(fh_, "----- VirtualShell debug stop ------\n");
            std::fclose(fh_);
            fh_ = nullptr;
        }
    }

    std::atomic<bool> enabled_{false};
    std::string path_;
    std::mutex mx_;
    std::FILE* fh_{nullptr};
};

}} // namespace virtualshell::dev

#if defined(_WIN32) && defined(__clang__)
  #pragma clang diagnostic pop
#endif
#if defined(_WIN32) && defined(_MSC_VER)
  #pragma warning(pop)
#endif
