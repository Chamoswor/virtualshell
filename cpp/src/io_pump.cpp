#include "io_pump.hpp"
#include "process.hpp"

#include <chrono>
#include <utility>

using namespace std::chrono_literals;

namespace virtualshell {
namespace core {

namespace {
constexpr auto kIdleSleep = 2ms; // Sleep duration when no data is available.

void join_if_joinable(std::thread& thread) noexcept {
    if (thread.joinable()) {
        thread.join();
    }
}

} // namespace

IoPump::IoPump() = default;
IoPump::~IoPump() {
    stop();
}

IoPump::IoPump(IoPump&& other) noexcept {
    // Transfer ownership while holding the source lifecycle guard to avoid racing the worker threads.
    std::lock_guard<std::mutex> guard(other.lifecycle_mutex_);

    running_.store(other.running_.load(std::memory_order_acquire), std::memory_order_release);
    process_ = other.process_;
    handler_ = std::move(other.handler_);

    write_queue_ = std::move(other.write_queue_);

    stdout_thread_ = std::move(other.stdout_thread_);
    stderr_thread_ = std::move(other.stderr_thread_);
    writer_thread_ = std::move(other.writer_thread_);

    other.running_.store(false, std::memory_order_release);
    other.process_ = nullptr;
    other.clear_write_queue_();
}

IoPump& IoPump::operator=(IoPump&& other) noexcept {
    if (this == &other) {
        return *this;
    }

    // Lock both lifecycles to ensure no threads mutate shared state during reassignment.
    std::scoped_lock guard(lifecycle_mutex_, other.lifecycle_mutex_);

    stop_locked_();

    running_.store(other.running_.load(std::memory_order_acquire), std::memory_order_release);
    process_ = other.process_;
    handler_ = std::move(other.handler_);

    write_queue_ = std::move(other.write_queue_);

    stdout_thread_ = std::move(other.stdout_thread_);
    stderr_thread_ = std::move(other.stderr_thread_);
    writer_thread_ = std::move(other.writer_thread_);

    other.running_.store(false, std::memory_order_release);
    other.process_ = nullptr;
    other.clear_write_queue_();

    return *this;
}

void IoPump::start(Process& process, ChunkHandler handler) {
    std::lock_guard<std::mutex> guard(lifecycle_mutex_);

    stop_locked_();

    process_ = &process;
    handler_ = std::move(handler);

    // Mark the pump active before we spin up threads so they observe the flag.
    running_.store(true, std::memory_order_release);

    stdout_thread_ = std::thread(&IoPump::reader_loop_, this, false);
    stderr_thread_ = std::thread(&IoPump::reader_loop_, this, true);
    writer_thread_ = std::thread(&IoPump::writer_loop_, this);
}

void IoPump::stop() {
    std::lock_guard<std::mutex> guard(lifecycle_mutex_);
    stop_locked_();
}

bool IoPump::enqueue_write(std::string data) {
    std::lock_guard<std::mutex> lock(write_mutex_);
    if (!running_.load(std::memory_order_acquire)) {
        return false;
    }

    // Queue the payload for the writer thread and wake it up.
    write_queue_.emplace_back(std::move(data));
    write_cv_.notify_one();
    return true;
}

void IoPump::drain() {
    std::unique_lock<std::mutex> lock(write_mutex_);
    write_cv_.wait(lock, [this]() {
        return !running_.load(std::memory_order_acquire) || write_queue_.empty();
    });
}

void IoPump::stop_locked_() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        handler_ = nullptr;
        process_ = nullptr;
        clear_write_queue_();
        return;
    }

    // Signal the child to close pipes so reader threads unblock cleanly.
    if (process_) {
        process_->shutdown_streams();
    }

    write_cv_.notify_all();

    join_if_joinable(stdout_thread_);
    join_if_joinable(stderr_thread_);
    join_if_joinable(writer_thread_);

    clear_write_queue_();
    handler_ = nullptr;
    process_ = nullptr;
}

void IoPump::reader_loop_(bool is_stderr) {
    while (running_.load(std::memory_order_acquire)) {
        if (!process_) {
            break;
        }

        std::optional<std::string> chunk;
        try {
            chunk = is_stderr ? process_->read_stderr() : process_->read_stdout();
        } catch (...) {
            break;
        }

        if (!chunk) {
            // Nullopt indicates EOF/pipe closed; exit the loop so stop() can reclaim threads.
            break;
        }

        auto handler = handler_snapshot_();
        if (!handler) {
            continue;
        }

        try {
            handler(is_stderr, *chunk);
        } catch (...) {
            // Defensive: stop pumping if the handler throws to avoid tight retry loops.
            break;
        }
    }

    running_.store(false, std::memory_order_release);
    write_cv_.notify_all();
}

void IoPump::writer_loop_() {
    while (running_.load(std::memory_order_acquire)) {
        std::string next;
        {
            std::unique_lock<std::mutex> lock(write_mutex_);
            if (write_queue_.empty()) {
                write_cv_.wait_for(lock, kIdleSleep, [this]() {
                    return !running_.load(std::memory_order_acquire) || !write_queue_.empty();
                });

                if (!running_.load(std::memory_order_acquire)) {
                    break;
                }

                if (write_queue_.empty()) {
                    continue;
                }
            }

            next = std::move(write_queue_.front());
            write_queue_.pop_front();
            if (write_queue_.empty()) {
                write_cv_.notify_all();
            }
        }

        if (!process_) {
            break;
        }

        bool ok = false;
        try {
            ok = process_->write(next);
        } catch (...) {
            ok = false;
        }

        if (!ok) {
            // If the write fails, shut the pump down so callers can respond to the broken pipe.
            running_.store(false, std::memory_order_release);
            write_cv_.notify_all();
            if (process_) {
                process_->shutdown_streams();
            }
            break;
        }
    }
}

void IoPump::clear_write_queue_() {
    std::lock_guard<std::mutex> lock(write_mutex_);
    write_queue_.clear();
}

IoPump::ChunkHandler IoPump::handler_snapshot_() {
    std::lock_guard<std::mutex> lock(handler_mutex_);
    return handler_;
}

} // namespace core
} // namespace virtualshell
