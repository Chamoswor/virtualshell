#pragma once
// High-throughput Python bridge utilities for VirtualShell.
// - Single dedicated dispatcher thread holds the GIL and executes Python callables.
// - Tasks are batched with a time-slice + max batch size to avoid monopolizing the GIL.
// - Safe interpreter shutdown handling (atexit guard + Py_IsInitialized()).
// - Future bridge: std::future<T> -> concurrent.futures.Future with optional Python callback.
//
// Requires C++17 and pybind11.

#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
#include <utility>
#include "virtual_shell.hpp"
#include <future>

namespace virtualshell {
namespace pybridge {

// -------------------- Lightweight background ThreadPool --------------------
class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads)
        : stop_(false)
    {
        if (num_threads == 0) num_threads = 1;
        workers_.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lk(m_);
                        cv_.wait(lk, [this] { return stop_ || !q_.empty(); });
                        if (stop_ && q_.empty()) return;
                        task = std::move(q_.front());
                        q_.pop();
                    }
                    try { task(); } catch (...) { /* swallow */ }
                }
            });
        }
    }

    template <class F>
    void post(F&& f) {
        {
            std::lock_guard<std::mutex> lk(m_);
            if (stop_) throw std::runtime_error("ThreadPool stopped");
            q_.emplace(std::forward<F>(f));
        }
        cv_.notify_one();
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lk(m_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) t.join();
        }
    }

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> q_;
    std::mutex m_;
    std::condition_variable cv_;
    bool stop_;
};

// Global pool (cap threads for stability). You may tune the cap.
inline ThreadPool& get_global_pool() {
    static size_t n = std::min<size_t>(std::thread::hardware_concurrency() ? std::thread::hardware_concurrency() : 4, 8);
    static ThreadPool pool(n);
    return pool;
}

// --------------------- Python interpreter bridge ---------------------
namespace py = pybind11;

inline std::atomic<bool>& interpreter_is_down_flag() {
    static std::atomic<bool> flag{false};
    return flag;
}

// Conservative check usable also during finalization.
inline bool interpreter_down() noexcept {
    // Py_IsInitialized can return true late in shutdown; we prefer our atexit flag.
    if (interpreter_is_down_flag().load(std::memory_order_acquire)) return true;
#if defined(Py_LIMITED_API)
    // Limited API: rely on atexit flag only
    return false;
#else
    // Best effort; harmless if Python is fully down (we never acquire GIL here).
    return !Py_IsInitialized();
#endif
}

// --------------------- PyDispatcher ---------------------
class PyDispatcher {
public:
    using Task = std::function<void()>; // Executed with the GIL already held.

    static PyDispatcher& inst() {
        static PyDispatcher d;
        return d;
    }

    // Enqueue a Python task. It will run on the dispatcher thread under GIL.
    inline void post(Task task) {
        if (!task) return;
        // If interpreter is going down, discard early.
        if (interpreter_down()) return;

        {
            std::lock_guard<std::mutex> lk(mx_);
            q_.push(std::move(task));
        }
        cv_.notify_one();
    }

    // Optional: flush pending tasks quickly (used rarely, e.g., tests)
    void flush(std::chrono::milliseconds max_wait = std::chrono::milliseconds(50)) {
        auto end = std::chrono::steady_clock::now() + max_wait;
        while (std::chrono::steady_clock::now() < end) {
            if (empty()) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    bool empty() {
        std::lock_guard<std::mutex> lk(mx_);
        return q_.empty();
    }

private:
    PyDispatcher() { thr_ = std::thread([this] { run(); }); }
    ~PyDispatcher() {
        {
            std::lock_guard<std::mutex> lk(mx_);
            stop_ = true;
        }
        cv_.notify_one();
        if (thr_.joinable()) thr_.join();
    }
    PyDispatcher(const PyDispatcher&) = delete;
    PyDispatcher& operator=(const PyDispatcher&) = delete;

    void run() {
        // Batch parameters: tune to balance latency vs throughput.
        constexpr size_t MAX_TASKS_PER_SLICE = 256;
        constexpr auto   TIME_BUDGET         = std::chrono::microseconds(1500);

        for (;;) {
            std::queue<Task> local;
            {
                std::unique_lock<std::mutex> lk(mx_);
                cv_.wait(lk, [&]{ return stop_ || !q_.empty(); });
                if (stop_) break;
                // Steal queue in O(1)
                local.swap(q_);
            }

            if (interpreter_down()) {
                // Drop everything silently during shutdown.
                while (!local.empty()) local.pop();
                continue;
            }

            // Acquire GIL once per slice.
            {
                py::gil_scoped_acquire gil;
                auto deadline = std::chrono::steady_clock::now() + TIME_BUDGET;
                size_t processed = 0;

                while (!local.empty()) {
                    Task t = std::move(local.front());
                    local.pop();
                    try {
                        t(); // Must be Python-safe; GIL is held.
                    } catch (py::error_already_set& e) {
                        e.discard_as_unraisable("PyDispatcher::task");
                    } catch (...) {
                        // Swallow to keep dispatcher robust
                    }

                    if (++processed >= MAX_TASKS_PER_SLICE ||
                        std::chrono::steady_clock::now() >= deadline) {
                        // Yield GIL and continue on next wakeup.
                        break;
                    }
                }

                // If tasks remain in 'local', push them back and resignal.
                if (!local.empty()) {
                    std::lock_guard<std::mutex> lk(mx_);
                    while (!local.empty()) {
                        q_.push(std::move(local.front()));
                        local.pop();
                    }
                    cv_.notify_one();
                }
            }
        }
    }

    std::mutex mx_;
    std::condition_variable cv_;
    std::queue<Task> q_;
    std::thread thr_;
    bool stop_{false};
};

// Register an atexit guard in Python so we can stop posting safely at shutdown.
inline void install_atexit_guard() {
    try {
        py::gil_scoped_acquire gil;
        auto atexit = py::module_::import("atexit");
        atexit.attr("register")(py::cpp_function([](){
            interpreter_is_down_flag().store(true, std::memory_order_release);
        }));
        // Touch dispatcher so it is constructed under a live interpreter
        (void)PyDispatcher::inst();
    } catch (...) {
        // If this fails, we'll still rely on Py_IsInitialized() checks.
    }
}

// --------------------- Future bridge ---------------------
// Helper: create Python RuntimeError object
inline py::object make_py_runtime_error(const std::string& msg) {
    auto builtins = py::module_::import("builtins");
    return builtins.attr("RuntimeError")(py::str(msg));
}

// ---- replace your make_py_future_from_std_future<T> with this version ----
template <class T>
py::object make_py_future_from_std_future(std::future<T> fut, py::object py_callback /* may be None */) {
    // Create the Python Future now (under GIL).
    py::gil_scoped_acquire gil;
    py::object py_fut = py::module_::import("concurrent.futures").attr("Future")();

    // Hold objects safely across threads with custom cleanup that tolerates shutdown.
    auto py_fut_ptr = std::make_shared<py::object>(py_fut);
    auto cb_ptr = std::shared_ptr<py::object>(
        py_callback.is_none() ? nullptr : new py::object(py_callback),
        [](py::object* p) {
            if (!p) return;
            if (interpreter_down()) { p->release(); delete p; return; }
            delete p;
        }
    );

    // Move the std::future into a shared holder for the background pool.
    auto shared = std::make_shared<std::future<T>>(std::move(fut));

    // Background stage: block on get() without the GIL, then enqueue fulfilment.
    get_global_pool().post([shared, py_fut_ptr, cb_ptr]() mutable {
        bool ok = false;
        std::string err;
        std::shared_ptr<T> value_sp;

        try {
            if constexpr (std::is_void_v<T>) {
                shared->get();
                ok = true;
            } else {
                T v = shared->get();
                value_sp = std::make_shared<T>(std::move(v));
                ok = true;
            }
        } catch (const std::exception& e) {
            err = e.what();
        } catch (...) {
            err = "unknown C++ exception";
        }

        // Fulfil on the Python dispatcher (GIL held there).
        PyDispatcher::inst().post([py_fut_ptr, cb_ptr, ok, err, value_sp]() mutable {
            if (interpreter_down()) return;
            py::gil_scoped_acquire gil;
            py::object fut = *py_fut_ptr;

            auto set_exc = [&](const std::string& msg){
                try { fut.attr("set_exception")(make_py_runtime_error(msg)); } catch (...) {}
            };

            try {
                if (ok) {
                    if constexpr (std::is_void_v<T>) {
                        fut.attr("set_result")(py::none());
                        if (cb_ptr) { try { (*cb_ptr)(py::none()); } catch (py::error_already_set& e){ e.discard_as_unraisable("py_callback"); } }
                    } else {
                        py::object py_res = py::cast(*value_sp);
                        fut.attr("set_result")(py_res);
                        if (cb_ptr) { try { (*cb_ptr)(py_res); } catch (py::error_already_set& e){ e.discard_as_unraisable("py_callback"); } }
                    }
                } else {
                    set_exc(err.empty() ? "unknown error" : err);
                }
            } catch (py::error_already_set& e) {
                set_exc(e.what());
                e.discard_as_unraisable("make_py_future_from_std_future");
            }

            *py_fut_ptr = py::none();
            if (cb_ptr) *cb_ptr = py::none();
        });
    });

    return py_fut;
}

} // namespace pybridge
} // namespace virtualshell
