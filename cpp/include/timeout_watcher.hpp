#pragma once
#include <mutex>
#include <deque>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <chrono>
#include <memory>
#include <functional>
#include <algorithm>
#include <cstdint> // for uint64_t

#include "cmd_state.hpp"

namespace virtualshell {
namespace core {

/**
 * @brief Watches active commands and fulfils them once their deadline passes.
 *
 * The watcher runs on a dedicated thread, scanning the shared inflight map.
 * When a command times out it delegates to the provided fulfil function which
 * performs cleanup and surfaces the timeout back to callers.
 */
class TimeoutWatcher {
public:
    using InflightMap   = std::unordered_map<uint64_t, std::unique_ptr<CmdState>>;
    using InflightQueue = std::deque<uint64_t>;
    using FulfillFn     = std::function<void(std::unique_ptr<CmdState>, bool)>;

    /**
     * @brief Construct a watcher binding to the shared command collections.
     *
     * @param stateMx Mutex guarding inflight state
     * @param inflight Map of command id -> state
     * @param inflightOrder FIFO order of submitted commands
     * @param timerRun Flag toggled by the owning VirtualShell to stop scanning
     * @param fulfill Callback invoked to finish a timed-out command
     */
    TimeoutWatcher(std::mutex& stateMx,
                   InflightMap& inflight,
                   InflightQueue& inflightOrder,
                   std::atomic<bool>& timerRun,
                   FulfillFn fulfill)
        : stateMx_(stateMx),
          inflight_(inflight),
          inflightOrder_(inflightOrder),
          timerRun_(timerRun),
          fulfill_(std::move(fulfill)) {}

    /**
     * @brief Force a specific command id to timeout immediately.
     */
    void timeoutOne(uint64_t id) {
        std::unique_ptr<CmdState> st;
        {
            std::lock_guard<std::mutex> lk(stateMx_);
            auto it = inflight_.find(id);
            if (it == inflight_.end()) return;
            st = std::move(it->second);
            st->timedOut.store(true);
            inflight_.erase(it);

            if (!inflightOrder_.empty() && inflightOrder_.front() == id) {
                inflightOrder_.pop_front();
            } else {
                auto qit = std::find(inflightOrder_.begin(), inflightOrder_.end(), id);
                if (qit != inflightOrder_.end()) inflightOrder_.erase(qit);
            }
        }
        fulfill_(std::move(st), true);
    }

    /**
     * @brief Periodically walk inflight commands, fulfilling any past deadline.
     */
    void scan() {
        using clock = std::chrono::steady_clock;
        while (timerRun_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            if (!timerRun_) break;

            std::vector<uint64_t> toExpire;
            const auto now = clock::now();

            {
                std::lock_guard<std::mutex> lk(stateMx_);
                if (inflight_.empty()) continue;
                for (auto const& id : inflightOrder_) {
                    auto it = inflight_.find(id);
                    if (it == inflight_.end()) continue;
                    auto& S = *it->second;
                    if (S.tDeadline != clock::time_point::max() && now >= S.tDeadline) {
                        toExpire.push_back(id);
                    }
                }
            }

            for (auto id : toExpire) {
                timeoutOne(id);
            }
        }
    }

private:
    std::mutex& stateMx_;
    InflightMap& inflight_;
    InflightQueue& inflightOrder_;
    std::atomic<bool>& timerRun_;
    FulfillFn fulfill_;
};
}} // namespace virtualshell::core