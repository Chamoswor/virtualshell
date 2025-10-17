#pragma once
#include <mutex>
#include <deque>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <chrono>
#include <memory>
#include <functional>

#include "cmd_state.hpp"

namespace virtualshell {
namespace core {

class TimeoutWatcher {
public:
    using InflightMap   = std::unordered_map<uint64_t, std::unique_ptr<CmdState>>;
    using InflightQueue = std::deque<uint64_t>;
    using FulfillFn     = std::function<void(std::unique_ptr<CmdState>, bool)>;

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