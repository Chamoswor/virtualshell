#pragma once
#include <atomic>
#include <mutex>
#include <deque>
#include <unordered_map>
#include <string>
#include <functional>
#include <future>
#include "execution_result.hpp"

namespace virtualshell {
namespace core {

    /**
     * @brief Bookkeeping for an in-flight PowerShell command.
     *
     * Tracks buffers, lifecycle markers, timing metadata, and the completion
     * promise/callback so VirtualShell can resolve results once PowerShell
     * signals completion or timeout.
     */
    struct CmdState {
        uint64_t                           id{};          ///< Unique command identifier
        std::promise<ExecutionResult>      prom{};        ///< Promise to deliver the command result
        std::string                        outBuf{};      ///< Accumulated stdout buffer
        std::string                        errBuf{};      ///< Accumulated stderr buffer
        std::string                        beginMarker{}; ///< Unique begin marker (e.g. "\x1ESS_BEG_123\x1E")
        std::string                        endMarker{};   ///< Unique marker string (e.g. "\x1ESS_END_123\x1E")
        std::atomic<bool>                  begun{false};///< True once begin marker has been seen
        std::string                        preBuf{};      ///< Buffer for data before begin marker
        std::atomic<bool>                  done{false}; ///< True once command is completed
        std::atomic<bool>                  timedOut{false}; ///< True if command exceeded timeout
        std::atomic<bool>                  restartInterrupted{false}; ///< True if command was aborted during restart
        double                             startMonotonic{}; ///< Start time in monotonic seconds
        double                             timeoutSec{}; ///< Timeout in seconds for this command
        std::function<void(const ExecutionResult&)> cb{};  ///< Optional callback for completion
        std::chrono::steady_clock::time_point tStart{};   ///< Start timestamp
        std::chrono::steady_clock::time_point tDeadline{};///< Absolute deadline for timeout (steady_clock)
    };
}} // namespace virtualshell::core