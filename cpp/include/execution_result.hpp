#pragma once

#include <string>
#include <vector>

namespace virtualshell {
namespace core {

    /**
     * @brief Result payload returned for every PowerShell invocation.
     *
     * Captures raw streams, exit metadata, and timing so callers can implement
     * retries, telemetry, or higher-level orchestration policies.
     */
    struct ExecutionResult {
        std::string out{};        ///< Captured stdout text (may be empty when uncaptured)
        std::string err{};        ///< Captured stderr text (may be empty when uncaptured)
        int         exitCode{};      ///< Native exit code reported by PowerShell (0 typically means success)
        bool        success{};       ///< Convenience flag mirroring exitCode == 0 or backend success semantics
        double      executionTime{}; ///< Wall-clock execution time in seconds measured by the backend
    };

    /**
     * @brief Progress payload surfaced during batch executions.
     *
     * Emitted whenever a command in the batch completes, enabling long-running
     * batches to report incremental state back to the caller.
     */
    struct BatchProgress {
        size_t currentCommand{};                 ///< Zero-based index of the command that just completed
        size_t totalCommands{};                  ///< Total number of commands scheduled in the batch
        ExecutionResult lastResult{};            ///< Result payload for the most recently finished command
        bool isComplete{};                       ///< True once every command in the batch has been processed
        std::vector<ExecutionResult> allResults{}; ///< Snapshot of completed command results (fully populated at completion)
    };

} // namespace core
} // namespace virtualshell
