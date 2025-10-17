#pragma once

#include <string>
#include <vector>

namespace virtualshell {
namespace core {

    /**
     * @brief Result of a PowerShell command.
     */
    struct ExecutionResult {
        std::string out{};        ///< Stdout from the command
        std::string err{};        ///< Stderr from the command
        int         exitCode{};      ///< Exit code (0 = success)
        bool        success{};       ///< Whether the command completed successfully
        double      executionTime{}; ///< Execution time in seconds
    };

    /**
     * @brief Progress callback payload for batch executions.
     */
    struct BatchProgress {
        size_t currentCommand{};                 ///< Index of the current command in the batch
        size_t totalCommands{};                  ///< Total number of commands in the batch
        ExecutionResult lastResult{};            ///< Result of the most recently completed command
        bool isComplete{};                       ///< True when the batch has finished
        std::vector<ExecutionResult> allResults{}; ///< Results for all commands (filled at completion)
    };

} // namespace core
} // namespace virtualshell
