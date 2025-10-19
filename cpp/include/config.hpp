#pragma once

#include <map>
#include <string>
#include <vector>

namespace virtualshell {
namespace core {
/**
 * @brief Runtime configuration supplied to the VirtualShell engine.
 */
struct Config {
	std::string powershellPath{"pwsh"};     ///< Absolute or relative path to the PowerShell executable
	std::string workingDirectory{""};       ///< Working directory for the child process (empty = current)
	bool captureOutput{true};               ///< Capture stdout content produced by commands
	bool captureError{true};                ///< Capture stderr content produced by commands
	bool autoRestartOnTimeout{true};        ///< Restart the backing process when a command times out
	int  timeoutSeconds{30};                ///< Default per-command timeout (seconds, 0 disables)
	std::map<std::string, std::string> environment;   ///< Additional environment variables merged into the child
	std::vector<std::string> initialCommands;         ///< Commands executed immediately after start/ restart
	std::string restoreScriptPath{""};       ///< Optional path to a restore script invoked after start
	std::string sessionSnapshotPath{""};     ///< Optional path where session snapshots are persisted
};
} // namespace core
} // namespace virtualshell
