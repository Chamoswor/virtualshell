#pragma once

#include <map>
#include <string>
#include <vector>

namespace virtualshell {
namespace core {
struct Config {
	std::string powershellPath{"pwsh"};     ///< Path to the PowerShell executable
	std::string workingDirectory{""};       ///< Working directory (empty = current directory)
	bool captureOutput{true};               ///< Capture stdout
	bool captureError{true};                ///< Capture stderr
	bool autoRestartOnTimeout{true};        ///< If true, restart the process on command timeout
	int  timeoutSeconds{30};                ///< Default per-command timeout (seconds)
	std::map<std::string, std::string> environment;   ///< Extra environment variables
	std::vector<std::string> initialCommands;         ///< Commands to run right after startup
	std::string restoreScriptPath{""};       ///< Optional path to get-session.ps1
	std::string sessionSnapshotPath{""};     ///< Optional path to session_{RUN-ID}.xml
};
} // namespace core
} // namespace virtualshell
