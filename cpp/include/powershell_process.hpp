#pragma once

#include "process.hpp"

#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <sys/types.h>
#endif

namespace virtualshell {
namespace core {

/**
 * @brief Runtime configuration for launching the PowerShell host process.
 */
struct ProcessConfig {
    std::string powershell_path{"pwsh"};          ///< Executable to launch (pwsh or powershell)
    std::string working_directory{};               ///< Working directory passed to CreateProcess/posix_spawn
    std::map<std::string, std::string> environment{}; ///< Extra environment variables for the child
    std::vector<std::string> additional_arguments{};  ///< Additional command-line arguments appended verbatim
};

/**
 * @brief Small RAII wrapper around the low-level PowerShell child process.
 *
 * This class encapsulates pipe setup, cross-platform spawning, and safe
 * teardown. It exposes a Process interface consumed by VirtualShell or tests.
 */
class PowerShellProcess final : public Process {
public:
    /**
     * @brief Construct with the given process configuration snapshot.
     */
    explicit PowerShellProcess(ProcessConfig config);
    ~PowerShellProcess() override;

    PowerShellProcess(const PowerShellProcess&) = delete;
    PowerShellProcess& operator=(const PowerShellProcess&) = delete;
    PowerShellProcess(PowerShellProcess&&) = delete;
    PowerShellProcess& operator=(PowerShellProcess&&) = delete;

    /**
     * @brief Launch the PowerShell process (idempotent when already running).
     */
    bool start();
    /**
     * @brief Terminate the child process immediately, closing pipes.
     */
    void terminate();
    /**
     * @brief Check whether the process is currently alive.
     */
    bool is_alive() const noexcept;

    /**
     * @brief Write bytes to the child stdin pipe.
     */
    bool write(std::string_view data) override;
    /**
     * @brief Read pending data from stdout (non-blocking, returns nullopt when empty).
     */
    std::optional<std::string> read_stdout() override;
    /**
     * @brief Read pending data from stderr (non-blocking, returns nullopt when empty).
     */
    std::optional<std::string> read_stderr() override;
    /**
     * @brief Close stdin/stdout/stderr pipes so the child can exit gracefully.
     */
    void shutdown_streams() override;

#ifdef _WIN32
    /**
     * @brief Access the native process handle for integration with Windows facilities.
     */
    HANDLE native_process_handle() const noexcept { return process_info_.hProcess; }
#else
    /**
     * @brief Access the child pid for integration with POSIX facilities.
     */
    pid_t native_pid() const noexcept { return child_pid_; }
#endif

private:
    /**
     * @brief Spawn the PowerShell process using the configured options.
     */
    bool spawn_child_();
    /**
     * @brief Allocate and configure stdin/stdout/stderr pipes.
     */
    bool create_pipes_();
    /**
     * @brief Best-effort cleanup of pipe handles/descriptors.
     */
    void close_pipes_() noexcept;
    /**
     * @brief Mark running_ false and clear bookkeeping after child exit.
     */
    void mark_not_running_() noexcept;

#ifdef _WIN32
    /**
     * @brief Read from a Windows HANDLE pipe into a string (non-blocking).
     */
    std::optional<std::string> read_pipe_(HANDLE& handle);
    /**
     * @brief Write a string_view to a Windows pipe HANDLE.
     */
    bool write_pipe_(HANDLE handle, std::string_view data);
#else
    /**
     * @brief Read from a POSIX file descriptor into a string (non-blocking).
     */
    std::optional<std::string> read_fd_(int& fd);
    /**
     * @brief Write a string_view to a POSIX file descriptor.
     */
    bool write_fd_(int fd, std::string_view data);
#endif

    /**
     * @brief Build the full command line passed to PowerShell.
     */
    std::string build_command_line_() const;
#ifdef _WIN32
    /**
     * @brief Create a Windows environment block for CreateProcessW.
     */
    std::vector<wchar_t> build_environment_block_wide_() const;
    /**
     * @brief Convert UTF-8 to UTF-16 wide strings for Windows APIs.
     */
    std::wstring to_wide_(const std::string& value) const;
#endif

    ProcessConfig config_;
    std::atomic<bool> running_{false};

#ifdef _WIN32
    HANDLE stdin_read_{nullptr};
    HANDLE stdin_write_{nullptr};
    HANDLE stdout_read_{nullptr};
    HANDLE stdout_write_{nullptr};
    HANDLE stderr_read_{nullptr};
    HANDLE stderr_write_{nullptr};
    PROCESS_INFORMATION process_info_{};
#else
    int stdin_pipe_[2]{-1, -1};
    int stdout_pipe_[2]{-1, -1};
    int stderr_pipe_[2]{-1, -1};
    pid_t child_pid_{-1};
#endif

    mutable std::mutex stdin_mutex_;
};

} // namespace core
} // namespace virtualshell
