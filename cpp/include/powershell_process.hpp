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

struct ProcessConfig {
    std::string powershell_path{"pwsh"};
    std::string working_directory{};
    std::map<std::string, std::string> environment{};
    std::vector<std::string> additional_arguments{};
};

class PowerShellProcess final : public Process {
public:
    explicit PowerShellProcess(ProcessConfig config);
    ~PowerShellProcess() override;

    PowerShellProcess(const PowerShellProcess&) = delete;
    PowerShellProcess& operator=(const PowerShellProcess&) = delete;
    PowerShellProcess(PowerShellProcess&&) = delete;
    PowerShellProcess& operator=(PowerShellProcess&&) = delete;

    bool start();
    void terminate();
    bool is_alive() const noexcept;

    bool write(std::string_view data) override;
    std::optional<std::string> read_stdout() override;
    std::optional<std::string> read_stderr() override;
    void shutdown_streams() override;

#ifdef _WIN32
    HANDLE native_process_handle() const noexcept { return process_info_.hProcess; }
#else
    pid_t native_pid() const noexcept { return child_pid_; }
#endif

private:
    bool spawn_child_();
    bool create_pipes_();
    void close_pipes_() noexcept;
    void mark_not_running_() noexcept;

#ifdef _WIN32
    std::optional<std::string> read_pipe_(HANDLE& handle);
    bool write_pipe_(HANDLE handle, std::string_view data);
#else
    std::optional<std::string> read_fd_(int& fd);
    bool write_fd_(int fd, std::string_view data);
#endif

    std::string build_command_line_() const;
#ifdef _WIN32
    std::vector<wchar_t> build_environment_block_wide_() const;
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
