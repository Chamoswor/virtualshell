#include "powershell_process.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <thread>
#include <utility>

#ifdef _WIN32
#include <processthreadsapi.h>
#else
#include <csignal>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace std::chrono_literals;

namespace virtualshell {
namespace core {
namespace {
constexpr std::size_t kReadBufferSize = 64 * 1024;

std::string quote_argument(const std::string& value) {
    if (value.empty()) {
        return "\"\"";
    }

    bool needs_quote = value.find_first_of(" \t\"\n") != std::string::npos;
    if (!needs_quote) {
        return value;
    }

    std::string result;
    result.reserve(value.size() + 4);
    result.push_back('"');

    unsigned backslashes = 0;
    for (char ch : value) {
        if (ch == '\\') {
            backslashes += 1;
            result.push_back('\\');
        } else if (ch == '"') {
            result.append(backslashes + 1, '\\');
            result.push_back('"');
            backslashes = 0;
        } else {
            backslashes = 0;
            result.push_back(ch);
        }
    }

    result.push_back('"');
    return result;
}

#ifdef _WIN32
void cancel_io_if_available(HANDLE handle) {
    if (!handle || handle == INVALID_HANDLE_VALUE) {
        return;
    }
    using Fn = BOOL(WINAPI*)(HANDLE, LPOVERLAPPED);
    // Look up CancelIoEx dynamically so we still run on older Windows builds that lack it.
    static Fn fn = reinterpret_cast<Fn>(
        ::GetProcAddress(::GetModuleHandleW(L"kernel32.dll"), "CancelIoEx"));
    if (fn) {
        fn(handle, nullptr);
    }
}
#endif

} // namespace

PowerShellProcess::PowerShellProcess(ProcessConfig config)
    : config_(std::move(config)) {}

PowerShellProcess::~PowerShellProcess() {
    shutdown_streams();
    terminate();
    close_pipes_();
#ifdef _WIN32
    if (process_info_.hThread) {
        CloseHandle(process_info_.hThread);
        process_info_.hThread = nullptr;
    }
    if (process_info_.hProcess) {
        CloseHandle(process_info_.hProcess);
        process_info_.hProcess = nullptr;
    }
#endif
}

bool PowerShellProcess::start() {
    if (running_.load(std::memory_order_acquire)) {
        return false;
    }

    if (!create_pipes_()) {
        return false;
    }

    try {
        if (!spawn_child_()) {
            close_pipes_();
            return false;
        }
    } catch (...) {
        close_pipes_();
        return false;
    }

    running_.store(true, std::memory_order_release);
    return true;
}

void PowerShellProcess::terminate() {
#ifdef _WIN32
    if (!process_info_.hProcess) {
        return;
    }

    if (is_alive()) {
        TerminateProcess(process_info_.hProcess, 1u);
        WaitForSingleObject(process_info_.hProcess, 5000u);
    }
    if (process_info_.hThread) {
        CloseHandle(process_info_.hThread);
        process_info_.hThread = nullptr;
    }
    if (process_info_.hProcess) {
        CloseHandle(process_info_.hProcess);
        process_info_.hProcess = nullptr;
    }
#else
    if (child_pid_ <= 0) {
        return;
    }

    if (kill(child_pid_, SIGTERM) == -1 && errno != ESRCH) {
        kill(child_pid_, SIGKILL);
    }
    int status = 0;
    waitpid(child_pid_, &status, 0);
#endif
    close_pipes_();
    mark_not_running_();
}

bool PowerShellProcess::is_alive() const noexcept {
#ifdef _WIN32
    if (!process_info_.hProcess) {
        return false;
    }
    DWORD exit_code = 0;
    if (!GetExitCodeProcess(process_info_.hProcess, &exit_code)) {
        return false;
    }
    return exit_code == STILL_ACTIVE;
#else
    if (child_pid_ <= 0) {
        return false;
    }
    int status = 0;
    pid_t result = waitpid(child_pid_, &status, WNOHANG);
    if (result == 0) {
        return true;
    }
    if (result == child_pid_) {
        return false;
    }
    return false;
#endif
}

bool PowerShellProcess::write(std::string_view data) {
#ifdef _WIN32
    std::lock_guard<std::mutex> guard(stdin_mutex_);
    return write_pipe_(stdin_write_, data);
#else
    std::lock_guard<std::mutex> guard(stdin_mutex_);
    return write_fd_(stdin_pipe_[1], data);
#endif
}

std::optional<std::string> PowerShellProcess::read_stdout() {
#ifdef _WIN32
    return read_pipe_(stdout_read_);
#else
    return read_fd_(stdout_pipe_[0]);
#endif
}

std::optional<std::string> PowerShellProcess::read_stderr() {
#ifdef _WIN32
    return read_pipe_(stderr_read_);
#else
    return read_fd_(stderr_pipe_[0]);
#endif
}

void PowerShellProcess::shutdown_streams() {
#ifdef _WIN32
    if (stdin_write_) {
        std::lock_guard<std::mutex> guard(stdin_mutex_);
        if (stdin_write_) {
            CloseHandle(stdin_write_);
            stdin_write_ = nullptr;
        }
    }

    if (stdout_read_) {
        cancel_io_if_available(stdout_read_);
        CloseHandle(stdout_read_);
        stdout_read_ = nullptr;
    }

    if (stderr_read_) {
        cancel_io_if_available(stderr_read_);
        CloseHandle(stderr_read_);
        stderr_read_ = nullptr;
    }
#else
    if (stdin_pipe_[1] != -1) {
        std::lock_guard<std::mutex> guard(stdin_mutex_);
        if (stdin_pipe_[1] != -1) {
            close(stdin_pipe_[1]);
            stdin_pipe_[1] = -1;
        }
    }
    if (stdout_pipe_[0] != -1) {
        close(stdout_pipe_[0]);
        stdout_pipe_[0] = -1;
    }
    if (stderr_pipe_[0] != -1) {
        close(stderr_pipe_[0]);
        stderr_pipe_[0] = -1;
    }
#endif
}

bool PowerShellProcess::create_pipes_() {
#ifdef _WIN32
    SECURITY_ATTRIBUTES attrs{};
    attrs.nLength = sizeof(attrs);
    attrs.bInheritHandle = TRUE;

    if (!CreatePipe(&stdin_read_, &stdin_write_, &attrs, 0)) {
        return false;
    }
    SetHandleInformation(stdin_write_, HANDLE_FLAG_INHERIT, 0);

    if (!CreatePipe(&stdout_read_, &stdout_write_, &attrs, 0)) {
        CloseHandle(stdin_read_);
        CloseHandle(stdin_write_);
        stdin_read_ = nullptr;
        stdin_write_ = nullptr;
        return false;
    }
    SetHandleInformation(stdout_read_, HANDLE_FLAG_INHERIT, 0);

    if (!CreatePipe(&stderr_read_, &stderr_write_, &attrs, 0)) {
        CloseHandle(stdin_read_);
        CloseHandle(stdin_write_);
        CloseHandle(stdout_read_);
        CloseHandle(stdout_write_);
        stdin_read_ = nullptr;
        stdin_write_ = nullptr;
        stdout_read_ = nullptr;
        stdout_write_ = nullptr;
        return false;
    }
    SetHandleInformation(stderr_read_, HANDLE_FLAG_INHERIT, 0);
    return true;
#else
    if (pipe(stdin_pipe_) == -1) {
        return false;
    }
    if (pipe(stdout_pipe_) == -1) {
        close(stdin_pipe_[0]);
        close(stdin_pipe_[1]);
        stdin_pipe_[0] = stdin_pipe_[1] = -1;
        return false;
    }
    if (pipe(stderr_pipe_) == -1) {
        close(stdin_pipe_[0]);
        close(stdin_pipe_[1]);
        close(stdout_pipe_[0]);
        close(stdout_pipe_[1]);
        stdin_pipe_[0] = stdin_pipe_[1] = -1;
        stdout_pipe_[0] = stdout_pipe_[1] = -1;
        return false;
    }

    auto set_cloexec = [](int fd) {
        int flags = fcntl(fd, F_GETFD, 0);
        if (flags != -1) {
            fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
        }
    };

    set_cloexec(stdin_pipe_[1]);
    set_cloexec(stdout_pipe_[0]);
    set_cloexec(stderr_pipe_[0]);
    return true;
#endif
}

void PowerShellProcess::close_pipes_() noexcept {
#ifdef _WIN32
    if (stdin_read_) {
        CloseHandle(stdin_read_);
        stdin_read_ = nullptr;
    }
    if (stdin_write_) {
        CloseHandle(stdin_write_);
        stdin_write_ = nullptr;
    }
    if (stdout_read_) {
        CloseHandle(stdout_read_);
        stdout_read_ = nullptr;
    }
    if (stdout_write_) {
        CloseHandle(stdout_write_);
        stdout_write_ = nullptr;
    }
    if (stderr_read_) {
        CloseHandle(stderr_read_);
        stderr_read_ = nullptr;
    }
    if (stderr_write_) {
        CloseHandle(stderr_write_);
        stderr_write_ = nullptr;
    }
#else
    if (stdin_pipe_[0] != -1) {
        close(stdin_pipe_[0]);
        stdin_pipe_[0] = -1;
    }
    if (stdin_pipe_[1] != -1) {
        close(stdin_pipe_[1]);
        stdin_pipe_[1] = -1;
    }
    if (stdout_pipe_[0] != -1) {
        close(stdout_pipe_[0]);
        stdout_pipe_[0] = -1;
    }
    if (stdout_pipe_[1] != -1) {
        close(stdout_pipe_[1]);
        stdout_pipe_[1] = -1;
    }
    if (stderr_pipe_[0] != -1) {
        close(stderr_pipe_[0]);
        stderr_pipe_[0] = -1;
    }
    if (stderr_pipe_[1] != -1) {
        close(stderr_pipe_[1]);
        stderr_pipe_[1] = -1;
    }
#endif
}

void PowerShellProcess::mark_not_running_() noexcept {
    running_.store(false, std::memory_order_release);
}

std::string PowerShellProcess::build_command_line_() const {
    std::vector<std::string> args;
    args.reserve(7 + config_.additional_arguments.size());
    args.push_back(config_.powershell_path);
    args.emplace_back("-NoProfile");
    args.emplace_back("-NonInteractive");
    args.emplace_back("-NoLogo");
    args.emplace_back("-NoExit");
    args.emplace_back("-Command");
    args.emplace_back("-");
    args.insert(args.end(), config_.additional_arguments.begin(), config_.additional_arguments.end());

    // Compose a Windows-friendly command line by quoting each argument as needed.
    std::string command_line;
    bool first = true;
    for (const auto& arg : args) {
        if (!first) {
            command_line.push_back(' ');
        }
        first = false;
        command_line += quote_argument(arg);
    }
    return command_line;
}

#ifdef _WIN32
std::vector<wchar_t> PowerShellProcess::build_environment_block_wide_() const {
    if (config_.environment.empty()) {
        return {};
    }

    std::vector<wchar_t> block;
    for (const auto& [key, value] : config_.environment) {
        std::wstring entry = to_wide_(key + "=" + value);
        block.insert(block.end(), entry.begin(), entry.end());
        block.push_back(L'\0');
    }
    block.push_back(L'\0');
    return block;
}

std::wstring PowerShellProcess::to_wide_(const std::string& value) const {
    if (value.empty()) {
        return std::wstring();
    }
    int needed = MultiByteToWideChar(CP_UTF8, 0, value.c_str(), -1, nullptr, 0);
    if (needed <= 0) {
        throw std::runtime_error("Failed to convert string to UTF-16");
    }
    std::wstring result;
    result.resize(static_cast<std::size_t>(needed - 1));
    MultiByteToWideChar(CP_UTF8, 0, value.c_str(), -1, result.data(), needed);
    return result;
}

bool PowerShellProcess::spawn_child_() {
    std::wstring cmd_w = to_wide_(build_command_line_());
    std::vector<wchar_t> cmd_buffer(cmd_w.begin(), cmd_w.end());
    cmd_buffer.push_back(L'\0');

    std::vector<wchar_t> env_block = build_environment_block_wide_();

    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;
    startup.hStdInput = stdin_read_;
    startup.hStdOutput = stdout_write_;
    startup.hStdError = stderr_write_;
    startup.wShowWindow = SW_HIDE;

    std::wstring working_dir_w;
    LPCWSTR working_dir_ptr = nullptr;
    if (!config_.working_directory.empty()) {
        working_dir_w = to_wide_(config_.working_directory);
        working_dir_ptr = working_dir_w.c_str();
    }

    PROCESS_INFORMATION pi{};
    DWORD creation_flags = CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP;

    BOOL ok = CreateProcessW(
        nullptr,
        cmd_buffer.data(),
        nullptr,
        nullptr,
        TRUE,
        creation_flags,
        env_block.empty() ? nullptr : env_block.data(),
        working_dir_ptr,
        &startup,
        &pi);

    if (!ok) {
        return false;
    }

    process_info_ = pi;

    // Close inherited ends in the parent so only the child keeps them open.
    if (stdin_read_) {
        CloseHandle(stdin_read_);
        stdin_read_ = nullptr;
    }
    if (stdout_write_) {
        CloseHandle(stdout_write_);
        stdout_write_ = nullptr;
    }
    if (stderr_write_) {
        CloseHandle(stderr_write_);
        stderr_write_ = nullptr;
    }
    return true;
}

std::optional<std::string> PowerShellProcess::read_pipe_(HANDLE& handle) {
    if (!handle) {
        return std::nullopt;
    }

    std::array<char, kReadBufferSize> buffer{};
    DWORD bytes_read = 0;
    BOOL ok = ReadFile(handle, buffer.data(), static_cast<DWORD>(buffer.size()), &bytes_read, nullptr);
    if (!ok || bytes_read == 0) {
        DWORD err = GetLastError();
        if (err == ERROR_BROKEN_PIPE || err == ERROR_INVALID_HANDLE || err == ERROR_OPERATION_ABORTED) {
            CloseHandle(handle);
            handle = nullptr;
            return std::nullopt;
        }
        return std::nullopt;
    }

    return std::string(buffer.data(), bytes_read);
}

bool PowerShellProcess::write_pipe_(HANDLE handle, std::string_view data) {
    if (!handle) {
        return false;
    }

    // Write until all bytes are delivered or the pipe breaks.
    const char* ptr = data.data();
    std::size_t remaining = data.size();
    while (remaining > 0) {
        DWORD chunk = 0;
        DWORD to_write = static_cast<DWORD>(std::min<std::size_t>(remaining, static_cast<std::size_t>(std::numeric_limits<DWORD>::max())));
        BOOL ok = WriteFile(handle, ptr, to_write, &chunk, nullptr);
        if (!ok) {
            DWORD err = GetLastError();
            if (err == ERROR_BROKEN_PIPE || err == ERROR_INVALID_HANDLE) {
                return false;
            }
            return false;
        }
        if (chunk == 0) {
            return false;
        }
        ptr += chunk;
        remaining -= chunk;
    }
    return true;
}

#else

bool PowerShellProcess::spawn_child_() {
    child_pid_ = fork();
    if (child_pid_ == -1) {
        return false;
    }

    if (child_pid_ == 0) {
        // Child: wire up pipes and exec PowerShell.
        dup2(stdin_pipe_[0], STDIN_FILENO);
        dup2(stdout_pipe_[1], STDOUT_FILENO);
        dup2(stderr_pipe_[1], STDERR_FILENO);

        close(stdin_pipe_[0]);
        close(stdin_pipe_[1]);
        close(stdout_pipe_[0]);
        close(stdout_pipe_[1]);
        close(stderr_pipe_[0]);
        close(stderr_pipe_[1]);

        if (!config_.working_directory.empty()) {
            if (chdir(config_.working_directory.c_str()) != 0) {
                _exit(127);
            }
        }

        for (const auto& [key, value] : config_.environment) {
            setenv(key.c_str(), value.c_str(), 1);
        }

        std::vector<std::string> args;
        args.reserve(7 + config_.additional_arguments.size());
        args.push_back(config_.powershell_path);
        args.emplace_back("-NoProfile");
        args.emplace_back("-NonInteractive");
        args.emplace_back("-NoLogo");
        args.emplace_back("-NoExit");
        args.emplace_back("-Command");
        args.emplace_back("-");
        args.insert(args.end(), config_.additional_arguments.begin(), config_.additional_arguments.end());

        std::vector<char*> argv;
        for (auto& arg : args) {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        argv.push_back(nullptr);

        execvp(argv[0], argv.data());
        _exit(127);
    }

    // Parent: close pipe ends that belong to the child process.
    close(stdin_pipe_[0]);
    stdin_pipe_[0] = -1;
    close(stdout_pipe_[1]);
    stdout_pipe_[1] = -1;
    close(stderr_pipe_[1]);
    stderr_pipe_[1] = -1;

    return true;
}

std::optional<std::string> PowerShellProcess::read_fd_(int& fd) {
    if (fd == -1) {
        return std::nullopt;
    }

    std::array<char, kReadBufferSize> buffer{};
    ssize_t count = read(fd, buffer.data(), buffer.size());
    if (count <= 0) {
        if (count == 0 || errno == EPIPE || errno == EBADF) {
            close(fd);
            fd = -1;
        }
        return std::nullopt;
    }
    return std::string(buffer.data(), static_cast<std::size_t>(count));
}

bool PowerShellProcess::write_fd_(int fd, std::string_view data) {
    if (fd == -1) {
        return false;
    }

    // Write the entire buffer, retrying on EINTR/EAGAIN to accommodate pipes.
    const char* ptr = data.data();
    std::size_t remaining = data.size();
    while (remaining > 0) {
        ssize_t written = ::write(fd, ptr, remaining);
        if (written > 0) {
            ptr += written;
            remaining -= static_cast<std::size_t>(written);
            continue;
        }
        if (written == -1 && errno == EINTR) {
            continue;
        }
        if (written == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            std::this_thread::sleep_for(200us);
            continue;
        }
        return false;
    }
    return true;
}

#endif

} // namespace core
} // namespace virtualshell
