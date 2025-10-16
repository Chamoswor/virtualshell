#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#endif
#include "../include/virtual_shell.hpp"
#include "../include/helpers.h"
#include <chrono>
#include <algorithm>
#include <fstream>
#include <limits> 
#include <array>
#include <filesystem>
#include <map>
#if not defined(_WIN32)
#include <sstream>
#endif

#include "virtual_shell_debug.hpp"
using namespace VirtualShellDebug;
using namespace _VirtualShellHelpers;

namespace fs = std::filesystem;

#ifdef _WIN32
namespace {
BOOL cancel_io_ex_optional(HANDLE h) {
    if (!h || h == INVALID_HANDLE_VALUE) {
        return FALSE;
    }
    using Fn = BOOL (WINAPI *)(HANDLE, LPOVERLAPPED);
    static Fn fn = reinterpret_cast<Fn>(
        ::GetProcAddress(::GetModuleHandleW(L"kernel32.dll"), "CancelIoEx"));
    if (!fn) {
        return FALSE;
    }
    return fn(h, nullptr);
}

void cancel_thread_io_optional(HANDLE threadHandle) {
    if (!threadHandle) {
        return;
    }
    using Fn = BOOL (WINAPI *)(HANDLE);
    static Fn fn = reinterpret_cast<Fn>(
        ::GetProcAddress(::GetModuleHandleW(L"kernel32.dll"), "CancelSynchronousIo"));
    if (!fn) {
        return;
    }
    fn(threadHandle);
}
}
#endif

constexpr auto DOT_SOURCE_PREFIX = ". ";
constexpr auto NO_SOURCE_PREFIX  = "& ";


VirtualShell::VirtualShell(const Config& config) : config(config) {
    // Store configuration, actual process startup is deferred until start().
}

VirtualShell::~VirtualShell() {
    // Ensure process and IO are fully stopped before cleanup.
    if (isRunning_) {
        stop(true);
    }
    closePipes();
}

VirtualShell::VirtualShell(VirtualShell&& other) noexcept
    : config(std::move(other.config))
{
    // If source is running, force-stop it before stealing resources.
    if (other.isRunning_) {
        other.stop(true);
    }

#ifdef _WIN32
    // Transfer Win32 pipe/process handles from 'other' and null them out.
    hInputWrite  = other.hInputWrite;  other.hInputWrite  = NULL;
    hInputRead   = other.hInputRead;   other.hInputRead   = NULL;
    hOutputWrite = other.hOutputWrite; other.hOutputWrite = NULL;
    hOutputRead  = other.hOutputRead;  other.hOutputRead  = NULL;
    hErrorWrite  = other.hErrorWrite;  other.hErrorWrite  = NULL;
    hErrorRead   = other.hErrorRead;   other.hErrorRead   = NULL;
    hProcess     = other.hProcess;     other.hProcess     = NULL;
    hThread      = other.hThread;      other.hThread      = NULL;
    processInfo  = other.processInfo;  other.processInfo  = {};
#else
    // Transfer pipe file descriptors / pid on POSIX.
    inputPipe[0]  = other.inputPipe[0];  other.inputPipe[0]  = -1;
    inputPipe[1]  = other.inputPipe[1];  other.inputPipe[1]  = -1;
    outputPipe[0] = other.outputPipe[0]; other.outputPipe[0] = -1;
    outputPipe[1] = other.outputPipe[1]; other.outputPipe[1] = -1;
    errorPipe[0]  = other.errorPipe[0];  other.errorPipe[0]  = -1;
    errorPipe[1]  = other.errorPipe[1];  other.errorPipe[1]  = -1;
    processId     = other.processId;     other.processId     = -1;
#endif

    // Transfer ownership of IO threads.
    writerTh_ = std::move(other.writerTh_);
    rOutTh_   = std::move(other.rOutTh_);
    rErrTh_   = std::move(other.rErrTh_);

    // Reset state flags; new object must re-start IO explicitly.
    isRunning_  = false;
    ioRunning_  = false;

    // Move queues safely under lock; inflight states are discarded.
    {
        std::scoped_lock lk(other.writeMx_);
        writeQueue_ = std::move(other.writeQueue_);
    }
    {
        std::scoped_lock lk(other.chunkMx_);
        chunkQueue_ = std::move(other.chunkQueue_);
    }
    {
        std::scoped_lock lk(other.stateMx_);
        inflight_.clear();
    }

    // Transfer sequence counter.
    seq_.store(other.seq_.load());

    // Ensure donor is marked stopped.
    other.isRunning_ = false;
    other.ioRunning_ = false;
}

VirtualShell& VirtualShell::operator=(VirtualShell&& other) noexcept {
    if (this == &other) return *this;

    // Ensure this instance is quiescent before stealing resources.
    if (isRunning_) {
        stop(true);
    }
    closePipes(); // release any leftover handles/FDs

    // Ensure source instance is also quiescent.
    if (other.isRunning_) {
        other.stop(true);
    }

    // Transfer configuration.
    config = std::move(other.config);

#ifdef _WIN32
    // Steal Win32 handles and null out donor.
    hInputWrite  = other.hInputWrite;  other.hInputWrite  = NULL;
    hInputRead   = other.hInputRead;   other.hInputRead   = NULL;
    hOutputWrite = other.hOutputWrite; other.hOutputWrite = NULL;
    hOutputRead  = other.hOutputRead;  other.hOutputRead  = NULL;
    hErrorWrite  = other.hErrorWrite;  other.hErrorWrite  = NULL;
    hErrorRead   = other.hErrorRead;   other.hErrorRead   = NULL;
    hProcess     = other.hProcess;     other.hProcess     = NULL;
    hThread      = other.hThread;      other.hThread      = NULL;
    processInfo  = other.processInfo;  other.processInfo  = {};
#else
    // Steal POSIX FDs and pid; poison donor.
    inputPipe[0]  = other.inputPipe[0];  other.inputPipe[0]  = -1;
    inputPipe[1]  = other.inputPipe[1];  other.inputPipe[1]  = -1;
    outputPipe[0] = other.outputPipe[0]; other.outputPipe[0] = -1;
    outputPipe[1] = other.outputPipe[1]; other.outputPipe[1] = -1;
    errorPipe[0]  = other.errorPipe[0];  other.errorPipe[0]  = -1;
    errorPipe[1]  = other.errorPipe[1];  other.errorPipe[1]  = -1;
    processId     = other.processId;     other.processId     = -1;
#endif

    // Transfer thread objects (no threads are running now).
    writerTh_ = std::move(other.writerTh_);
    rOutTh_   = std::move(other.rOutTh_);
    rErrTh_   = std::move(other.rErrTh_);

    // New owner is in a stopped state until start() is called.
    isRunning_ = false;
    ioRunning_ = false;

    // Move queues under the donor's locks to avoid data races.
    {
        std::scoped_lock lk(other.writeMx_);
        writeQueue_ = std::move(other.writeQueue_);
    }
    {
        std::scoped_lock lk(other.chunkMx_);
        chunkQueue_ = std::move(other.chunkQueue_);
    }
    {
        std::scoped_lock lk(other.stateMx_);
        inflight_.clear(); // never adopt inflight commands (promises belong to donor)
    }

    // Transfer sequence ID for marker generation.
    seq_.store(other.seq_.load());

    // Explicitly mark donor as stopped.
    other.isRunning_ = false;
    other.ioRunning_ = false;

    return *this;
}

bool VirtualShell::start() {
    if (isRunning_) return false; // Already running; do not re-spawn a second process instance.

    if (!createPipes()) {
        return false; // Pipe setup failed; cannot attach stdio to child.
    }

    VSHELL_DBG("LIFECYCLE", "start() pwsh_path='%s'", config.powershellPath.c_str());

#ifdef _WIN32
    STARTUPINFOA startupInfo = {};
    startupInfo.cb = sizeof(STARTUPINFOA);
    startupInfo.dwFlags = STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;
    startupInfo.hStdInput  = hInputRead;
    startupInfo.hStdOutput = hOutputWrite;
    startupInfo.hStdError  = hErrorWrite;
    startupInfo.wShowWindow = SW_HIDE; // Spawn hidden (no visible window).

    std::string commandLine = config.powershellPath + " -NoProfile -NoExit -Command -";
    const char* workDir = config.workingDirectory.empty() ? nullptr : config.workingDirectory.c_str();

    // Build environment block for CreateProcessA (ANSI). Must be double-NUL terminated and
    // use "key=value\0" entries. Keep the backing string alive through CreateProcessA.
    std::string envStr;
    LPVOID envBlock = nullptr;
    if (!config.environment.empty()) {
        for (const auto& [k,v] : config.environment) {
            envStr.append(k).push_back('=');
            envStr.append(v).push_back('\0');
        }
        envStr.push_back('\0'); // double NUL
        envBlock = envStr.empty() ? nullptr : (LPVOID)envStr.data();
    }

    DWORD flags = CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP; // New group: enables Ctrl handling/termination later.
    SetConsoleCtrlHandler(NULL, TRUE); // Detach from CTRL_C events in parent to avoid propagating to child.

    // Note: bInheritHandles=TRUE requires the pipe handles to be inheritable and the *child sides* wired above.
    BOOL ok = CreateProcessA(
        nullptr,
        const_cast<char*>(commandLine.c_str()),
        nullptr, nullptr,
        TRUE, // inherit handles
        flags,
        envBlock,   // Null => inherit parent's env; non-null => use provided block.
        workDir,    // Null => inherit parent's CWD.
        &startupInfo,
        &processInfo
    );
    if (!ok) {
        closePipes(); // Best effort cleanup on spawn failure.
        return false;
    }

    hProcess = processInfo.hProcess;
    hThread  = processInfo.hThread;

    // Parent must close its copies of the child's ends to avoid deadlocks and to enable EOF signaling.
    if (hInputRead)  { CloseHandle(hInputRead);  hInputRead  = NULL; }
    if (hOutputWrite){ CloseHandle(hOutputWrite);hOutputWrite = NULL; }
    if (hErrorWrite) { CloseHandle(hErrorWrite); hErrorWrite  = NULL; }

#else
    processId = fork();
    if (processId == -1) {
        closePipes(); // Fork failed; release FDs.
        return false;
    }

    if (processId == 0) {
        // --- Child process context ---
        // Wire up stdio to the *read end* of stdin pipe and *write ends* of out/err pipes.
        dup2(inputPipe[0],  STDIN_FILENO);
        dup2(outputPipe[1], STDOUT_FILENO);
        dup2(errorPipe[1],  STDERR_FILENO);

        // Close all pipe FDs we no longer need in the child to prevent descriptor leaks and hanging writers.
        close(inputPipe[0]);  close(inputPipe[1]);
        close(outputPipe[0]); close(outputPipe[1]);
        close(errorPipe[0]);  close(errorPipe[1]);

        if (!config.workingDirectory.empty()) {
            if (chdir(config.workingDirectory.c_str()) != 0) {
                perror("chdir");
            }

        }
        for (const auto& [k,v] : config.environment) {
            setenv(k.c_str(), v.c_str(), 1); // Overwrite existing entries.
        }

        // Kjør pwsh og les skript fra STDIN eksplisitt.
        execlp(config.powershellPath.c_str(), config.powershellPath.c_str(),
           "-NoProfile", "-NonInteractive", "-NoLogo", "-NoExit",
           "-Command", "-", (char*)nullptr);

        perror("execlp pwsh");
        _exit(127);
    } else {
        // --- Parent process context ---
        // Close the child's ends in the parent so our IO threads talk only to the intended ends.
        close(inputPipe[0]);   inputPipe[0]  = -1;
        close(outputPipe[1]);  outputPipe[1] = -1;
        close(errorPipe[1]);   errorPipe[1]  = -1;

        int status = 0;
        for (int i = 0; i < 20; ++i) { // ~200 ms
            pid_t r = waitpid(processId, &status, WNOHANG);
            if (r == processId) { closePipes(); processId = -1; return false; }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
#endif

    isRunning_ = true; // Mark process as alive before spinning up IO threads.

    // Start new I/O engine threads (writer + stdout/stderr readers). These must start
    // after isRunning_ to avoid races on early reads/writes.
    startIoThreads_(); // <- (spawns: writerTh_, rOutTh_, rErrTh_)
    ioRunning_ = true;

    // Start watchdog that enforces per-command timeouts.
    timerRun_ = true;
    timerThread_ = std::thread([this]{ timeoutScan_(); });

    // Non-fatal warm-up: primes the pipeline and validates basic command path.

    (void)this->execute("$null | Out-Null", /*timeoutSeconds=*/5.0);
    (void)sendInitialCommands(); // Optional user/preset initialization; ignore failures.
    if (!config.restoreScriptPath.empty() && !config.sessionSnapshotPath.empty()) {
        VSHELL_DBG("LIFECYCLE",
                   "restore check restore='%s' snapshot='%s'",
                   config.restoreScriptPath.c_str(),
                   config.sessionSnapshotPath.c_str());
        try {
            fs::path snapshot = fs::u8path(config.sessionSnapshotPath);
            const bool hasSnapshot = fs::exists(snapshot);
            VSHELL_DBG("LIFECYCLE", "restore snapshot exists=%d", int(hasSnapshot));
            if (hasSnapshot) {
                fs::path restore = fs::u8path(config.restoreScriptPath);
                std::string restore_u8 = restore.u8string();
                std::string snapshot_u8 = snapshot.u8string();
                std::string command;
                command.reserve(restore_u8.size() + snapshot_u8.size() + 32);
                command += ". ";
                command += ps_quote(restore_u8);
                command += " -Path ";
                command += ps_quote(snapshot_u8);

                double restoreTimeout = config.timeoutSeconds > 0 ? static_cast<double>(config.timeoutSeconds)
                                                                  : 5.0;
                auto fut = this->submit(command, restoreTimeout, nullptr, /*bypassRestart=*/true);
                auto restoreResult = fut.get();
                if (!restoreResult.success) {
                    VSHELL_DBG("LIFECYCLE", "session restore failed exit=%d err='%s'",
                               restoreResult.exitCode,
                               restoreResult.err.c_str());
                } else {
                    VSHELL_DBG("LIFECYCLE", "session restore succeeded");
                }
            }
        } catch (const std::exception& ex) {
            VSHELL_DBG("LIFECYCLE", "session restore threw exception: %s", ex.what());
        } catch (...) {
            VSHELL_DBG("LIFECYCLE", "session restore threw unknown exception");
        }
    }
    isRestarting_.store(false, std::memory_order_release);

    return true;
}

void VirtualShell::stop(bool force) {
    std::unique_lock<std::mutex> stopLock(stopMx_);
    // Fast exit if process not running
    if (!isRunning_) {
        lifecycleGate_.store(false, std::memory_order_release);
        return; // Idempotent: safe if stop() is called multiple times.
    }

    lifecycleGate_.store(true, std::memory_order_release);

    VSHELL_DBG("LIFECYCLE", "stop(force=%d)", int(force));

    // 1) Signal new I/O engine to stop
    ioRunning_ = false;          // Request cooperative shutdown for writer/reader loops.
    writeCv_.notify_all();       // Wake writerLoop_() if it's waiting on empty queue.
    chunkCv_.notify_all();       // Wake any chunk parser; harmless if not currently waiting.

#ifdef _WIN32
    auto cancelThreadIo = [](std::thread& th) {
        if (!th.joinable()) return;
        HANDLE native = th.native_handle();
        cancel_thread_io_optional(native);
    };

    cancelThreadIo(rOutTh_);
    cancelThreadIo(rErrTh_);
    cancelThreadIo(writerTh_);
#endif

    // 2) Try graceful termination of PowerShell
    (void)sendInput("exit\n");   // Best-effort: if stdin is still open, ask the shell to exit cleanly.

    // 3) Close pipe ends to break blocking reads/writes in reader loops
#ifdef _WIN32
    cancel_io_ex_optional(hOutputRead);
    cancel_io_ex_optional(hErrorRead);
    cancel_io_ex_optional(hInputWrite);
    if (hInputWrite) { CloseHandle(hInputWrite); hInputWrite = NULL; }  // stdin (write end)
    if (hOutputRead) { CloseHandle(hOutputRead); hOutputRead = NULL; }  // stdout (read end)
    if (hErrorRead)  { CloseHandle(hErrorRead);  hErrorRead  = NULL; }  // stderr (read end)
#else
    if (inputPipe[1]  != -1) { ::close(inputPipe[1]);  inputPipe[1]  = -1; } // stdin write
    if (outputPipe[0] != -1) { ::close(outputPipe[0]); outputPipe[0] = -1; } // stdout read
    if (errorPipe[0]  != -1) { ::close(errorPipe[0]);  errorPipe[0]  = -1; } // stderr read
#endif
    // Note: Closing the local ends ensures blocking I/O in the worker threads is interrupted
    // (read() returns 0 / GetOverlappedResult fails), allowing threads to observe ioRunning_ = false.

    // 4) Join I/O threads (they should wake due to handle close + ioRunning_=false)
    // WARNING: stop() must not be called *from* any of these threads; otherwise join() would deadlock.
    if (rOutTh_.joinable())   rOutTh_.join();   // Wait for stdout reader to drain/exit.
    if (rErrTh_.joinable())   rErrTh_.join();   // Wait for stderr reader to drain/exit.
    if (writerTh_.joinable()) writerTh_.join(); // Ensure no further writes to child stdin.

    // 5) Mark process not running and fail any inflight commands
    isRunning_ = false;        // From this point, execute() should refuse new work.
    timerRun_ = false;         // Stop the timeout watchdog loop.
    if (timerThread_.joinable()) timerThread_.join(); // Ensure no races with inflight_ mutation below.

    {
        std::lock_guard<std::mutex> lk(stateMx_);
        // Fail all pending commands so waiters/promises are resolved deterministically.
        for (auto &kv : inflight_) {
            CmdState &S = *kv.second;
            if (!S.done.load()) {
                S.errBuf.append("Process stopped.\n");
                // completeCmdLocked_ requires stateMx_ held; marks futures/promises as finished.
                completeCmdLocked_(S, /*success=*/false);
            }
        }
        inflight_.clear();
    }

    // 6) Clear queues
    {
        std::lock_guard<std::mutex> lk(writeMx_);
        writeQueue_.clear(); // Drop any buffered writes that never reached the child.
    }
    {
        std::lock_guard<std::mutex> lk(chunkMx_);
        chunkQueue_.clear(); // Discard partial/queued parse chunks.
    }

    // 7) Wait for process to exit; force if requested
    bool exited = waitForProcess(force ? 0 : 5000); // Best-effort graceful wait (ms).
    if (!exited && force) {
#ifdef _WIN32
        if (hProcess) {
            // Last resort: hard-kill the process tree root. Caller opted-in via 'force'.
            TerminateProcess(hProcess, 1);
            (void)WaitForSingleObject(hProcess, 5000);
        }
#else
        if (processId > 0) {
            // Try polite SIGTERM first, then fallback to SIGKILL if still alive.
            kill(processId, SIGTERM);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            kill(processId, SIGKILL);
            int st = 0; waitpid(processId, &st, 0);
        }
#endif
    }
    // Note: Child-subprocess groups (grandchildren) may survive on POSIX unless started in their own
    // process group/new session; this function only targets the direct child by default.

    // 8) Close remaining process handles/ids
#ifdef _WIN32
    if (hProcess) { CloseHandle(hProcess); hProcess = NULL; } // Release kernel handle to process object.
    if (hThread)  { CloseHandle(hThread);  hThread  = NULL; } // Release primary thread handle.
#else
    processId = -1; // Mark as no longer valid. FDs already closed above.
#endif

    lifecycleGate_.store(false, std::memory_order_release);
}

bool VirtualShell::isAlive() const {
    if (!isRunning_) {
        return false;
    }
    
#ifdef _WIN32
    if (!hProcess) {
        return false;
    }
    
    DWORD exitCode;
    if (GetExitCodeProcess(hProcess, &exitCode)) {
        return exitCode == STILL_ACTIVE;
    }
    return false;
#else
    if (processId <= 0) {
        return false;
    }
    
    int status;
    pid_t result = waitpid(processId, &status, WNOHANG);
    return result == 0; // 0 means still running
#endif
}


VirtualShell::ExecutionResult
VirtualShell::execute(const std::string& command, double timeoutSeconds)
{
    auto fut = submit(command, timeoutSeconds, nullptr);
    const double to = (timeoutSeconds>0?timeoutSeconds:config.timeoutSeconds); // Per-call override, else default.

    // Wait up to 'to' seconds for completion; do not block indefinitely.
    if (fut.wait_for(std::chrono::duration<double>(to)) == std::future_status::ready)
        return fut.get();

    // Timeout path: return a synthetic timeout result immediately.
    // NOTE: The underlying command may still complete later; parser must ignore late chunks
    // for this command. Ensure submit()/timeoutScan_ marks the CmdState as "timed out".
    ExecutionResult r{}; r.success=false; r.exitCode=-1; r.err="timeout";
    return r;
}

std::future<VirtualShell::ExecutionResult>
VirtualShell::executeAsync(std::string command,
                           std::function<void(const ExecutionResult&)> callback, double timeoutSeconds)
{
    return submit(std::move(command), timeoutSeconds, std::move(callback));
}

VirtualShell::ExecutionResult VirtualShell::execute_script(
    const std::string& scriptPath,
    const std::vector<std::string>& args,
    double timeoutSeconds,
    bool dotSource,
    bool /*raiseOnError*/)
{
    fs::path abs = fs::absolute(scriptPath);

    if (!fs::exists(abs)) {
        ExecutionResult r{};
        r.err    = "Could not open script file: " + scriptPath;
        r.exitCode = -1;
        r.success  = false;
        return r;
    }

    // Build a PS array of quoted args and pass via splatting (@__args__).
    // Rationale: avoids command-line length issues and preserves exact arg boundaries.
    std::string argArray = "@(";
    for (size_t i = 0; i < args.size(); ++i) {
        if (i) argArray += ", ";
        argArray += ps_quote(args[i]); // Safe literal quoting for PowerShell.
    }
    argArray += ")";

#ifdef _WIN32
    // Convert wide native path to UTF-8 so ps_quote() can handle it consistently.
    std::string abs_u8 = wstring_to_utf8(abs.native());
#else
    // Use UTF-8 path string on POSIX.
    std::string abs_u8 = abs.u8string();
#endif
    
    // Choose invocation: dot-source (keeps scope/state) vs. normal call (&).
    std::string prefix = dotSource ? DOT_SOURCE_PREFIX : NO_SOURCE_PREFIX;

    // Compose a compact script: stash args, then invoke the target with splatting.
    std::string command;
    command.reserve(abs_u8.size() + argArray.size() + 64);
    command += "$__args__ = " + argArray + ";\n";
    command += prefix + ps_quote(abs_u8) + " @__args__";

    return execute(command, timeoutSeconds);
}

std::future<VirtualShell::ExecutionResult>
VirtualShell::executeAsync_script(std::string scriptPath,
                                  std::vector<std::string> args,
                                  double timeoutSeconds,
                                  bool dotSource,
                                  bool /*raiseOnError*/,
                                  std::function<void(const ExecutionResult&)> callback)
{
    namespace fs = std::filesystem;

    // Optional: early validation on caller's thread (cheap fast-fail).
    fs::path abs = fs::absolute(scriptPath);
    if (!fs::exists(abs)) {
        std::promise<ExecutionResult> p;
        ExecutionResult r{};
        r.success  = false;
        r.exitCode = -1;
        r.err    = "Could not open script file: " + scriptPath;
        p.set_value(std::move(r)); // Fulfill immediately so caller's future becomes ready.
        return p.get_future();
    }

    // Normalize path to UTF-8 for consistent quoting into PowerShell.
#ifdef _WIN32
    std::string abs_u8 = wstring_to_utf8(abs.native());
#else
    std::string abs_u8 = abs.u8string();
#endif

    // Build @(<args...>) once; ps_quote() returns already single-quoted PS literals.
    std::string argArray;
    {
        // Conservative pre-reserve to reduce reallocations on large arg sets.
        size_t cap = 4 + args.size() * 6; // rough estimate
        for (auto& a : args) cap += a.size();
        argArray.reserve(cap);
        argArray += "@(";
        bool first = true;
        for (auto& a : args) {
            if (!first) argArray += ", ";
            first = false;
            argArray += ps_quote(a);
        }
        argArray += ")";
    }

    // Choose invocation flavor: dot-source (keeps caller scope) or normal call (&).
    std::string prefix = dotSource ? DOT_SOURCE_PREFIX : NO_SOURCE_PREFIX;

    // Final PowerShell command: stash args, then invoke target with splatting.
    std::string command;
    command.reserve(abs_u8.size() + argArray.size() + 64);
    command += "$__args__ = " + argArray + ";\n";
    command += prefix + ps_quote(abs_u8) + " @__args__";

    // Hand off to the async I/O engine; callback fires when the parser completes the command.
    return submit(std::move(command), timeoutSeconds, std::move(callback));
}


VirtualShell::ExecutionResult VirtualShell::execute_batch(
    const std::vector<std::string>& commands, double timeoutSeconds)
{
    // Pre-size buffer for single write to the child (reduces syscalls/copies).
    size_t cap = 0;
    for (const auto& c : commands) cap += c.size() + 1; // + '\n'
    std::string joined;
    joined.reserve(cap);

    // Concatenate commands with trailing newlines so the shell executes each line.
    for (const auto& c : commands) {
        if (!c.empty()) {
            joined.append(c);
            joined.push_back('\n');
        }
    }
    return execute(joined, timeoutSeconds);
}

std::future<std::vector<VirtualShell::ExecutionResult>>
VirtualShell::executeAsync_batch(std::vector<std::string> commands,
                                 std::function<void(const BatchProgress&)> progressCallback,
                                 bool stopOnFirstError,
                                 double perCommandTimeoutSeconds /* = 0.0 */)
{
    // Promise/Future returned to the caller
    auto prom = std::make_shared<std::promise<std::vector<ExecutionResult>>>();
    auto fut  = prom->get_future();

    // Keep 'this' alive while the detached thread runs
    auto self = shared_from_this(); // Requires that VirtualShell is managed by std::enable_shared_from_this.

    std::thread([self,
                 cmds = std::move(commands),
                 progressCallback = std::move(progressCallback),
                 stopOnFirstError,
                 perCommandTimeoutSeconds,
                 p = std::move(prom)]() mutable
    {
        BatchProgress prog{};
        prog.totalCommands  = cmds.size();
        prog.currentCommand = 0;
        prog.isComplete     = false;
        prog.allResults.reserve(cmds.size());

        // Edge case: empty batch
        if (cmds.empty()) {
            prog.isComplete = true;
            if (progressCallback) { try { progressCallback(prog); } catch (...) {} } // Swallow user callback errors.
            try { p->set_value({}); } catch (...) {}
            return;
        }

        // Submit and wait one-by-one (preserves stopOnFirstError semantics)
        for (auto& cmd : cmds) {
            ++prog.currentCommand;

            // Submit single command (moves cmd to avoid copy)
            auto futOne = self->submit(std::move(cmd),
                                       perCommandTimeoutSeconds,
                                       /*cb=*/nullptr);

            ExecutionResult r{};
            if (perCommandTimeoutSeconds > 0.0) {
                // Enforce per-command timeout on the waiting side.
                // Note: the underlying command may still complete later in the I/O engine.
                auto status = futOne.wait_for(std::chrono::duration<double>(perCommandTimeoutSeconds));
                if (status == std::future_status::ready) {
                    r = futOne.get();
                } else {
                    r.success  = false;
                    r.exitCode = -1;
                    r.err    = "timeout";
                }
            } else {
                // No explicit timeout: wait until completion
                r = futOne.get();
            }

            prog.lastResult = r;
            prog.allResults.push_back(r);

            if (progressCallback) {
                try { progressCallback(prog); } catch (...) {} // Never let user exceptions kill the batch thread.
            }

            if (stopOnFirstError && !r.success) {
                break; // Honor early-stop contract.
            }
        }

        prog.isComplete = true;
        if (progressCallback) { try { progressCallback(prog); } catch (...) {} }

        // Resolve the batch future with collected results.
        try { p->set_value(std::move(prog.allResults)); } catch (...) {}
    }).detach(); // Detached by design: lifetime is tied to 'self' and 'p' shared_ptrs.

    return fut;
}

VirtualShell::ExecutionResult VirtualShell::execute_script_kv(
    const std::string& scriptPath,
    const std::map<std::string, std::string>& namedArgs,
    double timeoutSeconds,
    bool dotSource, bool /*raiseOnError*/)
{
    namespace fs = std::filesystem;
    fs::path abs = fs::absolute(scriptPath);
    if (!fs::exists(abs)) {
        ExecutionResult r{};
        r.err = "Could not open script file: " + scriptPath;
        r.exitCode = -1; r.success = false;
        return r;
    }

#ifdef _WIN32
    std::string abs_u8 = wstring_to_utf8(abs.native());
#else
    std::string abs_u8 = abs.u8string();
#endif

    // Build hashtable literal: @{ key='value'; key2='value2' }
    std::string mapStr = "@{";
    bool first = true;
    for (auto& [k,v] : namedArgs) {
        if (!first) mapStr += "; ";
        first = false;
        mapStr += k; mapStr += "="; mapStr += ps_quote(v);
    }
    mapStr += "}";

    std::string prefix = dotSource ? DOT_SOURCE_PREFIX : NO_SOURCE_PREFIX;

    std::string command;
    command.reserve(abs_u8.size() + mapStr.size() + 64);
    command += "$__params__ = " + mapStr + ";\n";
    command += prefix + ps_quote(abs_u8) + " @__params__";
    return execute(command, timeoutSeconds);
}

std::future<VirtualShell::ExecutionResult>
VirtualShell::executeAsync_script_kv(std::string scriptPath,
                                     std::map<std::string, std::string> namedArgs,
                                     double timeoutSeconds,
                                     bool dotSource,
                                     bool /*raiseOnError*/)
{
    namespace fs = std::filesystem;

    // Optional early validation on caller's thread (cheap fast-fail).
    fs::path abs = fs::absolute(scriptPath);
    if (!fs::exists(abs)) {
        std::promise<ExecutionResult> p;
        ExecutionResult r{};
        r.success  = false;
        r.exitCode = -1;
        r.err    = "Could not open script file: " + scriptPath;
        p.set_value(std::move(r));
        return p.get_future();
    }

    // Normalize path to UTF-8 for consistent quoting into PowerShell.
#ifdef _WIN32
    std::string abs_u8 = wstring_to_utf8(abs.native());
#else
    std::string abs_u8 = abs.u8string();
#endif

    // Build PowerShell hashtable literal: @{ key='value'; key2='value2' }.
    // NOTE: We assume keys are PS bareword-safe (no spaces/special chars). If not, they must be quoted/escaped.
    std::string mapStr;
    {
        // Conservative reserve to reduce reallocations.
        size_t cap = 4; // "@{ }"
        for (auto& kv : namedArgs) cap += kv.first.size() + kv.second.size() + 6;
        mapStr.reserve(cap);

        mapStr += "@{";
        bool first = true;
        for (auto& kv : namedArgs) {
            if (!first) mapStr += "; ";
            first = false;
            mapStr += kv.first;
            mapStr += "=";
            mapStr += ps_quote(kv.second); // ps_quote doubles internal single quotes and wraps in '...'
        }
        mapStr += "}";
    }

    const std::string prefix = dotSource ? DOT_SOURCE_PREFIX : NO_SOURCE_PREFIX;

    // Final command: stash params, then invoke with splatting.
    std::string command;
    command.reserve(abs_u8.size() + mapStr.size() + 64);
    command += "$__params__ = " + mapStr + ";\n";
    command += prefix + ps_quote(abs_u8) + " @__params__";

    // Route through the async I/O engine; no per-command callback for the KV variant.
    return submit(std::move(command), timeoutSeconds, /*cb=*/nullptr);
}

bool VirtualShell::sendInput(const std::string& input) {
    if (!isRunning_) {
        return false;
    }
    
#ifdef _WIN32
    return writeToPipe(hInputWrite, input);
#else
    return writeToPipe(inputPipe[1], input);
#endif
}

void VirtualShell::readerStdoutLoop_() {
    // Blocking read; no global buffers, no ioMutex/ioCv.
    // Chunks are forwarded directly to the parser/demux via onChunk_(isStderr=false, ...).
    // IMPORTANT: onChunk_ must not retain the std::string_view beyond this call;
    // if it needs to store data, it must copy, since 'buf' is reused on the next loop iteration.
    try {
        std::array<char, READ_BUFFER_SIZE> buf{};
    #ifdef _WIN32
        while (ioRunning_) {
            if (!hOutputRead) break;
            DWORD got = 0;
            BOOL ok = ::ReadFile(hOutputRead, buf.data(), (DWORD)buf.size(), &got, NULL);
            if (!ok) {
                DWORD e = ::GetLastError();
                // ERROR_BROKEN_PIPE or handle closed => exit loop (parent closed or child exited).
                (void)e; // best-effort: diagnostics handled elsewhere.
                break;
            }
            if (got > 0) {
                // Note: std::string_view can contain NULs; PS output may include them on some encodings.
                onChunk_(false, std::string_view(buf.data(), (size_t)got));
            }
        }
    #else
        for (;;) {
            if (!ioRunning_) break;
            if (outputPipe[0] == -1) break;
            ssize_t got = ::read(outputPipe[0], buf.data(), buf.size());
            if (got > 0) {
                onChunk_(false, std::string_view(buf.data(), (size_t)got));
            } else if (got == 0) {
                // EOF: peer closed (child exited or our write-end closed).
                break;
            } else if (errno == EINTR) {
                continue; // Retry on signal interruptions.
            } else {
                // Fatal read error; exit loop.
                break;
            }
        }
    #endif
    } catch (...) {
        // Swallow exceptions so the background reader doesn't terminate the process.
        // stop() will clean up and resolve any inflight commands.
    }
}


void VirtualShell::readerStderrLoop_() {
    // Same as readerStdoutLoop_(), but with isErr=true so the parser
    // routes chunks into the error buffer for the active command.
    try {
        std::array<char, READ_BUFFER_SIZE> buf{};
    #ifdef _WIN32
        while (ioRunning_) {
            if (!hErrorRead) break;
            DWORD got = 0;
            BOOL ok = ::ReadFile(hErrorRead, buf.data(), (DWORD)buf.size(), &got, NULL);
            if (!ok) {
                DWORD e = ::GetLastError();
                (void)e;
                break;
            }
            if (got > 0) {
                onChunk_(true, std::string_view(buf.data(), (size_t)got));
            }
        }
    #else
        for (;;) {
            if (!ioRunning_) break;
            if (errorPipe[0] == -1) break;
            ssize_t got = ::read(errorPipe[0], buf.data(), buf.size());
            if (got > 0) {
                onChunk_(true, std::string_view(buf.data(), (size_t)got));
            } else if (got == 0) {
                break; // EOF
            } else if (errno == EINTR) {
                continue; // retry
            } else {
                break;   // fatal
            }
        }
    #endif
    } catch (...) {
        // Swallow exceptions so unexpected errors don't kill the background reader.
    }
}

#ifdef _WIN32
std::string VirtualShell::readOutputOverlapped_(bool blocking) {
    return read_overlapped_once(outPipe_, blocking);
}

std::string VirtualShell::readErrorOverlapped_(bool blocking) {
    return read_overlapped_once(errPipe_, blocking);
}
#endif

std::string VirtualShell::readOutput(bool blocking) {
    std::string out;
    constexpr size_t BUF_SZ = 64 * 1024;
    out.resize(BUF_SZ);

#ifdef _WIN32
    HANDLE h = hOutputRead;
    if (!h || h == INVALID_HANDLE_VALUE) return {};

    if (!blocking) {
        DWORD avail = 0;
        // Peek to avoid blocking; if no data, return empty.
        if (!::PeekNamedPipe(h, nullptr, 0, nullptr, &avail, nullptr) || avail == 0)
            return {};
        DWORD toRead = (avail > BUF_SZ) ? static_cast<DWORD>(BUF_SZ) : avail;
        DWORD br = 0;
        if (!::ReadFile(h, out.data(), toRead, &br, nullptr) || br == 0) return {};
        out.resize(br);
        return out;
    } else {
        DWORD br = 0;
        BOOL ok = ::ReadFile(h, out.data(), static_cast<DWORD>(out.size()), &br, nullptr);
        if (!ok) {
            DWORD err = ::GetLastError();
            if (err == ERROR_BROKEN_PIPE || err == ERROR_HANDLE_EOF) return {}; // EOF
            return {};
        }
        if (br == 0) return {};
        out.resize(br);
        return out;
    }

#else
    int fd = outputPipe[0];
    if (fd < 0) return {};

    if (!blocking) {
        // Non-blocking read: if no data, return empty.
        ssize_t n = ::read(fd, out.data(), out.size());
        if (n <= 0) return {};
        out.resize(static_cast<size_t>(n));
        return out;
    } else {
        // Blocking read: use poll() to wait until readable.
        struct pollfd pfd { fd, POLLIN, 0 };
        int rc = ::poll(&pfd, 1, /*timeout ms*/ -1);
        if (rc <= 0 || !(pfd.revents & POLLIN)) return {};
        ssize_t n = ::read(fd, out.data(), out.size());
        if (n <= 0) return {};
        out.resize(static_cast<size_t>(n));
        return out;
    }
#endif
}

std::string VirtualShell::readError(bool blocking) {
    std::string out;
    constexpr size_t BUF_SZ = 64 * 1024;
    out.resize(BUF_SZ);

#ifdef _WIN32
    HANDLE h = hErrorRead;
    if (!h || h == INVALID_HANDLE_VALUE) return {};

    if (!blocking) {
        DWORD avail = 0;
        // Peek to avoid blocking; if no data, return empty.
        if (!::PeekNamedPipe(h, nullptr, 0, nullptr, &avail, nullptr) || avail == 0)
            return {};
        DWORD toRead = (avail > BUF_SZ) ? static_cast<DWORD>(BUF_SZ) : avail;
        DWORD br = 0;
        if (!::ReadFile(h, out.data(), toRead, &br, nullptr) || br == 0) return {};
        out.resize(br);
        return out;
    } else {
        DWORD br = 0;
        BOOL ok = ::ReadFile(h, out.data(), static_cast<DWORD>(out.size()), &br, nullptr);
        if (!ok) {
            DWORD err = ::GetLastError();
            if (err == ERROR_BROKEN_PIPE || err == ERROR_HANDLE_EOF) return {}; // EOF
            return {};
        }
        if (br == 0) return {};
        out.resize(br);
        return out;
    }

#else
    int fd = errorPipe[0];
    if (fd < 0) return {};

    if (!blocking) {
        // Non-blocking read: if no data or EOF, return empty.
        ssize_t n = ::read(fd, out.data(), out.size());
        if (n <= 0) return {};
        out.resize(static_cast<size_t>(n));
        return out;
    } else {
        // Blocking read: wait until readable, then read once.
        struct pollfd pfd { fd, POLLIN, 0 };
        int rc = ::poll(&pfd, 1, /*timeout ms*/ -1);
        if (rc <= 0 || !(pfd.revents & POLLIN)) return {};
        ssize_t n = ::read(fd, out.data(), out.size());
        if (n <= 0) return {};
        out.resize(static_cast<size_t>(n));
        return out;
    }
#endif
}

bool VirtualShell::setWorkingDirectory(const std::string& directory) {
    // Use -LiteralPath to avoid wildcard expansion; ps_quote ensures safe literal quoting.
    const std::string cmd = "Set-Location -LiteralPath " + ps_quote(directory);
    return execute(cmd).success;
}

std::string VirtualShell::getWorkingDirectory() {
    // Ask PowerShell for the absolute path of the current FileSystem location.
    const char* cmd =
        "[IO.Path]::GetFullPath((Get-Location -PSProvider FileSystem).Path)";
    ExecutionResult r = execute(cmd);
    if (!r.success) return "";
    std::string path = r.out;
    trim_inplace(path); // Normalize trailing newline/whitespace from PS output.
    return path;
}

bool VirtualShell::setEnvironmentVariable(const std::string& name, const std::string& value) {
    // Process-scoped env var only (won't affect parent OS process).
    const std::string cmd =
        "[Environment]::SetEnvironmentVariable("
        + ps_quote(name) + ", "
        + ps_quote(value) + ", 'Process')";
    return execute(cmd).success;
}

std::string VirtualShell::getEnvironmentVariable(const std::string& name) {
    // Read from Process scope to match the setter above.
    const std::string cmd =
        "[Environment]::GetEnvironmentVariable(" + ps_quote(name) + ", 'Process')";
    ExecutionResult r = execute(cmd);
    if (!r.success) return "";
    std::string val = r.out;
    trim_inplace(val); // Strip PS newline/whitespace.
    return val;
}

bool VirtualShell::isModuleAvailable(const std::string& moduleName) {
    // NOTE: moduleName is inserted verbatim in single quotes here;
    // use ps_quote(moduleName) if you expect spaces/special chars.
    std::string command = "Get-Module -ListAvailable -Name '" + moduleName + "'";
    ExecutionResult result = execute(command);
    return result.success && !result.out.empty(); // Non-empty output => module found.
}

bool VirtualShell::importModule(const std::string& moduleName) {
    // Same note as above: consider ps_quote if names may need escaping.
    std::string command = "Import-Module '" + moduleName + "'";
    ExecutionResult result = execute(command);
    return result.success;
}

std::string VirtualShell::getPowerShellVersion() {
    ExecutionResult result = execute("$PSVersionTable.PSVersion.ToString()");
    if (result.success) {
        std::string version = result.out;
        // Trim whitespace (could also use trim_inplace for consistency with other helpers).
        version.erase(version.find_last_not_of(" \t\r\n") + 1);
        version.erase(0, version.find_first_not_of(" \t\r\n"));
        return version;
    }
    return "";
}

std::vector<std::string> VirtualShell::getAvailableModules() {
    std::vector<std::string> modules;
    ExecutionResult result = execute("Get-Module -ListAvailable | Select-Object -ExpandProperty Name | Sort-Object -Unique");
    
    if (result.success) {
        std::istringstream iss(result.out);
        std::string line;
        while (std::getline(iss, line)) {
            // Trim whitespace
            line.erase(line.find_last_not_of(" \t\r\n") + 1);
            line.erase(0, line.find_first_not_of(" \t\r\n"));
            if (!line.empty()) {
                modules.push_back(line);
            }
        }
    }
    
    return modules;
}

bool VirtualShell::updateConfig(const Config& newConfig) {
    if (isRunning_) {
        return false; // Cannot change config while process is running
    }
    config = newConfig;
    return true;
}

// Private methods.


void VirtualShell::writerLoop_() {
    try {
        while (ioRunning_) {
            std::string pkt;

            // Wait for work: guarded by writeMx_. The predicate handles spurious wakeups.
            {
                std::unique_lock<std::mutex> lk(writeMx_);
                writeCv_.wait(lk, [this]{
                    return !ioRunning_ || !writeQueue_.empty();
                });
                if (!ioRunning_) break; // Cooperative shutdown requested.
                pkt = std::move(writeQueue_.front()); // Pop one packet for atomic write.
                writeQueue_.pop_front();
            }

#ifdef _WIN32
            if (!hInputWrite || hInputWrite == INVALID_HANDLE_VALUE) {
                ioRunning_ = false; // Input pipe no longer valid; stop loop.
                break;
            }

            if (!writeToPipe(hInputWrite, pkt)) {
                // Child may have exited or pipe is broken; exit cleanly.
                ioRunning_ = false;
                break;
            }
#else
            int fd = inputPipe[1];
            if (fd < 0) { ioRunning_ = false; break; } // Invalid/closed write end.
            if (!writeToPipe(fd, pkt)) {
                // Broken pipe or short write failure; terminate writer loop.
                ioRunning_ = false;
                break;
            }
#endif
        }
    } catch (...) {
        // Never let exceptions escape a background thread; signal shutdown.
        ioRunning_ = false;
    }
}

bool VirtualShell::sendInitialCommands() {
    if (!config.initialCommands.empty()) {
        // Concatenate initial commands into a single write to minimize round-trips.
        std::string joined;
        joined.reserve(INITIAL_COMMANDS_BUF_SIZE);
        for (const auto& cmd : config.initialCommands) {
            joined.append(cmd);
            joined.push_back('\n'); // Execute each line separately in the shell.
        }
        ExecutionResult r = execute(joined);
        return r.success;
    }
    return true; // Nothing to send is a successful no-op.
}

bool VirtualShell::createPipes() {
#ifdef _WIN32
    SECURITY_ATTRIBUTES secAttr = {};
    secAttr.nLength = sizeof(SECURITY_ATTRIBUTES);
    secAttr.bInheritHandle = TRUE;              // Child will inherit handles unless we clear on the parent side.
    secAttr.lpSecurityDescriptor = NULL;
    
    // stdin pipe (child reads; parent writes)
    if (!CreatePipe(&hInputRead, &hInputWrite, &secAttr, 0)) {
        return false;
    }
    SetHandleInformation(hInputWrite, HANDLE_FLAG_INHERIT, 0); // Parent write end must NOT be inheritable.

    // stdout pipe (child writes; parent reads)
    if (!CreatePipe(&hOutputRead, &hOutputWrite, &secAttr, 0)) {
        CloseHandle(hInputRead);
        CloseHandle(hInputWrite);
        return false;
    }
    SetHandleInformation(hOutputRead, HANDLE_FLAG_INHERIT, 0); // Parent read end must NOT be inheritable.

    // stderr pipe (child writes; parent reads)
    if (!CreatePipe(&hErrorRead, &hErrorWrite, &secAttr, 0)) {
        CloseHandle(hInputRead);
        CloseHandle(hInputWrite);
        CloseHandle(hOutputRead);
        CloseHandle(hOutputWrite);
        return false;
    }
    SetHandleInformation(hErrorRead, HANDLE_FLAG_INHERIT, 0); // Parent read end must NOT be inheritable.

    // Child-side inheritable ends remain inheritable (hInputRead, hOutputWrite, hErrorWrite)
    // so CreateProcess can attach them to STDIN/STDOUT/STDERR.

    return true;
#else
    if (pipe(inputPipe) == -1 || pipe(outputPipe) == -1 || pipe(errorPipe) == -1) {
        closePipes();
        return false;
    }

    // Keep read ends blocking; our reader loops block on read()/poll().
    // Set FD_CLOEXEC on parent ends so de ikke arves ved senere exec() i parent.
    auto set_cloexec = [](int fd){
        int f = fcntl(fd, F_GETFD, 0);
        if (f != -1) fcntl(fd, F_SETFD, f | FD_CLOEXEC);
    };

    // Parent bruker: inputPipe[1] (write to child's stdin),
    //                outputPipe[0] (read child's stdout),
    //                errorPipe[0]  (read child's stderr).
    set_cloexec(inputPipe[1]);
    set_cloexec(outputPipe[0]);
    set_cloexec(errorPipe[0]);

    // NB: Child-endene lukkes riktig etter fork():
    //  - i child: dup2(...) + close(...) på alle pipe-FDs
    //  - i parent: close(inputPipe[0]), close(outputPipe[1]), close(errorPipe[1])
    return true;
#endif

}

void VirtualShell::closePipes() {
#ifdef _WIN32
    // Idempotent close; handles may be NULL already.
    if (hInputWrite)  { CloseHandle(hInputWrite);  hInputWrite  = NULL; }
    if (hInputRead)   { CloseHandle(hInputRead);   hInputRead   = NULL; }
    if (hOutputWrite) { CloseHandle(hOutputWrite); hOutputWrite = NULL; }
    if (hOutputRead)  { CloseHandle(hOutputRead);  hOutputRead  = NULL; }
    if (hErrorWrite)  { CloseHandle(hErrorWrite);  hErrorWrite  = NULL; }
    if (hErrorRead)   { CloseHandle(hErrorRead);   hErrorRead   = NULL; }
#else
    // Idempotent close; FDs may already be -1.
    if (inputPipe[0]  != -1) { close(inputPipe[0]);  inputPipe[0]  = -1; }
    if (inputPipe[1]  != -1) { close(inputPipe[1]);  inputPipe[1]  = -1; }
    if (outputPipe[0] != -1) { close(outputPipe[0]); outputPipe[0] = -1; }
    if (outputPipe[1] != -1) { close(outputPipe[1]); outputPipe[1] = -1; }
    if (errorPipe[0]  != -1) { close(errorPipe[0]);  errorPipe[0]  = -1; }
    if (errorPipe[1]  != -1) { close(errorPipe[1]);  errorPipe[1]  = -1; }
#endif
}

void VirtualShell::startIoThreads_() {
    // Only start once by CAS; avoids races with concurrent starters.
    bool expected = false;
    if (!ioRunning_.compare_exchange_strong(expected, true)) {
        return; // already running
    }

    // Spawn writer + readers; ensure strong exception safety.
    try {
        writerTh_ = std::thread(&VirtualShell::writerLoop_, this);
        rOutTh_   = std::thread(&VirtualShell::readerStdoutLoop_, this);
        rErrTh_   = std::thread(&VirtualShell::readerStderrLoop_, this);
    } catch (...) {
        // Roll back if any thread creation fails.
        ioRunning_ = false;
        writeCv_.notify_all(); // Wake potential waiters if writer started.
        if (writerTh_.joinable()) writerTh_.join();
        if (rOutTh_.joinable())   rOutTh_.join();
        if (rErrTh_.joinable())   rErrTh_.join();
        throw;
    }
}

void VirtualShell::stopIoThreads_() {
    // Only stop once
    bool expected = true;
    if (!ioRunning_.compare_exchange_strong(expected, false)) {
        return; // already stopped
    }

    // Wake writer thread in case it's waiting on the queue
    writeCv_.notify_all();

#ifdef _WIN32
    // Unblock blocking ReadFile() in reader threads by closing read handles
    HANDLE outH = hOutputRead;  hOutputRead  = nullptr;
    HANDLE errH = hErrorRead;   hErrorRead   = nullptr;
    if (outH && outH != INVALID_HANDLE_VALUE) ::CloseHandle(outH);
    if (errH && errH != INVALID_HANDLE_VALUE) ::CloseHandle(errH);
#else
    // Unblock blocking poll()/read() in reader threads by closing fds
    int outfd = outputPipe[0];  outputPipe[0] = -1;
    int errfd = errorPipe[0];   errorPipe[0]  = -1;
    if (outfd != -1) ::close(outfd);
    if (errfd != -1) ::close(errfd);
#endif

    // Join threads
    // NOTE: stopIoThreads_ must not be called from any of these threads (deadlock on join).
    if (writerTh_.joinable()) writerTh_.join();
    if (rOutTh_.joinable())   rOutTh_.join();
    if (rErrTh_.joinable())   rErrTh_.join();
}


#ifdef _WIN32
DWORD VirtualShell::readFromPipe(HANDLE handle, char* buffer, DWORD size) {
    if (!handle) return 0;
    
    DWORD bytesRead = 0;
    DWORD totalBytesAvail = 0;
    
    // Check if data is available without blocking.
    if (!PeekNamedPipe(handle, NULL, 0, NULL, &totalBytesAvail, NULL)) {
        return 0;
    }
    
    if (totalBytesAvail == 0) {
        return 0;
    }
    
    // Single ReadFile; caller should handle partial reads if needed.
    if (ReadFile(handle, buffer, size, &bytesRead, NULL)) {
        return bytesRead;
    }
    
    return 0;
}

bool VirtualShell::writeToPipe(HANDLE handle, const std::string& data) {
    if (!handle) return false;
    DWORD written = 0, total = 0;
    while (total < data.size()) {
        DWORD chunk = 0;
        // WriteFile writes at most DWORD bytes; loop until all data is sent.
        BOOL ok = WriteFile(handle,
                            data.data() + total,
                            (DWORD)std::min<size_t>(std::numeric_limits<DWORD>::max(),
                                                    data.size() - total),
                            &chunk,
                            NULL);
        if (!ok) return false;
        total += chunk;
    }
    return true;
}
#else
ssize_t VirtualShell::readFromPipe(int fd, char* buffer, size_t size) {
    if (fd == -1) return -1;
    
    // Temporarily set non-blocking for this call.
    // DEV NOTE: toggling flags per-call can race if fd is shared across threads;
    // prefer setting O_NONBLOCK once at creation if possible.
    int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    
    ssize_t result = read(fd, buffer, size);
    
    // Restore original flags
    fcntl(fd, F_SETFL, flags);
    
    return result;
}

bool VirtualShell::writeToPipe(int fd, const std::string& data) {
    if (fd == -1) return false;
    const char* p = data.data();
    size_t left = data.size();
    while (left > 0) {
        ssize_t n = ::write(fd, p, left);
        if (n > 0) { p += n; left -= (size_t)n; continue; }
        if (n == -1 && (errno == EINTR)) continue; // retry on signal
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // Small pause for backpressure; avoids tight spinning when pipe is full.
            std::this_thread::sleep_for(std::chrono::microseconds(200));
            continue;
        }
        return false; // fatal error (e.g., EPIPE if peer closed)
    }
    return true;
}
#endif


bool VirtualShell::waitForProcess(int timeoutMs) {
#ifdef _WIN32
    if (!hProcess) return false;
    
    // Wait up to timeoutMs for the process handle to be signaled.
    // NOTE: WAIT_OBJECT_0 == exited; WAIT_TIMEOUT/WAIT_FAILED => false.
    DWORD result = WaitForSingleObject(hProcess, timeoutMs);
    return result == WAIT_OBJECT_0;
#else
    if (processId <= 0) return false;
    
    // Poll-based wait with WNOHANG to avoid blocking; simple sleep backoff.
    // DEV NOTE: This can take up to timeoutMs (+ jitter). If you need tighter
    // precision or immediate wakeup on exit, consider installing SIGCHLD and
    // using a condition variable instead of fixed sleeps.
    auto endTime = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    
    while (std::chrono::steady_clock::now() < endTime) {
        int status;
        pid_t result = waitpid(processId, &status, WNOHANG);
        
        if (result == processId) {
            // Child has exited.
            return true;
        } else if (result == -1) {
            // waitpid error (e.g., ECHILD). Treat as not running.
            return false;
        }
        
        // Still running; sleep briefly before retrying.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    return false; // Timed out.
#endif
}

std::string VirtualShell::build_pwsh_packet(uint64_t id, std::string_view cmd) {
    const std::string beg = "<<<SS_BEG_" + std::to_string(id) + ">>>";
    const std::string end = "<<<SS_END_" + std::to_string(id) + ">>>";

    auto ps_quote_single = [](std::string_view s) {
        std::string t; t.reserve(s.size()+2);
        t.push_back('\'');
        for (char c : s) t += (c=='\'' ? "''" : std::string(1, c));
        t.push_back('\'');
        return t;
    };

    std::string full;
    full.reserve(cmd.size() + beg.size() + end.size() + 96);

    // 1) BEG neds quoting
    full += "[Console]::Out.WriteLine(" + ps_quote_single(beg) + ")\n";

    // 2) COMMAND ( may contain multiple lines; must end with newline)
    full.append(cmd);
    if (full.empty() || full.back() != '\n') full.push_back('\n');

    // 3) END neds quoting
    full += "[Console]::Out.WriteLine(" + ps_quote_single(end) + ")\n";

    return full;
}

std::future<VirtualShell::ExecutionResult>
VirtualShell::submit(std::string command, double timeoutSeconds,
                     std::function<void(const ExecutionResult&)> cb, bool bypassRestart)
{
    if (lifecycleGate_.load(std::memory_order_acquire) && !bypassRestart) {
        std::promise<ExecutionResult> p; ExecutionResult r{};
        r.err = "PowerShell process is restarting";
        r.exitCode = -2;
        r.success = false;
        p.set_value(std::move(r));
        return p.get_future();
    }

    if (!isRunning_) {
        // Process not running: fulfill a ready future with an error result.
        std::promise<ExecutionResult> p; ExecutionResult r{};
        r.err   = "PowerShell process is not running";
        r.exitCode= -3;
        r.success = false;
        p.set_value(std::move(r));
        return p.get_future();
    }
    



    // Monotonic sequence for correlating output to this request.
    const uint64_t id = ++seq_;

    auto S = std::make_unique<CmdState>();

    using clock = std::chrono::steady_clock;
    S->tStart = clock::now();
    S->timeoutSec = (timeoutSeconds > 0 ? timeoutSeconds : config.timeoutSeconds);
    S->id = id;
    S->beginMarker = "<<<SS_BEG_" + std::to_string(id) + ">>>";
    S->endMarker = "<<<SS_END_" + std::to_string(id) + ">>>";
    S->timeoutSec = (timeoutSeconds > 0 ? timeoutSeconds : config.timeoutSeconds);
    S->cb = std::move(cb);
    if (S->timeoutSec > 0.0) {
        auto delta = std::chrono::duration<double>(S->timeoutSec);
        S->tDeadline = S->tStart + std::chrono::duration_cast<clock::duration>(delta);
    } else {
        S->tDeadline = clock::time_point::max();
    }
    
    auto fut = S->prom.get_future();
    {
        // Register in-flight command before enqueueing write, so readers can demux immediately.
        std::lock_guard<std::mutex> lk(stateMx_);
        inflight_.emplace(id, std::move(S));
        inflightOrder_.push_back(id); // Maintain ordering if needed (e.g., for debug/aging scans).
    }

    // Update in-flight counters and track a simple high-water mark (for diagnostics/metrics).
    const uint32_t now = ++inflightCount_;
    uint32_t hw = highWater_.load(std::memory_order_relaxed);
    while (now > hw && !highWater_.compare_exchange_weak(hw, now, std::memory_order_relaxed)) {
        /* CAS-loop */
    }

    std::string cmd = std::move(command);
    {
        // Enqueue packetized command for the writer thread.
        std::lock_guard<std::mutex> lk(writeMx_);
        writeQueue_.emplace_back(build_pwsh_packet(id, cmd));
        VSHELL_DBG("IO", "write id=%llu bytes=%zu cmd=%s", (unsigned long long)id, writeQueue_.back().size(), cmd.c_str());
    }
    writeCv_.notify_one();

    return fut;
}

void VirtualShell::onChunk_(bool isErr, std::string_view sv) {
    if (sv.empty()) return;

    VSHELL_DBG("IO", "read %s bytes=%zu", isErr ? "STDERR" : "STDOUT", sv.size());

    std::unique_lock<std::mutex> lk(stateMx_);

    if (isErr) {
        // Route all stderr to the oldest open command (FIFO front).
        // Rationale: PowerShell doesn't tag stderr to a specific command; we associate it
        // with the currently-active one to preserve ordering. This is a heuristic and can
        // misattribute interleaved errors when multiple commands are in flight.
        static constexpr std::string_view sentinel{INTERNAL_TIMEOUT_SENTINEL};

        std::string chunk(sv.data(), sv.size());

        CmdState* st = nullptr;
        uint64_t stId = 0;
        if (!inflightOrder_.empty()) {
            stId = inflightOrder_.front();
            auto it = inflight_.find(stId);
            if (it != inflight_.end()) {
                st = it->second.get();
            }
        }

        bool completeFromSentinel = false;
        while (!chunk.empty()) {
            size_t pos = chunk.find(sentinel);
            if (pos == std::string::npos) break;

            size_t eraseEnd = pos + sentinel.size();
            if (eraseEnd < chunk.size() && chunk[eraseEnd] == '\r') { ++eraseEnd; }
            if (eraseEnd < chunk.size() && chunk[eraseEnd] == '\n') { ++eraseEnd; }

            uint32_t expected = pendingTimeoutSentinels_.load(std::memory_order_relaxed);
            chunk.erase(pos, eraseEnd - pos);

            if (expected > 0) {
                pendingTimeoutSentinels_.fetch_sub(1, std::memory_order_relaxed);
                continue;
            }

            if (st) {
                st->timedOut.store(true);
                completeFromSentinel = true;
            }

            break; // Only handle the first relevant sentinel per chunk
        }

        if (st && !chunk.empty()) {
            st->errBuf.append(chunk.data(), chunk.size());
        }

        if (completeFromSentinel && st) {
            std::unique_ptr<CmdState> done;
            auto it = inflight_.find(stId);
            if (it != inflight_.end()) {
                done = std::move(it->second);
                inflight_.erase(it);
            }
            if (!inflightOrder_.empty() && inflightOrder_.front() == stId) {
                inflightOrder_.pop_front();
            } else {
                auto qit = std::find(inflightOrder_.begin(), inflightOrder_.end(), stId);
                if (qit != inflightOrder_.end()) inflightOrder_.erase(qit);
            }

            lk.unlock();
            fulfillTimeout_(std::move(done), false);
            lk.lock();
        }

        return;
    }

    // STDOUT: may contain multiple completions in a single chunk.
    std::string carry; carry.assign(sv.data(), sv.size());

    while (!carry.empty() && !inflightOrder_.empty()) {
        uint64_t id = inflightOrder_.front();
        auto it = inflight_.find(id);
        if (it == inflight_.end()) {
            // Unexpected: queue says there's an active id, but map doesn't have it.
            // Drop from queue and continue.

            VSHELL_DBG("PARSE", "drop expired front id=%llu (pre-begun=%d)",
                       static_cast<unsigned long long>(id), 0);
            inflightOrder_.pop_front();
            continue;
        }

        CmdState& S = *it->second;

        if (!S.begun) {
            // Look for beginMarker in preBuf + carry
            S.preBuf.append(carry);

            size_t bpos = S.preBuf.find(S.beginMarker);
            if (bpos == std::string::npos) {
                // Not found yet: trim preBuf if too large (avoid unbounded growth)
                constexpr size_t CAP = 256 * 1024;
                if (S.preBuf.size() > CAP) {
                    // Keep only the last CAP bytes
                    S.preBuf.erase(0, S.preBuf.size() - CAP);
                }
                // All of carry consumed
                carry.clear();
                break;
            }

            // Jump past beginMarker (+ optional CRLF)
            size_t after = bpos + S.beginMarker.size();
            if (after < S.preBuf.size() && S.preBuf[after] == '\r') ++after;
            if (after < S.preBuf.size() && S.preBuf[after] == '\n') ++after;

            // We found the begin marker; prepare post-begin carry
            std::string postBeg;
            if (after < S.preBuf.size()) {
                postBeg.assign(S.preBuf.data() + after, S.preBuf.size() - after);
            }
            S.preBuf.clear();   // done with pre-noise
            S.begun = true;
            VSHELL_DBG("PARSE", "BEGIN id=%llu", static_cast<unsigned long long>(id));


            // Now we replace carry with the data bytes after BEG and continue as before
            carry.swap(postBeg);
            // fall-through to END-scan below
        }

        S.outBuf.append(carry);

        const size_t mpos = S.outBuf.find(S.endMarker);
        if (mpos == std::string::npos) {
            carry.clear();
            break;
        }

        // Jump past end marker (+ optional CRLF) and prepare nextCarry for next command
        size_t tail = mpos + S.endMarker.size();
        if (tail < S.outBuf.size() && S.outBuf[tail] == '\r') ++tail;
        if (tail < S.outBuf.size() && S.outBuf[tail] == '\n') ++tail;

        std::string nextCarry;
        if (tail < S.outBuf.size()) {
            nextCarry.assign(S.outBuf.data() + tail, S.outBuf.size() - tail);
        }

        // Truncate outBuf to just the command's output (up to but not including END marker)
        S.outBuf.resize(mpos);

        VSHELL_DBG("PARSE", "END id=%llu out_len=%zu err_len=%zu",
           (unsigned long long)id, S.outBuf.size(), S.errBuf.size());

        // Complete this command
        completeCmdLocked_(S, /*success=*/true);

        inflight_.erase(it);
        inflightOrder_.pop_front();

        lk.unlock();
        inflightCount_.fetch_sub(1, std::memory_order_relaxed);
        lk.lock();

        // Continue parsing with the remaining bytes (belonging to the next command)
        carry.swap(nextCarry);
    }
}

void VirtualShell::completeCmdLocked_(CmdState& S, bool success) {
    if (S.done.exchange(true)) return; // Idempotent completion: ignore double-finishes.
    using clock = std::chrono::steady_clock;
    const auto now = clock::now();

    ExecutionResult r{};
    r.success = success && !S.timedOut.load(); // A timed-out command cannot be reported as success.
    r.exitCode = r.success ? 0 : -1;
    r.out   = std::move(S.outBuf);
    r.err    = std::move(S.errBuf);
    r.executionTime = std::chrono::duration<double>(now - S.tStart).count();

    VSHELL_DBG("COMPLETE", "id=%llu success=%d exit=%d timedOut=%d out=%zu err=%zu",
           (unsigned long long)S.id, int(r.success), r.exitCode, int(S.timedOut.load()),
           r.out.size(), r.err.size());


    // Resolve the promise first (primary completion path).
    try { S.prom.set_value(r); } catch (...) {}
    // Then invoke optional user callback; never throw past here.
    if (S.cb) {
        try { S.cb(r); } catch (...) {}
    }
}

void VirtualShell::fulfillTimeout_(std::unique_ptr<CmdState> st, bool expectSentinel) {
    if (!st) return;

    VSHELL_DBG("TIMEOUT", "id=%llu internal=%d",
               static_cast<unsigned long long>(st->id),
               expectSentinel ? 0 : 1);

    if (expectSentinel) {
        pendingTimeoutSentinels_.fetch_add(1, std::memory_order_relaxed);
    }

    inflightCount_.fetch_sub(1, std::memory_order_relaxed);

    ExecutionResult r{};
    r.success  = false;
    r.exitCode = -1;
    r.err = st->errBuf.empty() ? std::string("timeout") : std::move(st->errBuf);

    if (config.autoRestartOnTimeout) {
        VSHELL_DBG("TIMEOUT", "id=%llu scheduling forced restart", static_cast<unsigned long long>(st->id));
        requestRestartAsync_(true);
        isRestarting_.store(true, std::memory_order_release);
    }

    st->done.store(true);

    try { st->prom.set_value(r); } catch (...) {}
    if (st->cb) { try { st->cb(r); } catch (...) {} }
}

void VirtualShell::requestRestartAsync_(bool force) {
    auto weak = this->weak_from_this();
    if (weak.expired()) {
        return;
    }

    bool expected = false;
    if (!lifecycleGate_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        VSHELL_DBG("TIMEOUT", "restart already pending");
        return;
    }

    try {
        std::thread([weak = std::move(weak), force]() mutable {
            if (auto self = weak.lock()) {
                self->stop(force);
                self->lifecycleGate_.store(true, std::memory_order_release);
                bool restarted = false;
                try {
                    restarted = self->start();
                } catch (...) {
                    VSHELL_DBG("TIMEOUT", "restart start() threw");
                }
                if (!restarted) {
                    VSHELL_DBG("TIMEOUT", "restart start() failed");
                }
                self->lifecycleGate_.store(false, std::memory_order_release);
            }
        }).detach();
    } catch (...) {
        lifecycleGate_.store(false, std::memory_order_release);
        VSHELL_DBG("TIMEOUT", "failed to spawn restart thread");
    }
}

void VirtualShell::timeoutOne_(uint64_t id) {
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

    fulfillTimeout_(std::move(st), true);
}


void VirtualShell::timeoutScan_() {
    using clock = std::chrono::steady_clock;
    while (timerRun_) {
        // Small sleep to avoid spinning.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (!timerRun_) break;

        std::vector<uint64_t> toExpire;
        const auto now = clock::now();

        {
            std::lock_guard<std::mutex> lk(stateMx_);
            if (inflight_.empty()) continue;
            // Check FIFO order first but remain robust and test all.
            for (auto const &id : inflightOrder_) {
                auto it = inflight_.find(id);
                if (it == inflight_.end()) continue;
                auto &S = *it->second;
                if (S.tDeadline != clock::time_point::max() && now >= S.tDeadline) {
                    toExpire.push_back(id);
                } else {
                    // Optimization note: since most complete FIFO, you could break here
                    // when you encounter the first non-expired front; kept conservative.
                }
            }
        }

        for (auto id : toExpire) {
            timeoutOne_(id);  // Handles its own locking and fulfillment.
        }
    }
}
