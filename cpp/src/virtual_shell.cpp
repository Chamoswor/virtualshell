#include "../include/virtual_shell.hpp"
#include "../include/powershell_process.hpp"
#include "../include/helpers.hpp"
#include "../include/dev_debug.hpp"
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

// Convenience macro (keeps callsites short)
// Enable by defining VIRTUALSHELL_DEBUG=1 in the environment
// Optionally set VIRTUALSHELL_DEBUG_PATH=<path> to specify log file location
#define VSHELL_DBG(TAG, FMT, ...) \
    do { if (virtualshell::dev::Logger::instance().enabled()) \
        virtualshell::dev::Logger::instance().logf(TAG, FMT, ##__VA_ARGS__); } while(0)

namespace fs = std::filesystem;

constexpr auto DOT_SOURCE_PREFIX = ". ";
constexpr auto NO_SOURCE_PREFIX  = "& ";

static constexpr std::string_view INTERNAL_TIMEOUT_SENTINEL = "__VS_INTERNAL_TIMEOUT__";

VirtualShell::VirtualShell(const Config& config) : config(config) {
    // Store configuration; actual process startup is deferred until start().
}

VirtualShell::~VirtualShell() {
    if (isRunning_) {
        stop(true);
    }
    io_pump_.stop();
    if (process_) {
        process_->terminate();
        process_.reset();
    }
}

VirtualShell::VirtualShell(VirtualShell&& other) noexcept
    : config(std::move(other.config))
{
    if (other.isRunning_) {
        other.stop(true);
    }

    // Transfer backend resources and queues; guard against concurrent readers with scoped locks.
    io_pump_ = std::move(other.io_pump_);
    process_ = std::move(other.process_);

    lastOutput = std::move(other.lastOutput);
    lastError = std::move(other.lastError);

    seq_.store(other.seq_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    inflightCount_.store(other.inflightCount_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    highWater_.store(other.highWater_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    pendingTimeoutSentinels_.store(other.pendingTimeoutSentinels_.load(std::memory_order_relaxed),
                                   std::memory_order_relaxed);

    {
        std::scoped_lock lk(other.stdoutMx_);
        stdoutQueue_ = std::move(other.stdoutQueue_);
    }
    {
        std::scoped_lock lk(other.stderrMx_);
        stderrQueue_ = std::move(other.stderrQueue_);
    }
    {
        std::scoped_lock lk(other.stateMx_);
        inflight_ = std::move(other.inflight_);
        inflightOrder_ = std::move(other.inflightOrder_);
    }

    other.isRunning_.store(false, std::memory_order_release);
    other.lifecycleGate_.store(false, std::memory_order_release);
    other.isRestarting_.store(false, std::memory_order_release);
}

VirtualShell& VirtualShell::operator=(VirtualShell&& other) noexcept {
    if (this == &other) {
        return *this;
    }

    // Ensure we tear down our own process before adopting the other instance’s state.
    if (isRunning_) {
        stop(true);
    }
    io_pump_.stop();
    process_.reset();

    if (other.isRunning_) {
        other.stop(true);
    }

    config = std::move(other.config);

    io_pump_ = std::move(other.io_pump_);
    process_ = std::move(other.process_);

    lastOutput = std::move(other.lastOutput);
    lastError = std::move(other.lastError);

    seq_.store(other.seq_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    inflightCount_.store(other.inflightCount_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    highWater_.store(other.highWater_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    pendingTimeoutSentinels_.store(other.pendingTimeoutSentinels_.load(std::memory_order_relaxed),
                                   std::memory_order_relaxed);

    {
        std::scoped_lock lk(other.stdoutMx_);
        stdoutQueue_ = std::move(other.stdoutQueue_);
    }
    {
        std::scoped_lock lk(other.stderrMx_);
        stderrQueue_ = std::move(other.stderrQueue_);
    }
    {
        std::scoped_lock lk(other.stateMx_);
        inflight_ = std::move(other.inflight_);
        inflightOrder_ = std::move(other.inflightOrder_);
    }

    other.isRunning_.store(false, std::memory_order_release);
    other.lifecycleGate_.store(false, std::memory_order_release);
    other.isRestarting_.store(false, std::memory_order_release);

    return *this;
}

void VirtualShell::restoreFromSnapshot_(const std::string& restoreScriptPath, 
                                        const std::string& snapshotPath)
{
    fs::path absRestore = fs::absolute(restoreScriptPath);
    fs::path absSnapshot = fs::absolute(snapshotPath);

    if (!fs::exists(absRestore)) {
        VSHELL_DBG("LIFECYCLE", "restore script not found: %s", absRestore.u8string().c_str());
        return;
    }
    if (!fs::exists(absSnapshot)) {
        VSHELL_DBG("LIFECYCLE", "snapshot file not found: %s", absSnapshot.u8string().c_str());
        return;
    }

    std::string restore_u8 = absRestore.u8string();
    std::string snapshot_u8 = absSnapshot.u8string();
    std::string command;
    command.reserve(restore_u8.size() + snapshot_u8.size() + 32);
    command += ". ";
    command += virtualshell::helpers::parsers::ps_quote(restore_u8);
    command += " -Path ";
    command += virtualshell::helpers::parsers::ps_quote(snapshot_u8);

    double restoreTimeout = config.timeoutSeconds > 0
        ? static_cast<double>(config.timeoutSeconds)
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

bool VirtualShell::start() {
    if (isRunning_) {
        return false;
    }

    VSHELL_DBG("LIFECYCLE", "start() pwsh_path='%s'", config.powershellPath.c_str());

    virtualshell::core::ProcessConfig procCfg;
    procCfg.powershell_path = config.powershellPath;
    procCfg.working_directory = config.workingDirectory;
    procCfg.environment = config.environment;

    auto process = std::make_unique<virtualshell::core::PowerShellProcess>(std::move(procCfg));
    if (!process->start()) {
        VSHELL_DBG("LIFECYCLE", "failed to launch PowerShell host");
        return false;
    }

    try {
        // Start pumping stdout/stderr so we can parse markers emitted by build_pwsh_packet().
        io_pump_.start(*process, [this](bool isErr, std::string_view chunk) {
            onChunk_(isErr, chunk);
        });
    } catch (...) {
        process->terminate();
        throw;
    }

    {
        std::lock_guard<std::mutex> outLock(stdoutMx_);
        stdoutQueue_.clear();
    }
    {
        std::lock_guard<std::mutex> errLock(stderrMx_);
        stderrQueue_.clear();
    }

    process_ = std::move(process);
    isRunning_.store(true, std::memory_order_release);
    lifecycleGate_.store(false, std::memory_order_release);

    timerRun_ = true;
    // Background watchdog scans inflight commands for deadlines.
    timerThread_ = std::thread([this] { timeoutWatcher_.scan(); });

    try {
        (void)this->execute("$null | Out-Null", /*timeoutSeconds=*/5.0);
    } catch (...) {
        VSHELL_DBG("LIFECYCLE", "warm-up command failed");
    }

    (void)sendInitialCommands_();
    if (!config.restoreScriptPath.empty() && !config.sessionSnapshotPath.empty()) {
        VSHELL_DBG("LIFECYCLE",
                   "restore check restore='%s' snapshot='%s'",
                   config.restoreScriptPath.c_str(),
                   config.sessionSnapshotPath.c_str());
        restoreFromSnapshot_(config.restoreScriptPath, config.sessionSnapshotPath);
    }
    isRestarting_.store(false, std::memory_order_release);

    return true;
}

void VirtualShell::stop(bool force) {
    std::unique_lock<std::mutex> stopLock(stopMx_);
    if (!isRunning_) {
        lifecycleGate_.store(false, std::memory_order_release);
        return;
    }

    lifecycleGate_.store(true, std::memory_order_release);

    VSHELL_DBG("LIFECYCLE", "stop(force=%d)", int(force));

    if (!force) {
        io_pump_.enqueue_write("exit\n");
        io_pump_.drain();
    }

    io_pump_.stop();

    timerRun_ = false;
    if (timerThread_.joinable()) {
        timerThread_.join();
    }

    stdoutCv_.notify_all();
    stderrCv_.notify_all();

    {
        std::lock_guard<std::mutex> lk(stateMx_);
        for (auto &kv : inflight_) {
            CmdState &state = *kv.second;
            if (!state.done.load()) {
                state.errBuf.append("Process stopped.\n");
                state.restartInterrupted.store(true, std::memory_order_release);
                completeCmdLocked_(state, /*success=*/false);
            }
        }
        inflight_.clear();
        inflightOrder_.clear();
        inflightCount_.store(0, std::memory_order_relaxed);
    }

    {
        std::lock_guard<std::mutex> lk(stdoutMx_);
        stdoutQueue_.clear();
    }
    {
        std::lock_guard<std::mutex> lk(stderrMx_);
        stderrQueue_.clear();
    }

    const bool exited = waitForProcess_(force ? 0 : 5000);
    if (!exited && process_) {
        process_->terminate();
    }

    process_.reset();
    isRunning_.store(false, std::memory_order_release);
    lifecycleGate_.store(false, std::memory_order_release);
}

bool VirtualShell::waitForProcess_(int timeoutMs) {
    if (!process_) {
        return true; // nothing running -> already “exited”
    }

    const auto deadline = timeoutMs >= 0
        ? std::optional<std::chrono::steady_clock::time_point>(
              std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs))
        : std::nullopt;

    while (process_->is_alive()) {
        if (deadline && std::chrono::steady_clock::now() >= *deadline) {
            return false; // still running and we hit the timeout
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return true; // process exited before the deadline (or no timeout requested)
}

bool VirtualShell::isAlive() const {
    if (!isRunning_) {
        return false;
    }
    if (!process_) {
        return false;
    }
    return process_->is_alive();
}


VirtualShell::ExecutionResult
VirtualShell::execute(const std::string& command, double timeoutSeconds)
{
    constexpr int MAX_RESTART_RETRIES = 16;
    const double to = (timeoutSeconds > 0 ? timeoutSeconds : config.timeoutSeconds); // Per-call override, else default.

    for (int attempt = 0; attempt < MAX_RESTART_RETRIES; ++attempt) {
        uint64_t cmdId = 0;
        auto fut = submit(command, timeoutSeconds, nullptr, /*bypassRestart=*/false, &cmdId);

        if (fut.wait_for(std::chrono::duration<double>(to)) == std::future_status::ready) {
            ExecutionResult res = fut.get();

            if (!res.success && res.exitCode == -2 && config.autoRestartOnTimeout) {
                VSHELL_DBG("RESTART", "retrying command after restart interruption attempt=%d", attempt + 1);
                (void)awaitLifecycleReady_(to);
                continue;
            }

            return res;
        }

        if (cmdId != 0) {
            timeoutWatcher_.timeoutOne(cmdId);

            if (config.autoRestartOnTimeout) {
                using clock = std::chrono::steady_clock;
                const auto deadline = clock::now() + std::chrono::seconds(5);
                while (isRestarting_.load(std::memory_order_acquire)) {
                    if (clock::now() >= deadline) {
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }
        }

        ExecutionResult r{};
        r.success = false;
        r.exitCode = -1;
        r.err = "timeout";
        return r;
    }

    ExecutionResult r{};
    r.success = false;
    r.exitCode = -2;
    r.err = "restart retry limit reached";
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
        argArray += virtualshell::helpers::parsers::ps_quote(args[i]); // Safe literal quoting for PowerShell.
    }
    argArray += ")";

#ifdef _WIN32
    // Convert wide native path to UTF-8 so ps_quote() can handle it consistently.
    std::string abs_u8 = virtualshell::helpers::win::wstring_to_utf8(abs.native());
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
    command += prefix + virtualshell::helpers::parsers::ps_quote(abs_u8) + " @__args__";

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
    std::string abs_u8 = virtualshell::helpers::win::wstring_to_utf8(abs.native());
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
            argArray += virtualshell::helpers::parsers::ps_quote(a);
        }
        argArray += ")";
    }

    // Choose invocation flavor: dot-source (keeps caller scope) or normal call (&).
    std::string prefix = dotSource ? DOT_SOURCE_PREFIX : NO_SOURCE_PREFIX;

    // Final PowerShell command: stash args, then invoke target with splatting.
    std::string command;
    command.reserve(abs_u8.size() + argArray.size() + 64);
    command += "$__args__ = " + argArray + ";\n";
    command += prefix + virtualshell::helpers::parsers::ps_quote(abs_u8) + " @__args__";

    // Hand off to the async I/O engine; callback fires when the parser completes the command.
    return submit(std::move(command), timeoutSeconds, std::move(callback));
}


std::vector<VirtualShell::ExecutionResult> VirtualShell::execute_batch(
    const std::vector<std::string>& commands, double timeoutSeconds)
{
    std::vector<ExecutionResult> results;
    results.reserve(commands.size());

    for (const auto& cmd : commands) {
        ExecutionResult r = execute(cmd, timeoutSeconds);
        results.push_back(std::move(r));
    }

    return results;
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
    std::string abs_u8 = virtualshell::helpers::win::wstring_to_utf8(abs.native());
#else
    std::string abs_u8 = abs.u8string();
#endif

    // Build hashtable literal: @{ key='value'; key2='value2' }
    std::string mapStr = "@{";
    bool first = true;
    for (auto& [k,v] : namedArgs) {
        if (!first) mapStr += "; ";
        first = false;
        mapStr += k; mapStr += "="; mapStr += virtualshell::helpers::parsers::ps_quote(v);
    }
    mapStr += "}";

    std::string prefix = dotSource ? DOT_SOURCE_PREFIX : NO_SOURCE_PREFIX;

    std::string command;
    command.reserve(abs_u8.size() + mapStr.size() + 64);
    command += "$__params__ = " + mapStr + ";\n";
    command += prefix + virtualshell::helpers::parsers::ps_quote(abs_u8) + " @__params__";
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
    std::string abs_u8 = virtualshell::helpers::win::wstring_to_utf8(abs.native());
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
            mapStr += virtualshell::helpers::parsers::ps_quote(kv.second); // ps_quote doubles internal single quotes and wraps in '...'
        }
        mapStr += "}";
    }

    const std::string prefix = dotSource ? DOT_SOURCE_PREFIX : NO_SOURCE_PREFIX;

    // Final command: stash params, then invoke with splatting.
    std::string command;
    command.reserve(abs_u8.size() + mapStr.size() + 64);
    command += "$__params__ = " + mapStr + ";\n";
    command += prefix + virtualshell::helpers::parsers::ps_quote(abs_u8) + " @__params__";

    // Route through the async I/O engine; no per-command callback for the KV variant.
    return submit(std::move(command), timeoutSeconds, /*cb=*/nullptr);
}

bool VirtualShell::sendInput(const std::string& input) {
    if (!isRunning_) {
        return false;
    }
    return io_pump_.enqueue_write(input);
}

std::string VirtualShell::readOutput(bool blocking) {
    std::unique_lock<std::mutex> lk(stdoutMx_);
    if (blocking) {
        stdoutCv_.wait(lk, [this] {
            return !stdoutQueue_.empty() || !isRunning_;
        });
    } else if (stdoutQueue_.empty()) {
        return {};
    }

    if (stdoutQueue_.empty()) {
        return {};
    }

    std::string chunk = std::move(stdoutQueue_.front());
    stdoutQueue_.pop_front();
    return chunk;
}

std::string VirtualShell::readError(bool blocking) {
    std::unique_lock<std::mutex> lk(stderrMx_);
    if (blocking) {
        stderrCv_.wait(lk, [this] {
            return !stderrQueue_.empty() || !isRunning_;
        });
    } else if (stderrQueue_.empty()) {
        return {};
    }

    if (stderrQueue_.empty()) {
        return {};
    }

    std::string chunk = std::move(stderrQueue_.front());
    stderrQueue_.pop_front();
    return chunk;
}

bool VirtualShell::setWorkingDirectory(const std::string& directory) {
    // Use -LiteralPath to avoid wildcard expansion; ps_quote ensures safe literal quoting.
    const std::string cmd = "Set-Location -LiteralPath " + virtualshell::helpers::parsers::ps_quote(directory);
    return execute(cmd).success;
}

std::string VirtualShell::getWorkingDirectory() {
    // Ask PowerShell for the absolute path of the current FileSystem location.
    const char* cmd =
        "[IO.Path]::GetFullPath((Get-Location -PSProvider FileSystem).Path)";
    ExecutionResult r = execute(cmd);
    if (!r.success) return "";
    std::string path = r.out;
    virtualshell::helpers::parsers::trim_inplace(path); // Normalize trailing newline/whitespace from PS output.
    return path;
}

bool VirtualShell::setEnvironmentVariable(const std::string& name, const std::string& value) {
    // Process-scoped env var only (won't affect parent OS process).
    const std::string cmd =
        "[Environment]::SetEnvironmentVariable("
        + virtualshell::helpers::parsers::ps_quote(name) + ", "
        + virtualshell::helpers::parsers::ps_quote(value) + ", 'Process')";
    return execute(cmd).success;
}

std::string VirtualShell::getEnvironmentVariable(const std::string& name) {
    // Read from Process scope to match the setter above.
    const std::string cmd =
        "[Environment]::GetEnvironmentVariable(" + virtualshell::helpers::parsers::ps_quote(name) + ", 'Process')";
    ExecutionResult r = execute(cmd);
    if (!r.success) return "";
    std::string val = r.out;
    virtualshell::helpers::parsers::trim_inplace(val); // Strip PS newline/whitespace.
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

bool VirtualShell::sendInitialCommands_() {
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

    full += "[Console]::Out.WriteLine(" + ps_quote_single(beg) + ")\n"; // Begin marker

    full.append(cmd);
    if (full.empty() || full.back() != '\n') full.push_back('\n'); // Ensure trailing newline

    full += "[Console]::Out.WriteLine(" + ps_quote_single(end) + ")\n"; // End marker

    return full;
}

std::future<VirtualShell::ExecutionResult>
VirtualShell::makeErrorFuture_(int exitCode, std::string_view err) const {
    std::promise<ExecutionResult> p;
    auto fut = p.get_future();

    ExecutionResult r{};
    r.success = false;
    r.exitCode = exitCode;
    r.err = std::string(err);

    p.set_value(std::move(r));
    return fut;
}

uint64_t VirtualShell::nextCommandId_() {
    return seq_.fetch_add(1, std::memory_order_relaxed) + 1;
}

std::unique_ptr<VirtualShell::CmdState>
VirtualShell::createCmdState_(uint64_t id,
                              double timeoutSeconds,
                              std::function<void(const ExecutionResult&)> cb) {
    using clock = std::chrono::steady_clock;

    auto state = std::make_unique<CmdState>();
    state->tStart = clock::now();
    state->timeoutSec = (timeoutSeconds > 0 ? timeoutSeconds : config.timeoutSeconds);
    state->id = id;
    state->beginMarker = "<<<SS_BEG_" + std::to_string(id) + ">>>";
    state->endMarker = "<<<SS_END_" + std::to_string(id) + ">>>";
    state->cb = std::move(cb);
    state->tDeadline = clock::time_point::max();

    return state;
}

void VirtualShell::registerCmdStateLocked_(uint64_t id, std::unique_ptr<CmdState> state) {
    std::lock_guard<std::mutex> lk(stateMx_);
    inflight_.emplace(id, std::move(state));
    inflightOrder_.push_back(id);
}

void VirtualShell::handleEnqueueFailure_(uint64_t id) {
    std::unique_ptr<CmdState> st;
    {
        std::lock_guard<std::mutex> lk(stateMx_);
        st = eraseStateLocked_(id);
    }

    inflightCount_.fetch_sub(1, std::memory_order_relaxed);

    if (st) {
        ExecutionResult r{};
        r.success = false;
        r.exitCode = -1;
        r.err = "failed to enqueue command";
        try { st->prom.set_value(r); } catch (...) {}
        if (st->cb) {
            try { st->cb(r); } catch (...) {}
        }
    }
}

bool VirtualShell::awaitLifecycleReady_(double maxWaitSeconds) {
    using clock = std::chrono::steady_clock;

    double budget = maxWaitSeconds;
    if (budget <= 0.0) {
        budget = (config.timeoutSeconds > 0)
            ? static_cast<double>(config.timeoutSeconds)
            : 5.0;
    }

    if (budget < 1.0) {
        budget = 1.0; // Always allow a short grace window for lifecycle transitions.
    }

    const auto deadline = clock::now() + std::chrono::duration_cast<clock::duration>(std::chrono::duration<double>(budget));

    while (true) {
        const bool gate = lifecycleGate_.load(std::memory_order_acquire);
        const bool running = isRunning_.load(std::memory_order_acquire);
        if (!gate && running) {
            return true;
        }

        if (!gate && !running && !isRestarting_.load(std::memory_order_acquire)) {
            return true; // Caller will surface the "not running" condition.
        }

        if (clock::now() >= deadline) {
            return false;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

std::future<VirtualShell::ExecutionResult>
VirtualShell::submit(std::string command, double timeoutSeconds,
                     std::function<void(const ExecutionResult&)> cb, bool bypassRestart,
                     uint64_t* outId)
{
    if (!bypassRestart) {
        if (!awaitLifecycleReady_(timeoutSeconds)) {
            return makeErrorFuture_(-2, "PowerShell process is restarting (wait timeout)");
        }
    }

    if (lifecycleGate_.load(std::memory_order_acquire) && !bypassRestart) {
        return makeErrorFuture_(-2, "PowerShell process is restarting");
    }

    if (!isRunning_) {
        return makeErrorFuture_(-3, "PowerShell process is not running");
    }

    const uint64_t id = nextCommandId_();
    auto state = createCmdState_(id, timeoutSeconds, std::move(cb));
    auto fut = state->prom.get_future();

    if (outId) {
        *outId = id;
    }

    registerCmdStateLocked_(id, std::move(state));

    // Update in-flight counters and track a simple high-water mark (for diagnostics/metrics).
    const uint32_t now = ++inflightCount_;
    uint32_t hw = highWater_.load(std::memory_order_relaxed);
    while (now > hw && !highWater_.compare_exchange_weak(hw, now, std::memory_order_relaxed)) {
        /* CAS-loop */
    }

    std::string packet = build_pwsh_packet(id, command);
    const size_t packetSize = packet.size();
    VSHELL_DBG("IO-CMD", "write id=%llu bytes=%zu cmd=\"%s\"",
               static_cast<unsigned long long>(id), packetSize, command.c_str());

    if (!io_pump_.enqueue_write(std::move(packet))) {
        VSHELL_DBG("IO-CMD", "enqueue failed id=%llu", static_cast<unsigned long long>(id));
        handleEnqueueFailure_(id);
    }

    return fut;
}

void VirtualShell::enqueueStreamChunk_(bool isErr, std::string_view chunk) {
    if (chunk.empty()) {
        return;
    }

    auto& mx = isErr ? stderrMx_ : stdoutMx_;
    auto& cv = isErr ? stderrCv_ : stdoutCv_;
    auto& queue = isErr ? stderrQueue_ : stdoutQueue_;

    {
        std::lock_guard<std::mutex> lk(mx);
        queue.emplace_back(chunk);
    }

    cv.notify_all();
}

void VirtualShell::onChunk_(bool isErr, std::string_view sv) {
    if (sv.empty()) return;

    VSHELL_DBG("IO", "read %s bytes=%zu", isErr ? "STDERR" : "STDOUT", sv.size());

    std::unique_lock<std::mutex> lk(stateMx_);

    if (isErr) {
        // Stderr carries timeout sentinels and error text; handle separately before returning to readers.
        handleErrorChunk_(lk, sv);
        return;
    }

    handleOutputChunk_(lk, sv);
}

void VirtualShell::handleErrorChunk_(std::unique_lock<std::mutex>& lk, std::string_view sv) {
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

    const bool completeFromSentinel = stripTimeoutSentinel_(chunk, st);

    if (st && !chunk.empty()) {
        st->errBuf.append(chunk.data(), chunk.size());
    }

    if (!chunk.empty()) {
        enqueueStreamChunk_(true, chunk);
    }

    if (completeFromSentinel && st) {
        auto done = eraseStateLocked_(stId);
        lk.unlock();
        fulfillTimeout_(std::move(done), false);
        lk.lock();
    }
}

void VirtualShell::handleOutputChunk_(std::unique_lock<std::mutex>& lk, std::string_view sv) {
    enqueueStreamChunk_(false, sv);
    std::string carry(sv.data(), sv.size());

    while (!carry.empty() && !inflightOrder_.empty()) {
        const uint64_t id = inflightOrder_.front();
        auto it = inflight_.find(id);
        if (it == inflight_.end()) {
            VSHELL_DBG("PARSE", "drop expired front id=%llu (pre-begun=%d)",
                       static_cast<unsigned long long>(id), 0);
            inflightOrder_.pop_front();
            continue;
        }

        CmdState& state = *it->second;

        if (!state.begun && !ensureCommandBegan_(id, state, carry)) {
            break;
        }

        std::string nextCarry;
        if (!tryFinalizeCommand_(id, state, carry, nextCarry)) {
            break;
        }

        completeCmdLocked_(state, /*success=*/true);
        (void)eraseStateLocked_(id);

        lk.unlock();
        inflightCount_.fetch_sub(1, std::memory_order_relaxed);
        lk.lock();

        carry.swap(nextCarry);
    }
}

bool VirtualShell::stripTimeoutSentinel_(std::string& chunk, CmdState* st) {
    bool completeFromSentinel = false;

    while (!chunk.empty()) {
        size_t pos = chunk.find(INTERNAL_TIMEOUT_SENTINEL);
        if (pos == std::string::npos) break;

        size_t eraseEnd = pos + INTERNAL_TIMEOUT_SENTINEL.size();
        if (eraseEnd < chunk.size() && chunk[eraseEnd] == '\r') { ++eraseEnd; }
        if (eraseEnd < chunk.size() && chunk[eraseEnd] == '\n') { ++eraseEnd; }

        uint32_t expected = pendingTimeoutSentinels_.load(std::memory_order_relaxed);
        chunk.erase(pos, eraseEnd - pos);

        if (expected > 0) {
            // Drop the sentinel silently; timeoutWatcher_ already consumed the matching command.
            pendingTimeoutSentinels_.fetch_sub(1, std::memory_order_relaxed);
            continue;
        }

        if (st) {
            st->timedOut.store(true);
        }

        completeFromSentinel = true;
        break;
    }

    return completeFromSentinel;
}

bool VirtualShell::ensureCommandBegan_(uint64_t id, CmdState& state, std::string& carry) {
    state.preBuf.append(carry);

    size_t bpos = state.preBuf.find(state.beginMarker);
    if (bpos == std::string::npos) {
        constexpr size_t CAP = 256 * 1024;
        if (state.preBuf.size() > CAP) {
            // Avoid unbounded growth if the marker never arrives (e.g. console spam).
            state.preBuf.erase(0, state.preBuf.size() - CAP);
        }
        carry.clear();
        return false;
    }

    size_t after = bpos + state.beginMarker.size();
    if (after < state.preBuf.size() && state.preBuf[after] == '\r') ++after;
    if (after < state.preBuf.size() && state.preBuf[after] == '\n') ++after;

    std::string postBeg;
    if (after < state.preBuf.size()) {
        postBeg.assign(state.preBuf.data() + after, state.preBuf.size() - after);
    }

    state.preBuf.clear();
    state.begun = true;
    VSHELL_DBG("PARSE", "BEGIN id=%llu", static_cast<unsigned long long>(id));

    if (state.timeoutSec > 0.0) {
        using clock = std::chrono::steady_clock;
        state.tStart = clock::now();
        auto delta = std::chrono::duration<double>(state.timeoutSec);
        state.tDeadline = state.tStart + std::chrono::duration_cast<clock::duration>(delta);
    }

    // Whatever followed the begin marker now moves into carry for payload handling.
    carry.swap(postBeg);
    return true;
}

bool VirtualShell::tryFinalizeCommand_(uint64_t id,
                                       CmdState& state,
                                       std::string& carry,
                                       std::string& nextCarry) {
    state.outBuf.append(carry);
    carry.clear();

    const size_t mpos = state.outBuf.find(state.endMarker);
    if (mpos == std::string::npos) {
        return false;
    }

    size_t tail = mpos + state.endMarker.size();
    if (tail < state.outBuf.size() && state.outBuf[tail] == '\r') ++tail;
    if (tail < state.outBuf.size() && state.outBuf[tail] == '\n') ++tail;

    if (tail < state.outBuf.size()) {
        nextCarry.assign(state.outBuf.data() + tail, state.outBuf.size() - tail);
    } else {
        nextCarry.clear();
    }

    state.outBuf.resize(mpos);

    VSHELL_DBG("PARSE", "END id=%llu out_len=%zu err_len=%zu",
               static_cast<unsigned long long>(id),
               state.outBuf.size(),
               state.errBuf.size());

    // Move any trailing data (beginning of the next command) so callers can continue parsing.
    return true;
}

std::unique_ptr<VirtualShell::CmdState>
VirtualShell::eraseStateLocked_(uint64_t id) {
    auto it = inflight_.find(id);
    std::unique_ptr<CmdState> st;
    if (it != inflight_.end()) {
        st = std::move(it->second);
        inflight_.erase(it);
    }

    if (!inflightOrder_.empty() && inflightOrder_.front() == id) {
        inflightOrder_.pop_front();
    } else {
        auto qit = std::find(inflightOrder_.begin(), inflightOrder_.end(), id);
        if (qit != inflightOrder_.end()) inflightOrder_.erase(qit);
    }

    return st;
}

void VirtualShell::completeCmdLocked_(CmdState& S, bool success) {
    if (S.done.exchange(true)) return; // Idempotent completion: ignore double-finishes.
    using clock = std::chrono::steady_clock;
    const auto now = clock::now();

    ExecutionResult r{};
    const bool timedOut = S.timedOut.load(std::memory_order_acquire);
    const bool interrupted = S.restartInterrupted.load(std::memory_order_acquire);

    if (interrupted) {
        r.success = false;
        r.exitCode = -2;
    } else {
        r.success = success && !timedOut; // A timed-out command cannot be reported as success.
        r.exitCode = r.success ? 0 : -1;
    }

    r.out   = std::move(S.outBuf);
    r.err   = std::move(S.errBuf);
    if (interrupted && r.err.empty()) {
        r.err = "Process stopped.\n";
    }
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
        isRestarting_.store(true, std::memory_order_release);
        requestRestartAsync_(true);
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
                self->isRestarting_.store(false, std::memory_order_release);
            }
        }).detach();
    } catch (...) {
        lifecycleGate_.store(false, std::memory_order_release);
        isRestarting_.store(false, std::memory_order_release);
        VSHELL_DBG("TIMEOUT", "failed to spawn restart thread");
    }
}