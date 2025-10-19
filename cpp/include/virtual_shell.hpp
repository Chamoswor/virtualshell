#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include <optional>
#include <functional>
#include <map>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <future>
#include <unordered_map>

#include "io_pump.hpp"
#include "execution_result.hpp"
#include "cmd_state.hpp"
#include "timeout_watcher.hpp"
#include "config.hpp"

namespace virtualshell {
namespace core {
class PowerShellProcess;
}
}

#define INITIAL_COMMANDS_BUF_SIZE (8 * 1024) // 8 KB buffer for initial commands, used in sendInitialCommands()

/**
 * @brief Headless PowerShell 7 process host.
 *
 * Start, communicate with, and control a PowerShell 7 process programmatically,
 * without showing a window.
 */
class VirtualShell : public std::enable_shared_from_this<VirtualShell> {
    /**
     * @brief Compose a framed payload understood by the PowerShell listener.
     *
     * Adds message metadata (command id framing) around the raw script text so
     * the embedded PowerShell host can correlate responses.
     */
    static std::string build_pwsh_packet(uint64_t id, std::string_view cmd);
public:
    /**
     * @brief Tagged chunk emitted by the background I/O pump.
     */
    struct OutChunk { bool isErr; std::string data; };
    using CmdState = virtualshell::core::CmdState;
    using ExecutionResult = virtualshell::core::ExecutionResult;
    using BatchProgress = virtualshell::core::BatchProgress;
    using TimeoutWatcher = virtualshell::core::TimeoutWatcher;
    using Config = virtualshell::core::Config;
private:


    Config config;                        ///< Current process configuration
    std::atomic<bool> isRunning_{false};  ///< True if PowerShell process is alive
    std::atomic<bool> lifecycleGate_{false};  ///< Blocks submissions while lifecycle transitions run
    std::atomic<bool> isRestarting_{false}; ///< True if a restart is in progress
    std::mutex stopMx_;                   ///< Serializes stop() invocations
    
    std::unique_ptr<virtualshell::core::PowerShellProcess> process_; ///< Active PowerShell host process
    virtualshell::core::IoPump io_pump_; ///< Background I/O pump mediating process streams

    std::mutex stdoutMx_;
    std::condition_variable stdoutCv_;
    std::deque<std::string> stdoutQueue_;

    std::mutex stderrMx_;
    std::condition_variable stderrCv_;
    std::deque<std::string> stderrQueue_;

    std::mutex stateMx_; ///< Protects inflight_ and inflightOrder_
    std::unordered_map<uint64_t, std::unique_ptr<virtualshell::core::CmdState>> inflight_; ///< Active commands by ID

    std::atomic<uint64_t> seq_{0}; ///< Monotonic sequence for command IDs



    std::string lastOutput; ///< Last captured stdout (for sync APIs)
    std::string lastError;  ///< Last captured stderr (for sync APIs)

    std::atomic<uint32_t> inflightCount_{0}; ///< Current number of in-flight commands
    std::atomic<uint32_t> highWater_{0};     ///< High-water mark of in-flight commands
    std::deque<uint64_t>  inflightOrder_;    ///< FIFO order of in-flight command IDs
    std::atomic<uint32_t> pendingTimeoutSentinels_{0}; ///< Expected stderr timeout sentinels to discard

    /**
     * @internal
     * @brief Remove and return the CmdState for a given ID.
     * 
     * Thread-safe: acquires stateMx_.
     * @param id Command identifier
     * @return Unique pointer to the CmdState, or nullptr if not found
     */
    std::unique_ptr<CmdState> takeState_(uint64_t id) {
        std::lock_guard<std::mutex> lk(stateMx_);
        auto it = inflight_.find(id);
        if (it == inflight_.end()) return {};
        auto ptr = std::move(it->second);
        inflight_.erase(it);
        return ptr;
    }

    TimeoutWatcher timeoutWatcher_{
        stateMx_,
        inflight_,
        inflightOrder_,
        timerRun_,
        [this](std::unique_ptr<CmdState> st, bool expectSentinel) {
            fulfillTimeout_(std::move(st), expectSentinel);
        }
    };

    void fulfillTimeout_(std::unique_ptr<CmdState> st, bool expectSentinel);
    void requestRestartAsync_(bool force);
    bool awaitLifecycleReady_(double maxWaitSeconds);

    std::thread timerThread_;       ///< Background watchdog thread for timeouts
    std::atomic<bool> timerRun_{false}; ///< True while timeout watchdog is active

public:

    /**
     * @todo Add public API functions to expose these metrics for monitoring.
     * 
     * @brief Runtime metrics for shell activity.
     */
    struct Metrics {
        uint32_t inflight;   ///< Number of commands currently in-flight
        uint32_t queued;     ///< Number of commands waiting in the write queue
        uint32_t high_water; ///< Highest observed number of in-flight commands
    };

    


    /**
     * @brief Construct a new VirtualShell with the given configuration.
     * 
     * @param config Process configuration (PowerShell path, env, timeouts, etc.)
     */
    explicit VirtualShell(const Config& config);

    /**
     * @brief Construct a new VirtualShell with default configuration.
     * 
     * Uses "pwsh.exe" from PATH, current directory, captures output,
     * and a default timeout of 30 seconds.
     */
    inline VirtualShell() : VirtualShell(Config{}) {}

    /**
     * @brief Destructor.
     * 
     * Ensures the PowerShell process and associated resources are stopped
     * and cleaned up if still running.
     */
    ~VirtualShell();

    /**
     * @brief Copy construction is disabled.
     * 
     * VirtualShell manages OS handles and threads, which cannot be safely copied.
     */
    VirtualShell(const VirtualShell&) = delete;

    /**
     * @brief Copy assignment is disabled.
     */
    VirtualShell& operator=(const VirtualShell&) = delete;

    /**
     * @brief Move constructor.
     * 
     * Transfers ownership of process handles, threads, and state from another instance.
     * @param other Source shell to move from
     */
    VirtualShell(VirtualShell&& other) noexcept;

    /**
     * @brief Move assignment operator.
     * 
     * Transfers ownership of process handles, threads, and state from another instance.
     * @param other Source shell to move from
     * @return Reference to this shell
     */
    VirtualShell& operator=(VirtualShell&& other) noexcept;

    /**
     * @brief Start the PowerShell process.
     * 
     * Allocates pipes, spawns the child process, and launches reader/writer threads.
     * @return true if the process started successfully
     * @return false if process creation failed
     */
    bool start();

    /**
     * @brief Stop the PowerShell process and all associated I/O threads.
     * 
     * @param force If true, terminate the process forcefully. If false, attempt graceful shutdown.
     */
    void stop(bool force = false);

    /**
     * @brief Check if the PowerShell process is currently alive.
     * 
     * @return true if the process is running
     * @return false otherwise
     */
    bool isAlive() const;

    /**
     * @brief Report whether the worker is currently cycling the PowerShell process.
     */
    bool isRestarting() const { return isRestarting_.load(std::memory_order_acquire); }

    /**
     * @brief Return the configured default timeout (seconds) for new commands.
     */
    int getDefaultTimeout() const { return config.timeoutSeconds; }

    /**
     * @brief Enqueue a command for execution.
     * @param command Command string to run
     * @param timeoutSeconds Timeout (0 = use default)
     * @param cb Optional completion callback
    * @param bypassRestart If true, skip safety checks while a restart is underway
    * @param outId Optional pointer receiving the assigned command identifier
     * @return Future resolving to the ExecutionResult
     */
    std::future<ExecutionResult> submit(std::string command,
                                        double timeoutSeconds,
                                        std::function<void(const ExecutionResult&)> cb = nullptr,
                                        bool bypassRestart = false,
                                        uint64_t* outId = nullptr);
    

    /**
     * @brief Execute a single PowerShell command synchronously.
     * 
     * @param command Command string to execute
     * @param timeoutSeconds Optional timeout for this command (0 = use default)
     * @return ExecutionResult Result object containing output, error, and exit code
     */
    ExecutionResult execute(const std::string& command, double timeoutSeconds = 0.0);

    /**
     * @brief Execute a batch of PowerShell commands synchronously.
     * 
     * Commands are executed sequentially in the same persistent session.
     * 
     * @param commands Vector of command strings
     * @param timeoutSeconds Timeout per command (0 = use default)
     * @return std::vector<ExecutionResult> Final result as an array of ExecutionResult objects
     */
    std::vector<ExecutionResult> execute_batch(const std::vector<std::string>& commands, double timeoutSeconds = 0.0);

    /**
     * @brief Execute a PowerShell script with named parameters (key/value pairs).
     * 
     * @param scriptPath Path to script file (.ps1)
     * @param namedArgs Map of parameter names to string values
     * @param timeoutSeconds Timeout for script execution (0 = use default)
     * @param dotSource If true, dot-source the script so its state persists in the session
     * @param raiseOnError If true, treat non-zero exit code as error
     * @return ExecutionResult Result of the script execution
     */
    ExecutionResult execute_script_kv(
        const std::string& scriptPath,
        const std::map<std::string, std::string>& namedArgs,
        double timeoutSeconds = 0.0,
        bool dotSource = false,
        bool raiseOnError = false);

    /**
     * @brief Execute a PowerShell script with positional arguments.
     * 
     * @param scriptPath Path to script file (.ps1)
     * @param args Vector of positional arguments
     * @param timeoutSeconds Timeout for script execution (0 = use default)
     * @param dotSource If true, dot-source the script so its state persists in the session
     * @param raiseOnError If true, treat non-zero exit code as error
     * @return ExecutionResult Result of the script execution
     */
    ExecutionResult execute_script(
        const std::string& scriptPath,
        const std::vector<std::string>& args,
        double timeoutSeconds = 0.0,
        bool dotSource = false,
        bool raiseOnError = false);

    /**
     * @brief Execute a command asynchronously.
     * 
     * @param command Command string to execute
     * @param callback Optional callback invoked when the result is available
    * @param timeoutSeconds Timeout (0 = use default)
     * @return std::future<ExecutionResult> Future that resolves when the command completes
     */
    std::future<ExecutionResult>
    executeAsync(std::string command,
                std::function<void(const ExecutionResult&)> callback = nullptr,
                double timeoutSeconds = 0.0);

    /**
     * @brief Execute a batch of commands asynchronously.
     * 
     * Provides optional progress callbacks and can stop early on first error.
     * 
     * @param commands Vector of command strings
     * @param progressCallback Callback reporting batch progress
     * @param stopOnFirstError Stop batch if one command fails
     * @param perCommandTimeoutSeconds Timeout per command (0 = use default)
     * @return std::future<std::vector<ExecutionResult>> Future resolving to results of all commands
     */
    std::future<std::vector<ExecutionResult>>
    executeAsync_batch(std::vector<std::string> commands,
                                 std::function<void(const BatchProgress&)> progressCallback,
                                 bool stopOnFirstError,
                                 double perCommandTimeoutSeconds = 0.0);

    /**
     * @brief Execute a script asynchronously with positional arguments.
     * 
     * @param scriptPath Path to script file
     * @param args Vector of positional arguments
     * @param timeoutSeconds Timeout for script execution (0 = use default)
     * @param dotSource If true, dot-source the script so its state persists in the session
     * @param raiseOnError If true, treat non-zero exit code as error
     * @param callback Optional callback invoked with the ExecutionResult
     * @return std::future<ExecutionResult> Future resolving to the script execution result
     */
    std::future<ExecutionResult> executeAsync_script(
        std::string scriptPath,
        std::vector<std::string> args,
        double timeoutSeconds,
        bool dotSource = false,
        bool raiseOnError = false,
        std::function<void(const ExecutionResult&)> callback = {}
    );

    /**
     * @brief Execute a script asynchronously with named parameters.
     * 
     * @param scriptPath Path to script file
     * @param namedArgs Map of parameter names to values
     * @param timeoutSeconds Timeout for script execution (0 = use default)
     * @param dotSource If true, dot-source the script so its state persists in the session
     * @param raiseOnError If true, treat non-zero exit code as error
     * @return std::future<ExecutionResult> Future resolving to the script execution result
     */
    std::future<ExecutionResult> executeAsync_script_kv(
        std::string scriptPath,
        std::map<std::string, std::string> namedArgs,
        double timeoutSeconds = 0.0,
        bool dotSource = false,
        bool raiseOnError = false);

    /**
     * @brief Send raw input to the PowerShell process.
     *
     * Typically used for interactive commands or scripts that read from stdin.
     *
     * @param input String to send to the process stdin
     * @return true if input was successfully written, false otherwise
     */
    bool sendInput(const std::string& input);

    /**
     * @brief Read from the PowerShell standard output.
     *
     * @param blocking If true, block until output is available. If false, return immediately.
     * @return Collected stdout text (empty if none available and non-blocking)
     */
    std::string readOutput(bool blocking = false);

    /**
     * @brief Read from the PowerShell standard error.
     *
     * @param blocking If true, block until error output is available. If false, return immediately.
     * @return Collected stderr text (empty if none available and non-blocking)
     */
    std::string readError(bool blocking = false);

    /**
     * @brief Change the working directory for the PowerShell process.
     *
     * @param directory Path to the new working directory
     * @return true if successfully set, false otherwise
     */
    bool setWorkingDirectory(const std::string& directory);

    /**
     * @brief Get the current working directory of the PowerShell process.
     *
     * @return Current working directory as a string
     */
    std::string getWorkingDirectory();

    /**
     * @brief Set an environment variable for the PowerShell process.
     *
     * @param name Name of the environment variable
     * @param value Value to assign
     * @return true if successfully set, false otherwise
     */
    bool setEnvironmentVariable(const std::string& name, const std::string& value);

    /**
     * @brief Get the value of an environment variable from the PowerShell process.
     *
     * @param name Name of the environment variable
     * @return Value of the variable, or empty string if not set
     */
    std::string getEnvironmentVariable(const std::string& name);

    /**
     * @brief Check if a given PowerShell module is available for import.
     *
     * @param moduleName Name of the module to check
     * @return true if available, false otherwise
     */
    bool isModuleAvailable(const std::string& moduleName);

    /**
     * @brief Import a PowerShell module into the current session.
     *
     * @param moduleName Name of the module to import
     * @return true if successfully imported, false otherwise
     */
    bool importModule(const std::string& moduleName);

    /**
     * @brief Get the version string of the running PowerShell process.
     *
     * @return Version string (e.g., "7.5.3")
     */
    std::string getPowerShellVersion();

    /**
     * @brief Get a list of available PowerShell modules in the current session.
     *
     * @return Vector of module names
     */
    std::vector<std::string> getAvailableModules();

    /**
     * @brief Get the current process configuration.
     *
     * @return Const reference to the Config object
     */
    const Config& getConfig() const { return config; }

    /**
     * @brief Update the configuration for the PowerShell process.
     *
     * Can only be called when the process is not running.
     *
     * @param newConfig New configuration values
     * @return true if successfully updated, false otherwise
     */
    bool updateConfig(const Config& newConfig);


private:
    /**
     * @brief Rehydrate a prior PowerShell session if snapshot metadata exists.
     */
    void restoreFromSnapshot_(const std::string& restoreScriptPath,
                             const std::string& snapshotPath);

    /**
     * @internal
     * @brief Handle an incoming data chunk from stdout or stderr.
     *
     * @param isErr True if the chunk came from stderr, false if from stdout
     * @param sv    String view of the data
     */
    /**
     * @brief Route a raw stdout/stderr chunk received from the process.
     */
    void onChunk_(bool isErr, std::string_view sv);
    /**
     * @brief Buffer a chunk for later readOutput/readError calls.
     */
    void enqueueStreamChunk_(bool isErr, std::string_view chunk);
    /**
     * @brief Apply stderr-specific handling (sentinels, ordering) while holding the lock.
     */
    void handleErrorChunk_(std::unique_lock<std::mutex>& lk, std::string_view chunk);
    /**
     * @brief Apply stdout-specific handling while holding the lock.
     */
    void handleOutputChunk_(std::unique_lock<std::mutex>& lk, std::string_view chunk);
    /**
     * @brief Ensure we have registered command metadata before processing output.
     */
    bool ensureCommandBegan_(uint64_t id, CmdState& state, std::string& carry);
    /**
     * @brief Attempt to resolve a command result once its sentinel is detected.
     */
    bool tryFinalizeCommand_(uint64_t id, CmdState& state, std::string& carry, std::string& nextCarry);
    /**
     * @brief Remove timeout sentinel tokens emitted on stderr when applicable.
     */
    bool stripTimeoutSentinel_(std::string& chunk, CmdState* state);
    /**
     * @brief Remove a command state from the tracking map while the state mutex is held.
     */
    std::unique_ptr<CmdState> eraseStateLocked_(uint64_t id);
    
    /**
     * @brief Produce a future that resolves to an immediate error result.
     */
    std::future<ExecutionResult> makeErrorFuture_(int exitCode, std::string_view err) const;
    /**
     * @brief Allocate a new unique command identifier.
     */
    uint64_t nextCommandId_();
    /**
     * @brief Create command bookkeeping structures for submission.
     */
    std::unique_ptr<CmdState> createCmdState_(uint64_t id,
                                              double timeoutSeconds,
                                              std::function<void(const ExecutionResult&)> cb);
    /**
     * @brief Register a command state (stateMx_ must already be locked).
     */
    void registerCmdStateLocked_(uint64_t id, std::unique_ptr<CmdState> state);
    /**
     * @brief Undo state changes when enqueueing a command fails midway.
     */
    void handleEnqueueFailure_(uint64_t id);

    /**
     * @brief Finalise a command and fulfil its future while stateMx_ is locked.
     */
    void completeCmdLocked_(CmdState& S, bool success);

    /**
     * @brief Send one-time initial commands into the PowerShell session (encoding, etc.).
     */
    bool sendInitialCommands_();

    /**
     * @internal
     * @brief Wait for the PowerShell process to become ready or exit.
     *
     * @param timeoutMs Timeout in milliseconds
     * @return True if process responded within timeout, false otherwise
     */
    bool waitForProcess_(int timeoutMs = 5000);
    
};
