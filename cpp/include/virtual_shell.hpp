#pragma once
#
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

#define READ_BUFFER_SIZE (64 * 1024) // 64 KB buffer for reading output, used in readOutput() and readError()
#define INITIAL_COMMANDS_BUF_SIZE (8 * 1024) // 8 KB buffer for initial commands, used in sendInitialCommands()

#ifdef _WIN32
    #include <io.h>
    #include <fcntl.h>
    #include <windows.h>

    // Non-owning wrapper for an overlapped pipe handle with a reusable buffer.
    struct OverlappedPipe {
        HANDLE      h{INVALID_HANDLE_VALUE};
        OVERLAPPED  ov{};
        bool        pending{false};
        std::vector<char> buf;
        OverlappedPipe() {
            ZeroMemory(&ov, sizeof(ov));
            ov.hEvent = ::CreateEvent(nullptr, /*manualReset*/ TRUE, /*initialState*/ FALSE, nullptr);
            buf.resize(64 * 1024); // adjust if needed
        }
        ~OverlappedPipe() {
            if (ov.hEvent) ::CloseHandle(ov.hEvent);
        }
    };
    
    static std::string wstring_to_utf8(const std::wstring& w);
    static bool send_ctrl_break(DWORD pid);
    static bool complete_overlapped(HANDLE h, OVERLAPPED& ov, DWORD& bytes, bool blocking);

    static std::string read_overlapped_once(OverlappedPipe& P, bool blocking);
#else
    #include <unistd.h>
    #include <sys/wait.h>
    #include <signal.h>
    #include <fcntl.h>
    #include <poll.h>
    #include <sys/types.h>
    #include <errno.h>
#endif

/**
 * @brief Headless PowerShell 7 process host.
 *
 * Start, communicate with, and control a PowerShell 7 process programmatically,
 * without showing a window.
 */
class VirtualShell : public std::enable_shared_from_this<VirtualShell> {
    static std::string build_pwsh_packet(uint64_t id, std::string_view command);
public:
    struct OutChunk { bool isErr; std::string data; };

    /**
     * @brief Result of a PowerShell command.
     */
    struct ExecutionResult {
        std::string out;        ///< Stdout from the command
        std::string err;        ///< Stderr from the command
        int         exitCode;      ///< Exit code (0 = success)
        bool        success;       ///< Whether the command completed successfully
        double      executionTime; ///< Execution time in seconds
    };

    /**
     * @brief Progress callback payload for batch executions.
     */
    struct BatchProgress {
        size_t currentCommand;                 ///< Index of the current command in the batch
        size_t totalCommands;                  ///< Total number of commands in the batch
        ExecutionResult lastResult;            ///< Result of the most recently completed command
        bool isComplete;                       ///< True when the batch has finished
        std::vector<ExecutionResult> allResults; ///< Results for all commands (filled at completion)
    };

    /**
     * @brief Configuration for the PowerShell process.
     */
    struct Config {
        std::string powershellPath = "pwsh.exe";  ///< Path to the PowerShell executable
        std::string workingDirectory = "";        ///< Working directory (empty = current directory)
        bool captureOutput = true;                ///< Capture stdout
        bool captureError  = true;                ///< Capture stderr
        int  timeoutSeconds = 30;                 ///< Default per-command timeout (seconds)
        std::map<std::string, std::string> environment;   ///< Extra environment variables
        std::vector<std::string> initialCommands;         ///< Commands to run right after startup
    };

    /**
     * @brief Internal state for a single in-flight command.
     */
    struct CmdState {
        uint64_t                           id;          ///< Unique command identifier
        std::promise<ExecutionResult>      prom;        ///< Promise to deliver the command result
        std::string                        outBuf;      ///< Accumulated stdout buffer
        std::string                        errBuf;      ///< Accumulated stderr buffer
        std::string                        endMarker;   ///< Unique marker string (e.g. "\x1ESS_END_123\x1E")
        std::atomic<bool>                  done{false}; ///< True once command is completed
        std::atomic<bool>                  timedOut{false}; ///< True if command exceeded timeout
        double                             startMonotonic{}; ///< Start time in monotonic seconds
        double                             timeoutSec{}; ///< Timeout in seconds for this command
        std::function<void(const ExecutionResult&)> cb;  ///< Optional callback for completion
        std::chrono::steady_clock::time_point tStart{};   ///< Start timestamp
        std::chrono::steady_clock::time_point tDeadline{};///< Absolute deadline for timeout
    };

private:
#ifdef _WIN32
    HANDLE hInputWrite = NULL;   ///< Write end of stdin pipe
    HANDLE hInputRead  = NULL;   ///< Read end of stdin pipe
    HANDLE hOutputWrite = NULL;  ///< Write end of stdout pipe
    HANDLE hOutputRead  = NULL;  ///< Read end of stdout pipe
    HANDLE hErrorWrite  = NULL;  ///< Write end of stderr pipe
    HANDLE hErrorRead   = NULL;  ///< Read end of stderr pipe
    HANDLE hProcess     = NULL;  ///< Process handle
    HANDLE hThread      = NULL;  ///< Primary thread handle
    PROCESS_INFORMATION processInfo = {}; ///< Process metadata
    OverlappedPipe outPipe_; ///< Overlapped pipe for stdout
    OverlappedPipe errPipe_; ///< Overlapped pipe for stderr
#else
    int inputPipe[2]  = {-1, -1}; ///< Stdin pipe [read, write]
    int outputPipe[2] = {-1, -1}; ///< Stdout pipe [read, write]
    int errorPipe[2]  = {-1, -1}; ///< Stderr pipe [read, write]
    pid_t processId   = -1;       ///< Child process ID
#endif

    Config config;                        ///< Current process configuration
    std::atomic<bool> isRunning_{false};  ///< True if PowerShell process is alive
    std::atomic<bool> shouldStop{false};  ///< Flag to request process termination
    
    std::thread writerTh_;                ///< Writer thread (stdin feeder)
    std::thread rOutTh_;                  ///< Reader thread for stdout
    std::thread rErrTh_;                  ///< Reader thread for stderr
    std::atomic<bool> ioRunning_{false};  ///< True while I/O threads are active

    std::mutex              writeMx_;     ///< Protects writeQueue_
    std::condition_variable writeCv_;     ///< Signals new data in writeQueue_
    std::deque<std::string> writeQueue_;  ///< Pending stdin packets

    std::mutex              chunkMx_;     ///< Protects chunkQueue_
    std::condition_variable chunkCv_;     ///< Signals new data in chunkQueue_
    std::deque<OutChunk>    chunkQueue_;  ///< Buffered stdout/stderr chunks

    std::mutex stateMx_; ///< Protects inflight_ and inflightOrder_
    std::unordered_map<uint64_t, std::unique_ptr<CmdState>> inflight_; ///< Active commands by ID

    std::atomic<uint64_t> seq_{0}; ///< Monotonic sequence for command IDs

    /**
     * @internal
     * @brief Enqueue a command for execution.
     * @param command Command string to run
     * @param timeoutSeconds Timeout (0 = use default)
     * @param cb Optional completion callback
     * @return Future resolving to the ExecutionResult
     */
    std::future<ExecutionResult> submit(std::string command,
                                        double timeoutSeconds,
                                        std::function<void(const ExecutionResult&)> cb = nullptr);

    std::string lastOutput; ///< Last captured stdout (for sync APIs)
    std::string lastError;  ///< Last captured stderr (for sync APIs)

    std::atomic<uint32_t> inflightCount_{0}; ///< Current number of in-flight commands
    std::atomic<uint32_t> highWater_{0};     ///< High-water mark of in-flight commands
    std::deque<uint64_t>  inflightOrder_;    ///< FIFO order of in-flight command IDs

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

    /**
     * @internal
     * @brief Handle a single timed-out command.
     * 
     * Builds a timeout ExecutionResult, signals its promise/callback,
     * and updates inflight bookkeeping.
     * @param id Command identifier
     */
    void timeoutOne_(uint64_t id);

    /**
     * @internal
     * @brief Periodically scan all in-flight commands for timeouts.
     * 
     * Runs inside timerThread_, checks tDeadline for each CmdState,
     * and calls timeoutOne_ as needed.
     */
    void timeoutScan_();

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
     * @return ExecutionResult Final result (aggregate or last command depending on implementation)
     */
    ExecutionResult execute_batch(const std::vector<std::string>& commands, double timeoutSeconds = 0.0);

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
        double timeoutSeconds,
        bool dotSource = false,
        bool raiseOnError = false);

    /**
     * @brief Execute a command asynchronously.
     * 
     * @param command Command string to execute
     * @param callback Optional callback invoked when the result is available
     * @return std::future<ExecutionResult> Future that resolves when the command completes
     */
    std::future<ExecutionResult>
    executeAsync(std::string command,
                std::function<void(const ExecutionResult&)> callback = nullptr);

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
        double timeoutSeconds,
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
     * @internal
     * @brief Main loop for writing commands to the PowerShell process stdin.
     *
     * Runs in its own thread. Pulls packets from writeQueue_ and writes them to the process.
     */
    void writerLoop_();

    /**
     * @internal
     * @brief Main loop for reading from the PowerShell stdout pipe.
     *
     * Runs in its own thread. Dispatches received data chunks to onChunk_ with isErr=false.
     */
    void readerStdoutLoop_();

    /**
     * @internal
     * @brief Main loop for reading from the PowerShell stderr pipe.
     *
     * Runs in its own thread. Dispatches received data chunks to onChunk_ with isErr=true.
     */
    void readerStderrLoop_();

    /**
     * @internal
     * @brief Perform a single overlapped read from stdout (Windows only).
     *
     * @param blocking If true, block until data or EOF is available.
     * @return Read data chunk (empty string if none available and non-blocking)
     */
    std::string readOutputOverlapped_(bool blocking);

    /**
     * @internal
     * @brief Perform a single overlapped read from stderr (Windows only).
     *
     * @param blocking If true, block until data or EOF is available.
     * @return Read error chunk (empty string if none available and non-blocking)
     */
    std::string readErrorOverlapped_(bool blocking);

    /**
     * @internal
     * @brief Handle an incoming data chunk from stdout or stderr.
     *
     * @param isErr True if the chunk came from stderr, false if from stdout
     * @param sv    String view of the data
     */
    void onChunk_(bool isErr, std::string_view sv);

    /**
     * @internal
     * @brief Complete a command when its end marker is detected.
     *
     * @param S Command state to finalize
     * @param success Whether the command completed successfully
     */
    void completeCmdLocked_(CmdState& S, bool success);

    /**
     * @internal
     * @brief Start background I/O threads (writer, stdout reader, stderr reader).
     */
    void startIoThreads_();

    /**
     * @internal
     * @brief Stop background I/O threads and join them cleanly.
     */
    void stopIoThreads_();

    /**
     * @internal
     * @brief Send the configured initial commands to the PowerShell session.
     *
     * Used after process startup to set encoding, error policy, or custom initialization.
     * @return True on success, false otherwise
     */
    bool sendInitialCommands();
    
    /**
     * @internal
     * @brief Create pipes for stdin, stdout, and stderr communication.
     *
     * Platform-specific setup of process I/O redirection.
     * @return True on success, false otherwise
     */
    bool createPipes();
    
    /**
     * @internal
     * @brief Close all process pipes (stdin, stdout, stderr).
     *
     * Safe to call multiple times; used during stop() and destructor cleanup.
     */
    void closePipes();
    
#ifdef _WIN32
    /**
     * @internal
     * @brief Read data from a Windows pipe handle.
     *
     * @param handle Pipe handle
     * @param buffer Destination buffer
     * @param size   Maximum bytes to read
     * @return Number of bytes read, or 0/negative on error
     */
    DWORD readFromPipe(HANDLE handle, char* buffer, DWORD size);
#else
    /**
     * @internal
     * @brief Read data from a POSIX file descriptor.
     *
     * @param fd     File descriptor
     * @param buffer Destination buffer
     * @param size   Maximum bytes to read
     * @return Number of bytes read, or -1 on error
     */
    ssize_t readFromPipe(int fd, char* buffer, size_t size);
#endif
    
#ifdef _WIN32
    /**
     * @internal
     * @brief Write data to a Windows pipe handle.
     *
     * @param handle Pipe handle
     * @param data   String to write
     * @return True on success, false otherwise
     */
    bool writeToPipe(HANDLE handle, const std::string& data);
#else
    /**
     * @internal
     * @brief Write data to a POSIX file descriptor.
     *
     * @param fd   File descriptor
     * @param data String to write
     * @return True on success, false otherwise
     */
    bool writeToPipe(int fd, const std::string& data);
#endif
    
    /**
     * @internal
     * @brief Wait for the PowerShell process to become ready or exit.
     *
     * @param timeoutMs Timeout in milliseconds
     * @return True if process responded within timeout, false otherwise
     */
    bool waitForProcess(int timeoutMs = 5000);

};
