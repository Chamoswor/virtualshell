#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

namespace virtualshell {
namespace core {

class Process; // forward declaration; concrete implementation provided separately.

/**
 * @brief Multiplexed I/O helper that bridges the child process and VirtualShell.
 *
 * Spawns reader threads for stdout/stderr and a writer thread for stdin, pushing
 * data into user-provided handlers while buffering outbound writes.
 */
class IoPump {
public:
	using ChunkHandler = std::function<void(bool is_stderr, std::string_view chunk)>;
	using ExitHandler = std::function<void()>;

	IoPump();
	~IoPump();

	IoPump(const IoPump&) = delete;
	IoPump& operator=(const IoPump&) = delete;
	IoPump(IoPump&&) noexcept;
	IoPump& operator=(IoPump&&) noexcept;

	 /**
	  * @brief Begin pumping I/O for the supplied process.
	  *
	  * Launches reader/writer threads and installs the callback that receives
	  * stdout/stderr chunks. `exit_handler` (optional) is invoked once, from a
	  * reader thread, when the process's output pipes reach EOF, i.e. when the
	  * child exited or was torn down. It must not call back into stop().
	  */
	 void start(Process& process, ChunkHandler handler, ExitHandler exit_handler = {});
	 /**
	  * @brief Stop all I/O threads and detach from the process.
	  */
	 void stop();

	 /**
	  * @brief Queue data for asynchronous writing to the child stdin pipe.
	  */
	bool enqueue_write(std::string data);
	 /**
	  * @brief Block until the write queue has been flushed.
	  */
	void drain();

	 /**
	  * @brief Check whether the pump is currently active.
	  */
	bool is_running() const noexcept { return running_.load(std::memory_order_acquire); }

private:
	 /**
	  * @brief Stop helper that assumes lifecycle_mutex_ is held.
	  */
	 void stop_locked_();
	 /**
	  * @brief Main loop for stdout/stderr reader threads.
	  */
	 void reader_loop_(bool is_stderr);
	 /**
	  * @brief Main loop for draining queued writes.
	  */
	 void writer_loop_();
	 /**
	  * @brief Release any pending writes when shutting down.
	  */
	 void clear_write_queue_();
	 /**
	  * @brief Safely snapshot the current chunk handler callback.
	  */
	 ChunkHandler handler_snapshot_();
	 /**
	  * @brief Safely snapshot the current exit callback.
	  */
	 ExitHandler exit_handler_snapshot_();

	std::atomic<bool> running_{false};
	std::atomic<bool> exit_notified_{false}; ///< Exit handler fired for the current process
	Process* process_{nullptr};

	std::mutex lifecycle_mutex_;
	std::mutex handler_mutex_;
	ChunkHandler handler_{};
	ExitHandler exit_handler_{};

	std::mutex write_mutex_;
	std::condition_variable write_cv_;
	std::deque<std::string> write_queue_;

	std::thread stdout_thread_;
	std::thread stderr_thread_;
	std::thread writer_thread_;
};

} // namespace core
} // namespace virtualshell