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

class IoPump {
public:
	using ChunkHandler = std::function<void(bool is_stderr, std::string_view chunk)>;

	IoPump();
	~IoPump();

	IoPump(const IoPump&) = delete;
	IoPump& operator=(const IoPump&) = delete;
	IoPump(IoPump&&) noexcept;
	IoPump& operator=(IoPump&&) noexcept;

	void start(Process& process, ChunkHandler handler);
	void stop();

	bool enqueue_write(std::string data);
	void drain();

	bool is_running() const noexcept { return running_.load(std::memory_order_acquire); }

private:
	void stop_locked_();
	void reader_loop_(bool is_stderr);
	void writer_loop_();
	void clear_write_queue_();
	ChunkHandler handler_snapshot_();

	std::atomic<bool> running_{false};
	Process* process_{nullptr};

	std::mutex lifecycle_mutex_;
	std::mutex handler_mutex_;
	ChunkHandler handler_{};

	std::mutex write_mutex_;
	std::condition_variable write_cv_;
	std::deque<std::string> write_queue_;

	std::thread stdout_thread_;
	std::thread stderr_thread_;
	std::thread writer_thread_;
};

} // namespace core
} // namespace virtualshell