/// Defines the Process interface consumed by the IoPump. The concrete
/// implementation will own the underlying OS handles and provide blocking read
/// and write helpers that the pump can call from its worker threads.

#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace virtualshell {
namespace core {

class Process {
public:
	virtual ~Process() = default;

	/// Write raw bytes to the child stdin stream.
	virtual bool write(std::string_view data) = 0;

	/// Blocking read from stdout. Returns nullopt if the stream is closed.
	virtual std::optional<std::string> read_stdout() = 0;

	/// Blocking read from stderr. Returns nullopt if the stream is closed.
	virtual std::optional<std::string> read_stderr() = 0;

	/// Close or otherwise interrupt any blocking I/O operations.
	virtual void shutdown_streams() = 0;
};

} // namespace core
} // namespace virtualshell
