// cpp/include/mem/shared_memory.hpp

#pragma once

#include <pybind11/functional.h>
#include <pybind11/gil.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace py = pybind11;

struct alignas(64) SharedMemoryHeader {
    static constexpr uint32_t kMagic = 0x4D485356; // 'VSHM'
    static constexpr uint32_t kVersion = 1;

    uint64_t magic_and_version;
    uint64_t frame_bytes;
    std::atomic<uint64_t> python_seq;
    std::atomic<uint64_t> powershell_seq;
    std::atomic<uint64_t> python_length;
    std::atomic<uint64_t> powershell_length;
    uint64_t reserved[10];
};

static_assert(sizeof(SharedMemoryHeader) == 128, "SharedMemoryHeader must be 128 bytes");

class SharedMemoryChannel {
public:
    static constexpr size_t HeaderBytes() noexcept { return sizeof(SharedMemoryHeader); }

    SharedMemoryChannel(std::string name, size_t n_slots, size_t frame_bytes)
        : name_(std::move(name)),
          frame_bytes_(frame_bytes),
          total_bytes_(HeaderBytes() + 2 * frame_bytes_)
    {
        if (frame_bytes_ == 0) {
            throw std::invalid_argument("frame_bytes must be positive");
        }
        if (n_slots != 1) {
            throw std::invalid_argument("SharedMemoryChannel v2 only supports a single slot");
        }
        if (frame_bytes_ > std::numeric_limits<uint64_t>::max() / 2) {
            throw std::invalid_argument("frame_bytes is too large");
        }

        create_shared_memory();

        header_ = static_cast<SharedMemoryHeader*>(pBuf_);
        python_to_ps_buffer_ = static_cast<uint8_t*>(pBuf_) + HeaderBytes();
        ps_to_python_buffer_ = python_to_ps_buffer_ + frame_bytes_;
    }

    SharedMemoryChannel(std::string name, size_t frame_bytes)
        : SharedMemoryChannel(std::move(name), 1, frame_bytes) {}

    ~SharedMemoryChannel() {
        destroy_shared_memory();
    }

    const std::string& name() const noexcept { return name_; }
    size_t frame_bytes() const noexcept { return frame_bytes_; }
    static size_t header_bytes_static() noexcept { return HeaderBytes(); }

    void WriteToPowerShell(py::buffer b) {
        py::buffer_info info = b.request();
        if (!info.ptr) {
            throw std::runtime_error("Buffer pointer is null");
        }
        if (info.size < 0) {
            throw std::runtime_error("Buffer size must be non-negative");
        }

        const size_t element_count = static_cast<size_t>(info.size);
        const size_t item_size = static_cast<size_t>(info.itemsize);
        if (item_size != 0 && element_count > std::numeric_limits<size_t>::max() / item_size) {
            throw std::runtime_error("Buffer size too large");
        }
        const size_t buffer_bytes = element_count * item_size;
        if (buffer_bytes > frame_bytes_) {
            throw std::runtime_error("Payload exceeds frame capacity");
        }

        const uint8_t* src = static_cast<const uint8_t*>(info.ptr);

        std::lock_guard<std::mutex> lock(p2p_mutex_);
        {
            py::gil_scoped_release release;
            if (buffer_bytes > 0) {
                std::memcpy(python_to_ps_buffer_, src, buffer_bytes);
            }
        }

        header_->python_length.store(static_cast<uint64_t>(buffer_bytes), std::memory_order_release);
        const uint64_t next = header_->python_seq.fetch_add(1ULL, std::memory_order_release) + 1ULL;
        (void)next;
    }

    py::bytes ReadFromPowerShell(uint64_t seq) {
        const uint64_t current_seq = GetPowerShellSeq();
        if (seq >= current_seq) {
            throw std::runtime_error("Sequence number not yet available from PowerShell");
        }
        const uint64_t length = header_->powershell_length.load(std::memory_order_acquire);
        if (length > frame_bytes_) {
            throw std::runtime_error("Payload length reported by PowerShell exceeds frame size");
        }

        PyObject* py_obj = PyBytes_FromStringAndSize(nullptr, static_cast<Py_ssize_t>(length));
        if (!py_obj) {
            throw std::runtime_error("Failed to allocate Python bytes buffer");
        }
        py::bytes result = py::reinterpret_steal<py::bytes>(py_obj);
        if (length == 0) {
            return result;
        }
        char* dst = PyBytes_AsString(result.ptr());
        if (!dst) {
            throw std::runtime_error("Failed to access Python bytes buffer");
        }
        {
            py::gil_scoped_release release;
            std::memcpy(dst, ps_to_python_buffer_, static_cast<size_t>(length));
        }
        return result;
    }

    void ReadIntoPowerShell(uint64_t seq, py::buffer out_buffer) {
        const uint64_t current_seq = GetPowerShellSeq();
        if (seq >= current_seq) {
            throw std::runtime_error("Sequence number not yet available from PowerShell");
        }

        py::buffer_info info = out_buffer.request();
        if (info.readonly) {
            throw std::runtime_error("Output buffer must be writable");
        }
        if (!info.ptr) {
            throw std::runtime_error("Output buffer pointer is null");
        }
        if (info.size < 0) {
            throw std::runtime_error("Output buffer size must be non-negative");
        }

        const size_t element_count = static_cast<size_t>(info.size);
        const size_t item_size = static_cast<size_t>(info.itemsize);
        if (item_size != 0 && element_count > std::numeric_limits<size_t>::max() / item_size) {
            throw std::runtime_error("Output buffer size too large");
        }
        const size_t available = element_count * item_size;
        const uint64_t length = header_->powershell_length.load(std::memory_order_acquire);
        if (length > frame_bytes_) {
            throw std::runtime_error("Payload length reported by PowerShell exceeds frame size");
        }
        if (available < length) {
            throw std::runtime_error("Output buffer is smaller than payload length");
        }

        if (length == 0) {
            return;
        }
        uint8_t* dst = static_cast<uint8_t*>(info.ptr);
        {
            py::gil_scoped_release release;
            std::memcpy(dst, ps_to_python_buffer_, static_cast<size_t>(length));
        }
    }

    uint64_t GetPowerShellSeq() const noexcept {
        if (!header_) {
            return 0;
        }
        return header_->powershell_seq.load(std::memory_order_acquire);
    }

    uint64_t GetPythonSeq() const noexcept {
        if (!header_) {
            return 0;
        }
        return header_->python_seq.load(std::memory_order_acquire);
    }

    uint64_t GetPowerShellLength() const noexcept {
        if (!header_) {
            return 0;
        }
        return header_->powershell_length.load(std::memory_order_acquire);
    }

    uint64_t GetPythonLength() const noexcept {
        if (!header_) {
            return 0;
        }
        return header_->python_length.load(std::memory_order_acquire);
    }

    

private:
    static constexpr uint64_t PackMagicAndVersion() noexcept {
        return (static_cast<uint64_t>(SharedMemoryHeader::kVersion) << 32) |
               static_cast<uint64_t>(SharedMemoryHeader::kMagic);
    }

    const std::string name_;
    const size_t frame_bytes_;
    const size_t total_bytes_;

    SharedMemoryHeader* header_ = nullptr;
    uint8_t* python_to_ps_buffer_ = nullptr;
    uint8_t* ps_to_python_buffer_ = nullptr;
    std::mutex p2p_mutex_;

#ifdef _WIN32
    HANDLE hMapFile_ = nullptr;
    void* pBuf_ = nullptr;

    void create_shared_memory() {
        const uint64_t total = total_bytes_;
        const DWORD size_high = static_cast<DWORD>((total >> 32) & 0xFFFFFFFFULL);
        const DWORD size_low = static_cast<DWORD>(total & 0xFFFFFFFFULL);
        hMapFile_ = CreateFileMappingA(
            INVALID_HANDLE_VALUE,
            NULL,
            PAGE_READWRITE,
            size_high,
            size_low,
            name_.c_str());
        if (hMapFile_ == NULL) {
            throw std::runtime_error("CreateFileMapping failed");
        }
        const DWORD last_error = GetLastError();
        const bool initialize = (last_error != ERROR_ALREADY_EXISTS);

        pBuf_ = MapViewOfFile(hMapFile_, FILE_MAP_ALL_ACCESS, 0, 0, total_bytes_);
        if (pBuf_ == NULL) {
            CloseHandle(hMapFile_);
            throw std::runtime_error("MapViewOfFile failed");
        }

        if (initialize) {
            std::memset(pBuf_, 0, total_bytes_);
            auto* header = static_cast<SharedMemoryHeader*>(pBuf_);
            header->magic_and_version = PackMagicAndVersion();
            header->frame_bytes = static_cast<uint64_t>(frame_bytes_);
            header->python_seq.store(0, std::memory_order_relaxed);
            header->powershell_seq.store(0, std::memory_order_relaxed);
            header->python_length.store(0, std::memory_order_relaxed);
            header->powershell_length.store(0, std::memory_order_relaxed);
        } else {
            auto* header = static_cast<SharedMemoryHeader*>(pBuf_);
            if (header->magic_and_version != PackMagicAndVersion()) {
                UnmapViewOfFile(pBuf_);
                CloseHandle(hMapFile_);
                throw std::runtime_error("Shared memory version mismatch");
            }
            if (header->frame_bytes != static_cast<uint64_t>(frame_bytes_)) {
                UnmapViewOfFile(pBuf_);
                CloseHandle(hMapFile_);
                throw std::runtime_error("Shared memory frame size mismatch");
            }
        }
    }

    void destroy_shared_memory() {
        if (pBuf_) {
            UnmapViewOfFile(pBuf_);
            pBuf_ = nullptr;
        }
        if (hMapFile_) {
            CloseHandle(hMapFile_);
            hMapFile_ = nullptr;
        }
    }
#else
    int shm_fd_ = -1;
    void* pBuf_ = nullptr;

    void create_shared_memory() {
        shm_fd_ = shm_open(name_.c_str(), O_RDWR | O_CREAT, 0666);
        if (shm_fd_ == -1) {
            throw std::runtime_error("shm_open failed");
        }

        struct stat st;
        if (fstat(shm_fd_, &st) == -1) {
            close(shm_fd_);
            throw std::runtime_error("fstat failed");
        }

        bool initialize = false;
        if (static_cast<uint64_t>(st.st_size) < total_bytes_) {
            if (ftruncate(shm_fd_, static_cast<off_t>(total_bytes_)) == -1) {
                close(shm_fd_);
                throw std::runtime_error("ftruncate failed");
            }
            initialize = true;
        } else if (st.st_size == 0) {
            if (ftruncate(shm_fd_, static_cast<off_t>(total_bytes_)) == -1) {
                close(shm_fd_);
                throw std::runtime_error("ftruncate failed");
            }
            initialize = true;
        }

        pBuf_ = mmap(nullptr, total_bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
        if (pBuf_ == MAP_FAILED) {
            close(shm_fd_);
            throw std::runtime_error("mmap failed");
        }

        auto* header = static_cast<SharedMemoryHeader*>(pBuf_);
        if (initialize || header->magic_and_version == 0) {
            std::memset(pBuf_, 0, total_bytes_);
            header->magic_and_version = PackMagicAndVersion();
            header->frame_bytes = static_cast<uint64_t>(frame_bytes_);
            header->python_seq.store(0, std::memory_order_relaxed);
            header->powershell_seq.store(0, std::memory_order_relaxed);
            header->python_length.store(0, std::memory_order_relaxed);
            header->powershell_length.store(0, std::memory_order_relaxed);
        } else {
            if (header->magic_and_version != PackMagicAndVersion()) {
                munmap(pBuf_, total_bytes_);
                close(shm_fd_);
                throw std::runtime_error("Shared memory version mismatch");
            }
            if (header->frame_bytes != static_cast<uint64_t>(frame_bytes_)) {
                munmap(pBuf_, total_bytes_);
                close(shm_fd_);
                throw std::runtime_error("Shared memory frame size mismatch");
            }
        }
    }

    void destroy_shared_memory() {
        if (pBuf_ && pBuf_ != MAP_FAILED) {
            munmap(pBuf_, total_bytes_);
            pBuf_ = nullptr;
        }
        if (shm_fd_ != -1) {
            close(shm_fd_);
            shm_fd_ = -1;
        }
        shm_unlink(name_.c_str());
    }
#endif
};