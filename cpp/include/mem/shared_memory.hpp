// cpp/include/mem/shared_memory.hpp

#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>
#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>
#include <chrono>
#include <cstring>
#include <string>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace py = pybind11;

// Header som ligger i starten av det delte minnet
struct SharedMemoryHeader {
    std::atomic<uint64_t> p2p_seq;
    std::atomic<uint64_t> ps2p_seq;
};

class SharedMemoryChannel {
public:
    SharedMemoryChannel(std::string name, size_t n_slots, size_t frame_bytes)
        : name_(std::move(name)),
          n_slots_(n_slots),
          frame_bytes_(frame_bytes)
    {
        size_t single_buffer_bytes = n_slots * frame_bytes;
        total_bytes_ = sizeof(SharedMemoryHeader) + 2 * single_buffer_bytes;

        create_shared_memory();

        // Initialiser pekere
        header_ = static_cast<SharedMemoryHeader*>(pBuf_);
        p2p_buffer_start_ = static_cast<uint8_t*>(pBuf_) + sizeof(SharedMemoryHeader);
        ps2p_buffer_start_ = p2p_buffer_start_ + single_buffer_bytes;

        // Nullstill tellere ved opprettelse
        header_->p2p_seq.store(0, std::memory_order_relaxed);
        header_->ps2p_seq.store(0, std::memory_order_relaxed);
    }

    ~SharedMemoryChannel() {
        destroy_shared_memory();
    }

    const std::string& name() const { return name_; }
    size_t n_slots() const noexcept { return n_slots_; }
    size_t frame_bytes() const noexcept { return frame_bytes_; }

    // Skriv data TIL PowerShell
    void WriteToPowerShell(py::buffer b) {
        py::buffer_info info = b.request();
        if (static_cast<size_t>(info.size) != frame_bytes_) {
            throw std::runtime_error("Buffer length must equal frame_bytes");
        }

        uint64_t next = header_->p2p_seq.load(std::memory_order_relaxed);
        size_t slot = static_cast<size_t>(next % n_slots_);
        uint8_t* dst = p2p_buffer_start_ + slot * frame_bytes_;
        std::memcpy(dst, info.ptr, frame_bytes_);

        header_->p2p_seq.store(next + 1, std::memory_order_release);
    }

    // Les data FRA PowerShell
    py::bytes ReadFromPowerShell(uint64_t seq) {
        uint64_t current_seq = GetPowerShellSeq();
        if (seq >= current_seq) {
            throw std::runtime_error("Sequence number not yet available from PowerShell.");
        }
        size_t slot = seq % n_slots_;
        uint8_t* src = ps2p_buffer_start_ + slot * frame_bytes_;
        return py::bytes(reinterpret_cast<const char*>(src), frame_bytes_);
    }

    // Hent siste sekvensnummer FRA PowerShell
    uint64_t GetPowerShellSeq() const noexcept {
        if (!header_) return 0;
        return header_->ps2p_seq.load(std::memory_order_acquire);
    }
    
    // Hent siste sekvensnummer TIL PowerShell
    uint64_t GetPythonSeq() const noexcept {
        if (!header_) return 0;
        return header_->p2p_seq.load(std::memory_order_acquire);
    }


private:
    const std::string name_;
    const size_t n_slots_;
    const size_t frame_bytes_;
    size_t total_bytes_;

    SharedMemoryHeader* header_;
    uint8_t* p2p_buffer_start_; // Python -> PS buffer
    uint8_t* ps2p_buffer_start_; // PS -> Python buffer

#ifdef _WIN32
    HANDLE hMapFile_ = nullptr;
    void* pBuf_ = nullptr;

    void create_shared_memory() {
        hMapFile_ = CreateFileMappingA(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, (DWORD)total_bytes_, name_.c_str());
        if (hMapFile_ == NULL) throw std::runtime_error("CreateFileMapping failed");
        pBuf_ = MapViewOfFile(hMapFile_, FILE_MAP_ALL_ACCESS, 0, 0, total_bytes_);
        if (pBuf_ == NULL) {
            CloseHandle(hMapFile_);
            throw std::runtime_error("MapViewOfFile failed");
        }
    }

    void destroy_shared_memory() {
        if (pBuf_) UnmapViewOfFile(pBuf_);
        if (hMapFile_) CloseHandle(hMapFile_);
    }
#else // POSIX
    int shm_fd_ = -1;
    void* pBuf_ = nullptr;

    void create_shared_memory() {
        shm_fd_ = shm_open(name_.c_str(), O_CREAT | O_RDWR, 0666);
        if (shm_fd_ == -1) throw std::runtime_error("shm_open failed");
        if (ftruncate(shm_fd_, total_bytes_) == -1) {
            close(shm_fd_);
            shm_unlink(name_.c_str());
            throw std::runtime_error("ftruncate failed");
        }
        pBuf_ = mmap(0, total_bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
        if (pBuf_ == MAP_FAILED) {
            close(shm_fd_);
            shm_unlink(name_.c_str());
            throw std::runtime_error("mmap failed");
        }
    }

    void destroy_shared_memory() {
        if (pBuf_ != MAP_FAILED) munmap(pBuf_, total_bytes_);
        if (shm_fd_ != -1) close(shm_fd_);
        shm_unlink(name_.c_str());
    }
#endif
};