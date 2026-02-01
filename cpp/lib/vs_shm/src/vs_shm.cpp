// vs_shm.cpp - Clean implementation of zero-copy shared memory
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>
#include <cstdio>
#include <cstring>
#include <string>
#include <memory>
#include "../include/vs_shm.h"

// =============================================================================
// INTERNAL HELPERS
// =============================================================================

static inline uint32_t atomic_load_u32(volatile LONG* ptr) {
    return static_cast<uint32_t>(InterlockedCompareExchange(ptr, 0, 0));
}

static inline void atomic_store_u32(volatile LONG* ptr, uint32_t value) {
    InterlockedExchange(ptr, static_cast<LONG>(value));
}

static inline uint64_t atomic_load_u64(volatile LONG64* ptr) {
    return static_cast<uint64_t>(InterlockedCompareExchange64(ptr, 0, 0));
}

static inline void atomic_store_u64(volatile LONG64* ptr, uint64_t value) {
    InterlockedExchange64(ptr, static_cast<LONG64>(value));
}

// =============================================================================
// CHANNEL STRUCTURE
// =============================================================================

struct Channel {
    HANDLE hMap = nullptr;
    HANDLE hMutex = nullptr;
    
    // Events for Python → PowerShell
    HANDLE evPy2PsReady = nullptr;  // Python signals chunk ready
    HANDLE evPy2PsAck = nullptr;    // PowerShell acks chunk
    
    // Events for PowerShell → Python
    HANDLE evPs2PyReady = nullptr;  // PowerShell signals chunk ready
    HANDLE evPs2PyAck = nullptr;    // Python acks chunk
    
    uint8_t* base = nullptr;
    size_t total_size = 0;
    
    VS_Header* header = nullptr;
    uint8_t* py2ps_region = nullptr;  // Python → PowerShell data
    uint8_t* ps2py_region = nullptr;  // PowerShell → Python data
};

// =============================================================================
// CHANNEL LIFECYCLE
// =============================================================================

VS_API VS_Channel VS_CreateChannel(const wchar_t* name, uint64_t frame_bytes) {
    if (!name || frame_bytes == 0) {
        return nullptr;
    }
    
    auto ch = std::make_unique<Channel>();
    
    // Calculate total size: header + two data regions
    uint64_t header_size = sizeof(VS_Header);
    uint64_t total = header_size + (frame_bytes * 2);
    
    if (total > SIZE_MAX) {
        return nullptr;
    }
    
    ch->total_size = static_cast<size_t>(total);
    
    // Create file mapping
    HANDLE hMap = CreateFileMappingW(
        INVALID_HANDLE_VALUE,
        nullptr,
        PAGE_READWRITE,
        static_cast<DWORD>(total >> 32),
        static_cast<DWORD>(total & 0xFFFFFFFF),
        name
    );
    
    if (!hMap) {
        return nullptr;
    }
    
    DWORD last_error = GetLastError();
    bool existed = (last_error == ERROR_ALREADY_EXISTS);
    
    ch->hMap = hMap;
    
    // Map view
    void* view = MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, ch->total_size);
    if (!view) {
        CloseHandle(hMap);
        return nullptr;
    }
    
    ch->base = static_cast<uint8_t*>(view);
    ch->header = reinterpret_cast<VS_Header*>(ch->base);
    ch->py2ps_region = ch->base + header_size;
    ch->ps2py_region = ch->py2ps_region + frame_bytes;
    
    // Initialize header if newly created
    if (!existed) {
        memset(ch->header, 0, sizeof(VS_Header));
        ch->header->magic = VS_MAGIC;
        ch->header->version = VS_VERSION;
        ch->header->frame_bytes = frame_bytes;
    } else {
        // Verify header
        if (ch->header->magic != VS_MAGIC || ch->header->version != VS_VERSION) {
            UnmapViewOfFile(view);
            CloseHandle(hMap);
            return nullptr;
        }
    }
    
    // Create synchronization objects
    wchar_t mutex_name[256];
    wchar_t ev1[256], ev2[256], ev3[256], ev4[256];
    
    swprintf_s(mutex_name, 256, L"%s:mtx", name);
    swprintf_s(ev1, 256, L"%s:py2ps:ready", name);
    swprintf_s(ev2, 256, L"%s:py2ps:ack", name);
    swprintf_s(ev3, 256, L"%s:ps2py:ready", name);
    swprintf_s(ev4, 256, L"%s:ps2py:ack", name);
    
    ch->hMutex = CreateMutexW(nullptr, FALSE, mutex_name);
    if (!ch->hMutex) {
        UnmapViewOfFile(view);
        CloseHandle(hMap);
        return nullptr;
    }
    
    // Create events (auto-reset)
    ch->evPy2PsReady = CreateEventW(nullptr, FALSE, FALSE, ev1);
    ch->evPy2PsAck = CreateEventW(nullptr, FALSE, FALSE, ev2);
    ch->evPs2PyReady = CreateEventW(nullptr, FALSE, FALSE, ev3);
    ch->evPs2PyAck = CreateEventW(nullptr, FALSE, FALSE, ev4);
    
    if (!ch->evPy2PsReady || !ch->evPy2PsAck || !ch->evPs2PyReady || !ch->evPs2PyAck) {
        if (ch->evPy2PsReady) CloseHandle(ch->evPy2PsReady);
        if (ch->evPy2PsAck) CloseHandle(ch->evPy2PsAck);
        if (ch->evPs2PyReady) CloseHandle(ch->evPs2PyReady);
        if (ch->evPs2PyAck) CloseHandle(ch->evPs2PyAck);
        CloseHandle(ch->hMutex);
        UnmapViewOfFile(view);
        CloseHandle(hMap);
        return nullptr;
    }
    
    return ch.release();
}

VS_API void VS_DestroyChannel(VS_Channel handle) {
    if (!handle) return;
    
    auto ch = static_cast<Channel*>(handle);
    
    if (ch->evPy2PsReady) CloseHandle(ch->evPy2PsReady);
    if (ch->evPy2PsAck) CloseHandle(ch->evPy2PsAck);
    if (ch->evPs2PyReady) CloseHandle(ch->evPs2PyReady);
    if (ch->evPs2PyAck) CloseHandle(ch->evPs2PyAck);
    if (ch->hMutex) CloseHandle(ch->hMutex);
    if (ch->base) UnmapViewOfFile(ch->base);
    if (ch->hMap) CloseHandle(ch->hMap);
    
    delete ch;
}

// =============================================================================
// PYTHON → POWERSHELL TRANSFER
// =============================================================================

VS_API int32_t VS_BeginPy2PsTransfer(VS_Channel handle, uint64_t total_size, uint64_t chunk_size) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    if (chunk_size > ch->header->frame_bytes) {
        return VS_ERR_TOO_LARGE;
    }
    
    DWORD wait = WaitForSingleObject(ch->hMutex, 5000);
    if (wait != WAIT_OBJECT_0) {
        return (wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    // Reset transfer state
    ch->header->py2ps.total_size = total_size;
    ch->header->py2ps.chunk_size = chunk_size;
    ch->header->py2ps.num_chunks = static_cast<uint32_t>((total_size + chunk_size - 1) / chunk_size);
    ch->header->py2ps.current_chunk = 0;
    ch->header->py2ps.chunk_ready = 0;
    ch->header->py2ps.transfer_done = 0;
    
    ReleaseMutex(ch->hMutex);
    return VS_OK;
}

VS_API int32_t VS_SendPy2PsChunk(VS_Channel handle, uint32_t chunk_index, const uint8_t* data, uint64_t length, uint32_t timeout_ms) {
    if (!handle || !data) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    if (length > ch->header->frame_bytes) {
        return VS_ERR_TOO_LARGE;
    }
    
    DWORD wait = WaitForSingleObject(ch->hMutex, timeout_ms);
    if (wait != WAIT_OBJECT_0) {
        return (wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    // Copy chunk to shared memory
    memcpy(ch->py2ps_region, data, static_cast<size_t>(length));
    
    // Update metadata
    ch->header->py2ps.current_chunk = chunk_index;
    ch->header->py2ps.chunk_offset = static_cast<uint64_t>(ch->py2ps_region - ch->base);
    ch->header->py2ps.chunk_length = length;
    atomic_store_u32(reinterpret_cast<volatile LONG*>(&ch->header->py2ps.chunk_ready), 1);
    
    ReleaseMutex(ch->hMutex);
    
    // Signal PowerShell
    SetEvent(ch->evPy2PsReady);
    
    return VS_OK;
}

VS_API int32_t VS_WaitPy2PsAck(VS_Channel handle, uint32_t timeout_ms) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    DWORD wait = WaitForSingleObject(ch->evPy2PsAck, timeout_ms);
    if (wait == WAIT_OBJECT_0) {
        return VS_OK;
    } else if (wait == WAIT_TIMEOUT) {
        return VS_TIMEOUT;
    } else {
        return VS_ERR_SYSTEM;
    }
}

VS_API int32_t VS_FinishPy2PsTransfer(VS_Channel handle) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    DWORD wait = WaitForSingleObject(ch->hMutex, 5000);
    if (wait != WAIT_OBJECT_0) {
        return (wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    atomic_store_u32(reinterpret_cast<volatile LONG*>(&ch->header->py2ps.transfer_done), 1);
    
    ReleaseMutex(ch->hMutex);
    return VS_OK;
}

// =============================================================================
// POWERSHELL → PYTHON TRANSFER
// =============================================================================

VS_API int32_t VS_BeginPs2PyTransfer(VS_Channel handle, uint64_t total_size, uint64_t chunk_size) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    if (chunk_size > ch->header->frame_bytes) {
        return VS_ERR_TOO_LARGE;
    }
    
    DWORD wait = WaitForSingleObject(ch->hMutex, 5000);
    if (wait != WAIT_OBJECT_0) {
        return (wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    ch->header->ps2py.total_size = total_size;
    ch->header->ps2py.chunk_size = chunk_size;
    ch->header->ps2py.num_chunks = static_cast<uint32_t>((total_size + chunk_size - 1) / chunk_size);
    ch->header->ps2py.current_chunk = 0;
    ch->header->ps2py.chunk_ready = 0;
    ch->header->ps2py.transfer_done = 0;
    
    ReleaseMutex(ch->hMutex);
    return VS_OK;
}

VS_API int32_t VS_SendPs2PyChunk(VS_Channel handle, uint32_t chunk_index, const uint8_t* data, uint64_t length, uint32_t timeout_ms) {
    if (!handle || !data) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    if (length > ch->header->frame_bytes) {
        return VS_ERR_TOO_LARGE;
    }
    
    DWORD wait = WaitForSingleObject(ch->hMutex, timeout_ms);
    if (wait != WAIT_OBJECT_0) {
        return (wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    // Copy chunk to shared memory
    memcpy(ch->ps2py_region, data, static_cast<size_t>(length));
    
    // Update metadata
    ch->header->ps2py.current_chunk = chunk_index;
    ch->header->ps2py.chunk_offset = static_cast<uint64_t>(ch->ps2py_region - ch->base);
    ch->header->ps2py.chunk_length = length;
    atomic_store_u32(reinterpret_cast<volatile LONG*>(&ch->header->ps2py.chunk_ready), 1);
    
    ReleaseMutex(ch->hMutex);
    
    // Signal Python
    SetEvent(ch->evPs2PyReady);
    
    return VS_OK;
}

VS_API int32_t VS_WaitPs2PyAck(VS_Channel handle, uint32_t timeout_ms) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    DWORD wait = WaitForSingleObject(ch->evPs2PyAck, timeout_ms);
    if (wait == WAIT_OBJECT_0) {
        return VS_OK;
    } else if (wait == WAIT_TIMEOUT) {
        return VS_TIMEOUT;
    } else {
        return VS_ERR_SYSTEM;
    }
}

VS_API int32_t VS_FinishPs2PyTransfer(VS_Channel handle) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    DWORD wait = WaitForSingleObject(ch->hMutex, 5000);
    if (wait != WAIT_OBJECT_0) {
        return (wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    atomic_store_u32(reinterpret_cast<volatile LONG*>(&ch->header->ps2py.transfer_done), 1);
    
    ReleaseMutex(ch->hMutex);
    return VS_OK;
}

// =============================================================================
// ZERO-COPY RECEIVE (Python reads PowerShell chunks)
// =============================================================================

VS_API int32_t VS_WaitPs2PyChunk(VS_Channel handle, uint32_t* out_chunk_index, uint64_t* out_offset, uint64_t* out_length, uint32_t timeout_ms) {
    if (!handle || !out_chunk_index || !out_offset || !out_length) {
        return VS_ERR_INVALID;
    }
    auto ch = static_cast<Channel*>(handle);
    
    // Wait for PowerShell to signal chunk ready
    DWORD wait = WaitForSingleObject(ch->evPs2PyReady, timeout_ms);
    if (wait == WAIT_TIMEOUT) {
        return VS_TIMEOUT;
    } else if (wait != WAIT_OBJECT_0) {
        return VS_ERR_SYSTEM;
    }
    
    // Read metadata
    DWORD mutex_wait = WaitForSingleObject(ch->hMutex, timeout_ms);
    if (mutex_wait != WAIT_OBJECT_0) {
        return (mutex_wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    *out_chunk_index = ch->header->ps2py.current_chunk;
    *out_offset = ch->header->ps2py.chunk_offset;
    *out_length = ch->header->ps2py.chunk_length;
    
    ReleaseMutex(ch->hMutex);
    return VS_OK;
}

VS_API int32_t VS_AckPs2PyChunk(VS_Channel handle) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    DWORD wait = WaitForSingleObject(ch->hMutex, 5000);
    if (wait != WAIT_OBJECT_0) {
        return (wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    atomic_store_u32(reinterpret_cast<volatile LONG*>(&ch->header->ps2py.chunk_ready), 0);
    
    ReleaseMutex(ch->hMutex);
    
    // Signal PowerShell that chunk was consumed
    SetEvent(ch->evPs2PyAck);
    return VS_OK;
}

VS_API int32_t VS_IsPs2PyComplete(VS_Channel handle) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    uint32_t done = atomic_load_u32(reinterpret_cast<volatile LONG*>(&ch->header->ps2py.transfer_done));
    return done ? 1 : 0;
}

// =============================================================================
// ZERO-COPY RECEIVE (PowerShell reads Python chunks)
// =============================================================================

VS_API int32_t VS_WaitPy2PsChunk(VS_Channel handle, uint32_t* out_chunk_index, uint64_t* out_offset, uint64_t* out_length, uint32_t timeout_ms) {
    if (!handle || !out_chunk_index || !out_offset || !out_length) {
        return VS_ERR_INVALID;
    }
    auto ch = static_cast<Channel*>(handle);
    
    // Wait for Python to signal chunk ready
    DWORD wait = WaitForSingleObject(ch->evPy2PsReady, timeout_ms);
    if (wait == WAIT_TIMEOUT) {
        return VS_TIMEOUT;
    } else if (wait != WAIT_OBJECT_0) {
        return VS_ERR_SYSTEM;
    }
    
    // Read metadata
    DWORD mutex_wait = WaitForSingleObject(ch->hMutex, timeout_ms);
    if (mutex_wait != WAIT_OBJECT_0) {
        return (mutex_wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    *out_chunk_index = ch->header->py2ps.current_chunk;
    *out_offset = ch->header->py2ps.chunk_offset;
    *out_length = ch->header->py2ps.chunk_length;
    
    ReleaseMutex(ch->hMutex);
    return VS_OK;
}

VS_API int32_t VS_AckPy2PsChunk(VS_Channel handle) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    DWORD wait = WaitForSingleObject(ch->hMutex, 5000);
    if (wait != WAIT_OBJECT_0) {
        return (wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    atomic_store_u32(reinterpret_cast<volatile LONG*>(&ch->header->py2ps.chunk_ready), 0);
    
    ReleaseMutex(ch->hMutex);
    
    // Signal Python that chunk was consumed
    SetEvent(ch->evPy2PsAck);
    return VS_OK;
}

VS_API int32_t VS_IsPy2PsComplete(VS_Channel handle) {
    if (!handle) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    uint32_t done = atomic_load_u32(reinterpret_cast<volatile LONG*>(&ch->header->py2ps.transfer_done));
    return done ? 1 : 0;
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

VS_API void* VS_GetMemoryBase(VS_Channel handle) {
    if (!handle) return nullptr;
    auto ch = static_cast<Channel*>(handle);
    return ch->base;
}

VS_API int32_t VS_GetHeader(VS_Channel handle, VS_Header* out) {
    if (!handle || !out) return VS_ERR_INVALID;
    auto ch = static_cast<Channel*>(handle);
    
    DWORD wait = WaitForSingleObject(ch->hMutex, 5000);
    if (wait != WAIT_OBJECT_0) {
        return (wait == WAIT_TIMEOUT) ? VS_TIMEOUT : VS_ERR_SYSTEM;
    }
    
    memcpy(out, ch->header, sizeof(VS_Header));
    
    ReleaseMutex(ch->hMutex);
    return VS_OK;
}

// =============================================================================
// OBJECT SERIALIZATION - Forward declarations (implemented in object_serializer.cpp)
// =============================================================================

extern "C" {
    extern int SerializeObjectViaGCHandle(intptr_t gc_handle, const wchar_t* encoding_name, uint8_t** out_bytes, uint64_t* out_len);
    extern void FreeManagedBytes(uint8_t* bytes);
}

VS_API int32_t VS_SerializeObject(intptr_t gc_handle, uint8_t** out_bytes, uint64_t* out_length) {
    if (!out_bytes || !out_length) return VS_ERR_INVALID;
    
    int result = SerializeObjectViaGCHandle(gc_handle, L"utf-8", out_bytes, out_length);
    return (result == 0) ? VS_OK : VS_ERR_SYSTEM;
}

VS_API void VS_FreeBytes(uint8_t* bytes) {
    if (bytes) {
        FreeManagedBytes(bytes);
    }
}

