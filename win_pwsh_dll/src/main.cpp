// src/vs_shm.cpp
#if not defined(WIN32_LEAN_AND_MEAN)
    #define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <memory>
#include <algorithm>
#include "../include/vs_shm.h"

static constexpr uint32_t MAGIC = VS_HEADER_MAGIC;
static constexpr uint32_t VER   = VS_HEADER_VERSION;
static constexpr uint64_t MAGIC64 = (static_cast<uint64_t>(VER) << 32) | static_cast<uint64_t>(MAGIC);

static inline uint64_t atomic_load_u64(volatile LONG64* ptr) {
    return static_cast<uint64_t>(InterlockedCompareExchange64(ptr, 0, 0));
}

static inline void atomic_store_u64(volatile LONG64* ptr, uint64_t value) {
    InterlockedExchange64(ptr, static_cast<LONG64>(value));
}

static inline uint64_t atomic_inc_u64(volatile LONG64* ptr) {
    return static_cast<uint64_t>(InterlockedIncrement64(ptr));
}

struct Channel {
    HANDLE  hMap   = nullptr;
    HANDLE  hMutex = nullptr;
    HANDLE  evPsReq = nullptr, evPsAck = nullptr;
    HANDLE  evPyReq = nullptr, evPyAck = nullptr;
    uint8_t* base  = nullptr;
    size_t   total = 0;

    VS_Header* hdr = nullptr;
    uint8_t* ps2p  = nullptr; // PS→PY region
    uint8_t* py2p  = nullptr; // PY→PS region

    uint64_t last_python_seq_read = 0;    // last sequence number consumed from PY→PS region
    uint64_t last_powershell_seq_read = 0; // last sequence number consumed from PS→PY region
};

static std::wstring makeMutexName(const std::wstring& base) { return base + L":mtx"; }
static std::wstring makeEventName(const std::wstring& base, const wchar_t* suffix) { return base + suffix; }

// Compute sizes and pointers
static bool map_layout(Channel* c) {
    SIZE_T header_sz = sizeof(VS_Header);
    uint64_t frame = c->hdr->frame_bytes;
    if (frame == 0) {
        return false;
    }
    uint64_t region_sz = frame;

    uint64_t total64 = header_sz + region_sz + region_sz;
    if (total64 > SIZE_T(~0ULL)) return false;

    c->total = SIZE_T(total64);
    c->py2p  = (uint8_t*)c->base + header_sz;         // legacy layout: PY region first
    c->ps2p  = c->py2p + region_sz;                  // PS region second
    return true;
}

static HANDLE create_or_open_event(const std::wstring& name) {
    HANDLE h = CreateEventW(nullptr, FALSE, FALSE, name.c_str());
    if (!h && GetLastError() == ERROR_ACCESS_DENIED) {
        h = OpenEventW(EVENT_MODIFY_STATE | SYNCHRONIZE, FALSE, name.c_str());
    }
    return h;
}

static HANDLE create_or_open_mutex(const std::wstring& name) {
    HANDLE h = CreateMutexW(nullptr, FALSE, name.c_str());
    if (!h && GetLastError() == ERROR_ACCESS_DENIED) {
        h = OpenMutexW(SYNCHRONIZE | MUTEX_MODIFY_STATE, FALSE, name.c_str());
    }
    return h;
}

static void close_handle(HANDLE& h) { if (h) { CloseHandle(h); h = nullptr; } }

static int lock_mutex(HANDLE mtx, DWORD timeout_ms) {
    DWORD w = WaitForSingleObject(mtx, timeout_ms);
    if (w == WAIT_OBJECT_0) return VS_OK;
    if (w == WAIT_TIMEOUT)  return VS_TIMEOUT;
    return VS_SYS_ERROR;
}

static void unlock_mutex(HANDLE mtx) { ReleaseMutex(mtx); }

// Choose Local\ fallback if Global\ fails with ACCESS_DENIED
static std::wstring try_local_on_denied(std::wstring name, bool use_fallback) {
    if (!use_fallback) return name;
    if (name.rfind(L"Global\\", 0) == 0) {
        name.replace(0, 7, L"Local\\");
    }
    return name;
}

VS_API VS_Channel VS_OpenChannel(const wchar_t* name,
                                 uint64_t frame_bytes,
                                 uint32_t num_slots,
                                 int      use_global_fallback)
{
    if (!name || frame_bytes == 0 || num_slots == 0) return nullptr;
    if (num_slots != 1) return nullptr; // legacy channel supports a single slot

    auto ch = std::make_unique<Channel>();
    std::wstring wname(name);

    // 1) Create mapping
    uint64_t total64 = sizeof(VS_Header) + frame_bytes * num_slots * 2ull;
    if (total64 > SIZE_T(~0ULL)) return nullptr;

    HANDLE hMap = CreateFileMappingW(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE,
                                     (DWORD)(total64 >> 32), (DWORD)(total64 & 0xFFFFFFFF),
                                     wname.c_str());
    if (!hMap && GetLastError() == ERROR_ACCESS_DENIED) {
        std::wstring alt = try_local_on_denied(wname, use_global_fallback != 0);
        if (alt != wname) {
            hMap = CreateFileMappingW(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE,
                                      (DWORD)(total64 >> 32), (DWORD)(total64 & 0xFFFFFFFF),
                                      alt.c_str());
            wname = alt;
        }
    }
    if (!hMap) return nullptr;

    ch->hMap = hMap;
    ch->base = (uint8_t*)MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, (SIZE_T)total64);
    if (!ch->base) return nullptr;

    ch->hdr = reinterpret_cast<VS_Header*>(ch->base);
    if (ch->hdr->magic_and_version != MAGIC64) {
        ZeroMemory(ch->base, (SIZE_T)total64);
        ch->hdr->magic             = MAGIC;
        ch->hdr->version           = VER;
        ch->hdr->magic_and_version = MAGIC64;
        ch->hdr->frame_bytes       = frame_bytes;
        atomic_store_u64(reinterpret_cast<volatile LONG64*>(&ch->hdr->python_seq), 0);
        atomic_store_u64(reinterpret_cast<volatile LONG64*>(&ch->hdr->powershell_seq), 0);
        atomic_store_u64(reinterpret_cast<volatile LONG64*>(&ch->hdr->python_length), 0);
        atomic_store_u64(reinterpret_cast<volatile LONG64*>(&ch->hdr->powershell_length), 0);
    } else if (ch->hdr->frame_bytes != frame_bytes) {
        return nullptr; // incompatible frame size
    }
    if (!map_layout(ch.get())) return nullptr;

    // 2) Synchronization primitives
    std::wstring mtxName  = makeMutexName(wname);
    std::wstring evPsReqN = makeEventName(wname, L":ev_ps_req");
    std::wstring evPsAckN = makeEventName(wname, L":ev_ps_ack");
    std::wstring evPyReqN = makeEventName(wname, L":ev_py_req");
    std::wstring evPyAckN = makeEventName(wname, L":ev_py_ack");

    ch->hMutex = create_or_open_mutex(mtxName);
    ch->evPsReq = create_or_open_event(evPsReqN);
    ch->evPsAck = create_or_open_event(evPsAckN);
    ch->evPyReq = create_or_open_event(evPyReqN);
    ch->evPyAck = create_or_open_event(evPyAckN);
    if (!ch->hMutex || !ch->evPsReq || !ch->evPsAck || !ch->evPyReq || !ch->evPyAck) return nullptr;

    // Warm-up (touch first byte of each region to fault pages)
    volatile uint8_t touch = ch->ps2p[0] ^ ch->py2p[0];
    (void)touch;

    return ch.release();
}

VS_API void VS_CloseChannel(VS_Channel handle) {
    if (!handle) return;
    Channel* c = reinterpret_cast<Channel*>(handle);
    if (c->base) { UnmapViewOfFile(c->base); c->base = nullptr; }
    close_handle(c->evPsReq);
    close_handle(c->evPsAck);
    close_handle(c->evPyReq);
    close_handle(c->evPyAck);
    close_handle(c->hMutex);
    close_handle(c->hMap);
    delete c;
}

static int write_direction(Channel* c,
                           bool ps_to_py,
                           const uint8_t* data,
                           uint64_t len,
                           uint32_t timeout_ms,
                           uint64_t* next_seq)
{
    if (!c) return VS_INVALID_ARG;
    if (len > c->hdr->frame_bytes) return VS_INVALID_ARG;
    if (len > 0 && !data) return VS_INVALID_ARG;

    HANDLE evReq = ps_to_py ? c->evPsReq : c->evPyReq;
    HANDLE evAck = ps_to_py ? c->evPsAck : c->evPyAck;
    uint8_t* dst = ps_to_py ? c->ps2p : c->py2p;

    int lk = lock_mutex(c->hMutex, timeout_ms);
    if (lk != VS_OK) return lk;

    if (len > 0) {
        memcpy(dst, data, static_cast<size_t>(len));
    }

    if (ps_to_py) {
        atomic_store_u64(reinterpret_cast<volatile LONG64*>(&c->hdr->powershell_length), len);
    } else {
        atomic_store_u64(reinterpret_cast<volatile LONG64*>(&c->hdr->python_length), len);
    }
    uint64_t seq_value = ps_to_py
        ? atomic_inc_u64(reinterpret_cast<volatile LONG64*>(&c->hdr->powershell_seq))
        : atomic_inc_u64(reinterpret_cast<volatile LONG64*>(&c->hdr->python_seq));
    if (next_seq) *next_seq = seq_value;

    unlock_mutex(c->hMutex);

    if (evReq) {
        SetEvent(evReq); // best-effort wake-up for event-aware consumers
    }
    if (evAck) {
        WaitForSingleObject(evAck, 0); // non-blocking wait; ack optional for legacy flows
    }

    return VS_OK;
}

static int read_direction(Channel* c,
                          bool read_py_to_ps, // true => read PY→PS; false => read PS→PY
                          uint8_t* dst,
                          uint64_t cap,
                          uint64_t* out_len,
                          uint32_t timeout_ms)
{
    if (!c || !out_len) return VS_INVALID_ARG;

    HANDLE evReq = read_py_to_ps ? c->evPyReq : c->evPsReq;
    HANDLE evAck = read_py_to_ps ? c->evPyAck : c->evPsAck;
    uint8_t* src = read_py_to_ps ? c->py2p : c->ps2p;

    uint64_t* last_seq_ptr = read_py_to_ps ? &c->last_python_seq_read : &c->last_powershell_seq_read;
    volatile LONG64* seq_ptr = reinterpret_cast<volatile LONG64*>(read_py_to_ps
        ? &c->hdr->python_seq
        : &c->hdr->powershell_seq);
    volatile LONG64* len_ptr = reinterpret_cast<volatile LONG64*>(read_py_to_ps
        ? &c->hdr->python_length
        : &c->hdr->powershell_length);

    const uint64_t timeout64 = timeout_ms;
    ULONGLONG start_tick = GetTickCount64();
    const bool infinite_timeout = (timeout_ms == INFINITE);

    while (true) {
        uint64_t seq_value = atomic_load_u64(seq_ptr);
        if (seq_value > *last_seq_ptr) {
            break;
        }

        if (timeout_ms == 0) {
            return VS_TIMEOUT;
        }

        if (!read_py_to_ps && evReq) {
            DWORD wait_ms = INFINITE;
            if (!infinite_timeout) {
                ULONGLONG now = GetTickCount64();
                uint64_t elapsed = static_cast<uint64_t>(now - start_tick);
                if (elapsed >= timeout64) {
                    if (atomic_load_u64(seq_ptr) > *last_seq_ptr) {
                        break;
                    }
                    return VS_TIMEOUT;
                }
                wait_ms = static_cast<DWORD>(timeout64 - elapsed);
            }

            DWORD wait_rc = WaitForSingleObject(evReq, wait_ms);
            if (wait_rc == WAIT_FAILED) {
                return VS_SYS_ERROR;
            }
            if (wait_rc == WAIT_TIMEOUT) {
                if (atomic_load_u64(seq_ptr) > *last_seq_ptr) {
                    break;
                }
                return VS_TIMEOUT;
            }
            // WAIT_OBJECT_0 -> loop and re-check sequence
            continue;
        }

        if (!infinite_timeout) {
            ULONGLONG now = GetTickCount64();
            uint64_t elapsed = static_cast<uint64_t>(now - start_tick);
            if (elapsed >= timeout64) {
                if (atomic_load_u64(seq_ptr) > *last_seq_ptr) {
                    break;
                }
                return VS_TIMEOUT;
            }
        }

        Sleep(1);
    }

    int lk = lock_mutex(c->hMutex, timeout_ms);
    if (lk != VS_OK) return lk;

    uint64_t length = atomic_load_u64(len_ptr);
    if (length > c->hdr->frame_bytes) {
        unlock_mutex(c->hMutex);
        if (evAck) SetEvent(evAck);
        return VS_BAD_STATE;
    }

    *out_len = length;

    if (dst) {
        if (cap < length) {
            unlock_mutex(c->hMutex);
            if (evAck) SetEvent(evAck);
            return VS_SMALL_BUFFER;
        }
        if (length > 0) {
            memcpy(dst, src, static_cast<size_t>(length));
        }
    }

    *last_seq_ptr = atomic_load_u64(seq_ptr);

    unlock_mutex(c->hMutex);
    if (evAck) SetEvent(evAck);
    return VS_OK;
}

VS_API int32_t VS_WritePs2Py(VS_Channel ch,
                             const uint8_t* data,
                             uint64_t len,
                             uint32_t timeout_ms,
                             uint64_t* next_seq)
{
    Channel* c = reinterpret_cast<Channel*>(ch);
    return write_direction(c, /*ps_to_py*/true, data, len, timeout_ms, next_seq);
}

VS_API int32_t VS_WritePy2Ps(VS_Channel ch,
                             const uint8_t* data,
                             uint64_t len,
                             uint32_t timeout_ms,
                             uint64_t* next_seq)
{
    Channel* c = reinterpret_cast<Channel*>(ch);
    return write_direction(c, /*ps_to_py*/false, data, len, timeout_ms, next_seq);
}

VS_API int32_t VS_ReadPy2Ps(VS_Channel ch,
                            uint8_t* dst,
                            uint64_t dst_cap,
                            uint64_t* out_len,
                            uint32_t timeout_ms)
{
    Channel* c = reinterpret_cast<Channel*>(ch);
    return read_direction(c, /*read_py_to_ps*/true, dst, dst_cap, out_len, timeout_ms);
}

VS_API int32_t VS_ReadPs2Py(VS_Channel ch,
                            uint8_t* dst,
                            uint64_t dst_cap,
                            uint64_t* out_len,
                            uint32_t timeout_ms)
{
    Channel* c = reinterpret_cast<Channel*>(ch);
    return read_direction(c, /*read_py_to_ps*/false, dst, dst_cap, out_len, timeout_ms);
}

VS_API int32_t VS_GetHeader(VS_Channel ch, VS_Header* out) {
    if (!ch || !out) return VS_INVALID_ARG;
    Channel* c = reinterpret_cast<Channel*>(ch);
    VS_Header snapshot{};
    snapshot.magic_and_version = c->hdr->magic_and_version;
    snapshot.frame_bytes = c->hdr->frame_bytes;
    snapshot.python_seq = atomic_load_u64(reinterpret_cast<volatile LONG64*>(&c->hdr->python_seq));
    snapshot.powershell_seq = atomic_load_u64(reinterpret_cast<volatile LONG64*>(&c->hdr->powershell_seq));
    snapshot.python_length = atomic_load_u64(reinterpret_cast<volatile LONG64*>(&c->hdr->python_length));
    snapshot.powershell_length = atomic_load_u64(reinterpret_cast<volatile LONG64*>(&c->hdr->powershell_length));
    memcpy(snapshot.reserved, c->hdr->reserved, sizeof(snapshot.reserved));
    *out = snapshot;
    return VS_OK;
}
