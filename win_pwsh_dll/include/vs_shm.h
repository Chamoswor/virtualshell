// include/vs_shm.h
#pragma once
#include <stdint.h>
#include <stddef.h>
#if defined(_WIN32)
  #define VS_API extern "C" __declspec(dllexport)
#else
  #define VS_API extern "C"
#endif

static const uint32_t VS_HEADER_MAGIC = 0x4D485356u; // 'VSHM'
static const uint32_t VS_HEADER_VERSION = 1u;

// Opaque handle
typedef void* VS_Channel;

// Return codes
enum VS_Status : int32_t {
    VS_OK                  = 0,
    VS_TIMEOUT             = 1,
    VS_WOULD_BLOCK         = 2,
    VS_SMALL_BUFFER        = 3,
    VS_INVALID_ARG         = -1,
    VS_SYS_ERROR           = -2,
    VS_BAD_STATE           = -3
};

// Header (mirrors the mapped layout)
struct VS_Header {
  union {
    struct {
      uint32_t magic;      // 'VSHM' marker
      uint32_t version;    // protocol version
    };
    uint64_t magic_and_version; // compatibility with legacy writers
  };
  uint64_t frame_bytes;       // payload bytes per frame
  uint64_t python_seq;        // last PY→PS write seq (legacy python_seq)
  uint64_t powershell_seq;    // last PS→PY write seq (legacy powershell_seq)
  uint64_t python_length;     // length of last PY→PS payload
  uint64_t powershell_length; // length of last PS→PY payload
  uint64_t reserved[10];      // reserved / legacy padding
  // layout: [header][PY2P region][PS2P region]
};

#if defined(__cplusplus)
static_assert(sizeof(VS_Header) == 128, "VS_Header must be 128 bytes");
#endif

// Open or create a channel.
//
// name: e.g. "Local\\VS:MMF:MyChannel" or "Global\\VS:MMF:MyChannel"
// frame_bytes: per-frame payload capacity (e.g. 64*1024*1024)
// num_slots: number of frames per direction (>=1)
// use_global_fallback: if nonzero and CreateFileMappingW with Global\ fails w/ ERROR_ACCESS_DENIED,
//                      retry with Local\ automatically.
//
// Returns nullptr on error.
VS_API VS_Channel VS_OpenChannel(const wchar_t* name,
                                 uint64_t frame_bytes,
                                 uint32_t num_slots,
                                 int      use_global_fallback);

// Close & free resources (safe to pass nullptr)
VS_API void VS_CloseChannel(VS_Channel ch);

// Blocking write PowerShell→Python
// data/len: payload to write
// timeout_ms: 0=nonblocking, INFINITE=wait forever
// next_seq(out): returns new seq counter (seq_ps after write)
VS_API int32_t VS_WritePs2Py(VS_Channel ch,
                             const uint8_t* data,
                             uint64_t len,
                             uint32_t timeout_ms,
                             uint64_t* next_seq);

// Blocking read Python→PowerShell
// dst/dst_cap: user buffer. out_len returns payload length copied.
// If dst_cap is smaller than payload, returns VS_SMALL_BUFFER and out_len set to required length (no copy).
VS_API int32_t VS_ReadPy2Ps(VS_Channel ch,
                            uint8_t* dst,
                            uint64_t dst_cap,
                            uint64_t* out_len,
                            uint32_t timeout_ms);

// Nonblocking probe: get header snapshot
VS_API int32_t VS_GetHeader(VS_Channel ch, VS_Header* out);

// Signal helpers for PY→PS writers (optional for your PY side, exposed for symmetry)
VS_API int32_t VS_WritePy2Ps(VS_Channel ch,
                             const uint8_t* data,
                             uint64_t len,
                             uint32_t timeout_ms,
                             uint64_t* next_seq);

VS_API int32_t VS_ReadPs2Py(VS_Channel ch,
                            uint8_t* dst,
                            uint64_t dst_cap,
                            uint64_t* out_len,
                            uint32_t timeout_ms);
