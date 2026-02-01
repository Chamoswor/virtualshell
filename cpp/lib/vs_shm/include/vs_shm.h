// vs_shm.h - Zero-Copy Shared Memory API (Clean Redesign)
// ================================================
// Design principles:
// 1. Python owns channel lifecycle (creates/destroys)
// 2. All transfers use chunking (consistent, predictable)
// 3. Zero-copy via offset-based shared memory access
// 4. PowerShell serializes objects to bytes via C++/CLI

#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef _WIN32
  #define VS_API extern "C" __declspec(dllexport)
#else
  #define VS_API extern "C"
#endif

// =============================================================================
// CONSTANTS
// =============================================================================

static const uint32_t VS_MAGIC = 0x5653484D;  // 'VSHM'
static const uint32_t VS_VERSION = 2;

static const uint32_t VS_DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;  // 4 MB
static const uint32_t VS_DEFAULT_FRAME_SIZE = 64 * 1024 * 1024; // 64 MB

// =============================================================================
// STATUS CODES
// =============================================================================

enum VS_Status : int32_t {
    VS_OK            =  0,
    VS_TIMEOUT       =  1,
    VS_WOULD_BLOCK   =  2,
    VS_ERR_INVALID   = -1,
    VS_ERR_SYSTEM    = -2,
    VS_ERR_BAD_STATE = -3,
    VS_ERR_TOO_LARGE = -4
};

// =============================================================================
// HEADER STRUCTURE
// =============================================================================

struct VS_Header {
    uint32_t magic;            // 'VSHM' marker
    uint32_t version;          // Protocol version (2)
    uint64_t frame_bytes;      // Size of data region per direction
    
    // Python → PowerShell transfer state (48 bytes)
    struct {
        uint64_t total_size;   // Total bytes to transfer
        uint64_t chunk_size;   // Bytes per chunk
        uint32_t num_chunks;   // Total number of chunks
        uint32_t current_chunk;// Current chunk index (0-based)
        uint64_t chunk_offset; // Offset in shared memory for current chunk
        uint64_t chunk_length; // Length of current chunk
        uint32_t chunk_ready;  // 1 = chunk available, 0 = none
        uint32_t transfer_done;// 1 = all chunks sent, 0 = in progress
    } py2ps;
    
    // PowerShell → Python transfer state (48 bytes)
    struct {
        uint64_t total_size;
        uint64_t chunk_size;
        uint32_t num_chunks;
        uint32_t current_chunk;
        uint64_t chunk_offset;
        uint64_t chunk_length;
        uint32_t chunk_ready;
        uint32_t transfer_done;
    } ps2py;
    
    uint64_t reserved[10];      // Future use (80 bytes to reach 192 total)
};

// Total: 4+4+8 + 48 + 48 + 80 = 192 bytes
static_assert(sizeof(VS_Header) == 192, "VS_Header must be 192 bytes");

// =============================================================================
// CHANNEL HANDLE
// =============================================================================

typedef void* VS_Channel;

// =============================================================================
// CHANNEL LIFECYCLE (Python side)
// =============================================================================

// Create channel - Python owns lifecycle
// name: e.g. "Local\\VS_Channel_123" or "Global\\VS_Channel_123"
// frame_bytes: size of data region per direction (default: 64 MB)
VS_API VS_Channel VS_CreateChannel(
    const wchar_t* name,
    uint64_t frame_bytes
);

// Close channel - Python calls this when done
VS_API void VS_DestroyChannel(VS_Channel ch);

// =============================================================================
// PYTHON → POWERSHELL TRANSFER
// =============================================================================

// Begin chunked transfer (Python side)
// Sets up metadata for PowerShell to read chunks
VS_API int32_t VS_BeginPy2PsTransfer(
    VS_Channel ch,
    uint64_t total_size,
    uint64_t chunk_size
);

// Send one chunk (Python side)
// chunk_index: 0-based chunk number
// data: chunk data to copy into shared memory
// length: size of this chunk
VS_API int32_t VS_SendPy2PsChunk(
    VS_Channel ch,
    uint32_t chunk_index,
    const uint8_t* data,
    uint64_t length,
    uint32_t timeout_ms
);

// Wait for PowerShell to acknowledge chunk (Python side)
VS_API int32_t VS_WaitPy2PsAck(
    VS_Channel ch,
    uint32_t timeout_ms
);

// Mark transfer complete (Python side)
VS_API int32_t VS_FinishPy2PsTransfer(VS_Channel ch);

// =============================================================================
// POWERSHELL → PYTHON TRANSFER
// =============================================================================

// Begin chunked transfer (PowerShell side)
VS_API int32_t VS_BeginPs2PyTransfer(
    VS_Channel ch,
    uint64_t total_size,
    uint64_t chunk_size
);

// Send one chunk (PowerShell side)
VS_API int32_t VS_SendPs2PyChunk(
    VS_Channel ch,
    uint32_t chunk_index,
    const uint8_t* data,
    uint64_t length,
    uint32_t timeout_ms
);

// Wait for Python to acknowledge chunk (PowerShell side)
VS_API int32_t VS_WaitPs2PyAck(
    VS_Channel ch,
    uint32_t timeout_ms
);

// Mark transfer complete (PowerShell side)
VS_API int32_t VS_FinishPs2PyTransfer(VS_Channel ch);

// =============================================================================
// ZERO-COPY RECEIVE (Python side reads PowerShell chunks)
// =============================================================================

// Wait for next chunk from PowerShell
// Returns offset and length in shared memory - Python can read directly
VS_API int32_t VS_WaitPs2PyChunk(
    VS_Channel ch,
    uint32_t* out_chunk_index,
    uint64_t* out_offset,
    uint64_t* out_length,
    uint32_t timeout_ms
);

// Acknowledge chunk received (Python side)
VS_API int32_t VS_AckPs2PyChunk(VS_Channel ch);

// Check if transfer is complete
VS_API int32_t VS_IsPs2PyComplete(VS_Channel ch);

// =============================================================================
// ZERO-COPY RECEIVE (PowerShell side reads Python chunks)
// =============================================================================

// Wait for next chunk from Python
VS_API int32_t VS_WaitPy2PsChunk(
    VS_Channel ch,
    uint32_t* out_chunk_index,
    uint64_t* out_offset,
    uint64_t* out_length,
    uint32_t timeout_ms
);

// Acknowledge chunk received (PowerShell side)
VS_API int32_t VS_AckPy2PsChunk(VS_Channel ch);

// Check if transfer is complete
VS_API int32_t VS_IsPy2PsComplete(VS_Channel ch);

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

// Get shared memory base pointer (for calculating absolute addresses)
VS_API void* VS_GetMemoryBase(VS_Channel ch);

// Get header info
VS_API int32_t VS_GetHeader(VS_Channel ch, VS_Header* out);

// =============================================================================
// OBJECT SERIALIZATION (PowerShell → Bytes via C++/CLI)
// =============================================================================

// Serialize PowerShell object to bytes via GCHandle
// gc_handle: GCHandle.ToIntPtr() from PowerShell
// Returns allocated byte array (caller must free with VS_FreeBytes)
VS_API int32_t VS_SerializeObject(
    intptr_t gc_handle,
    uint8_t** out_bytes,
    uint64_t* out_length
);

// Free bytes allocated by VS_SerializeObject
VS_API void VS_FreeBytes(uint8_t* bytes);

// Combined: Serialize object and send via chunked transfer
// This is the most efficient path for PowerShell objects
VS_API int32_t VS_SendObjectPs2Py(
    VS_Channel ch,
    intptr_t gc_handle,
    uint64_t chunk_size,
    uint32_t timeout_ms
);
