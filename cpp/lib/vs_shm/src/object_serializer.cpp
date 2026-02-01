// object_serializer.cpp - Clean PowerShell object serialization via C++/CLI
#pragma once
#pragma managed



#include <algorithm>
#include <cstdint>
#include <cstring>
#include <cstdlib>

#include "../include/vs_shm.h"

#using <System.dll>
#using <System.Core.dll>
#using <System.Management.Automation.dll>

using namespace System;
using namespace System::Text;
using namespace System::Management::Automation;
using namespace System::Runtime::InteropServices;

// =============================================================================
// MANAGED SERIALIZATION LOGIC
// =============================================================================

static array<Byte>^ SerializeObjectToBytes(Object^ obj, String^ encoding) {
    if (obj == nullptr) {
        return gcnew array<Byte>(0);
    }

    // Fast path: already bytes
    if (obj->GetType() == array<Byte>::typeid) {
        return safe_cast<array<Byte>^>(obj);
    }

    // Fast path: string
    if (obj->GetType() == String::typeid) {
        Encoding^ enc = Encoding::GetEncoding(encoding);
        return enc->GetBytes(safe_cast<String^>(obj));
    }

    // Unwrap PSObject if needed
    PSObject^ psObj = dynamic_cast<PSObject^>(obj);
    if (static_cast<Object^>(psObj) != nullptr) {
        obj = psObj->BaseObject;
        
        if (obj->GetType() == array<Byte>::typeid) {
            return safe_cast<array<Byte>^>(obj);
        }
        if (obj->GetType() == String::typeid) {
            Encoding^ enc = Encoding::GetEncoding(encoding);
            return enc->GetBytes(safe_cast<String^>(obj));
        }
    }

    // Complex object: serialize with CliXml
    try {
        String^ xml = PSSerializer::Serialize(obj);
        Encoding^ enc = Encoding::GetEncoding(encoding);
        return enc->GetBytes(xml);
    }
    catch (Exception^) {
        // Fallback: ToString()
        Encoding^ enc = Encoding::GetEncoding(encoding);
        return enc->GetBytes(obj->ToString());
    }
}

// =============================================================================
// UNMANAGED EXPORTS
// =============================================================================

#pragma unmanaged
extern "C" {
    __declspec(dllexport) int SerializeObjectViaGCHandle(
        intptr_t gc_handle,
        const wchar_t* encoding_name,
        unsigned char** out_bytes,
        unsigned long long* out_len
    );
    
    __declspec(dllexport) void FreeManagedBytes(unsigned char* bytes);
}

#pragma managed
int SerializeObjectViaGCHandle(intptr_t gc_handle, const wchar_t* encoding_name, unsigned char** out_bytes, unsigned long long* out_len) {
    if (!out_bytes || !out_len) return -1;

    try {
        // Get object from GCHandle
        IntPtr handle = IntPtr(gc_handle);
        GCHandle gcHandle = GCHandle::FromIntPtr(handle);
        Object^ obj = gcHandle.Target;
        
        if (obj == nullptr) {
            *out_bytes = nullptr;
            *out_len = 0;
            return -1;
        }
        
        // Serialize to bytes
        String^ encodingStr = encoding_name ? gcnew String(encoding_name) : "utf-8";
        array<Byte>^ bytes = SerializeObjectToBytes(obj, encodingStr);
        
        if (bytes->Length == 0) {
            *out_bytes = nullptr;
            *out_len = 0;
            return 0;  // Empty is valid
        }
        
        // Allocate unmanaged memory and copy
        pin_ptr<Byte> pinnedBytes = &bytes[0];
        *out_len = bytes->Length;
        *out_bytes = (unsigned char*)malloc((size_t)*out_len);
        
        if (*out_bytes == nullptr) {
            return -1;
        }
        
        memcpy(*out_bytes, pinnedBytes, (size_t)*out_len);
        return 0;
    }
    catch (...) {
        *out_bytes = nullptr;
        *out_len = 0;
        return -1;
    }
}

#pragma managed
void FreeManagedBytes(unsigned char* bytes) {
    if (bytes) {
        free(bytes);
    }
}

// =============================================================================
// DIRECT CHUNKED SEND (MANAGED FAST PATH)
// =============================================================================

static int SendPinnedBytesToPython(VS_Channel handle, const unsigned char* basePtr, uint64_t totalSize, uint64_t chunkSize, uint32_t timeoutMs) {
    if (chunkSize == 0) {
        chunkSize = totalSize > 0 ? totalSize : 1;
    }

    int32_t result = VS_BeginPs2PyTransfer(handle, totalSize, chunkSize);
    if (result != VS_OK) {
        return result;
    }

    if (totalSize == 0) {
        return VS_FinishPs2PyTransfer(handle);
    }

    uint64_t offset = 0;
    uint32_t chunkIndex = 0;

    while (offset < totalSize) {
        const uint64_t remaining = totalSize - offset;
        const uint64_t chunkLen = std::min<uint64_t>(chunkSize, remaining);
        const unsigned char* chunkPtr = basePtr + offset;

        result = VS_SendPs2PyChunk(handle, chunkIndex, chunkPtr, chunkLen, timeoutMs);
        if (result != VS_OK) {
            return result;
        }

        result = VS_WaitPs2PyAck(handle, timeoutMs);
        if (result != VS_OK) {
            return result;
        }

        offset += chunkLen;
        ++chunkIndex;
    }

    return VS_FinishPs2PyTransfer(handle);
}

#pragma managed
extern "C" __declspec(dllexport) int VS_SendObjectPs2Py(VS_Channel handle, intptr_t gc_handle, uint64_t chunk_size, uint32_t timeout_ms) {
    if (!handle) {
        return VS_ERR_INVALID;
    }

    try {
        System::IntPtr handlePtr(gc_handle);
        GCHandle gcHandle = GCHandle::FromIntPtr(handlePtr);
        System::Object^ obj = gcHandle.Target;

        if (obj == nullptr) {
            return SendPinnedBytesToPython(handle, nullptr, 0, chunk_size, timeout_ms);
        }

        System::String^ encodingName = gcnew System::String(L"utf-8");
        array<Byte>^ bytes = SerializeObjectToBytes(obj, encodingName);
        const uint64_t totalSize = static_cast<uint64_t>(bytes->LongLength);

        if (totalSize == 0) {
            return SendPinnedBytesToPython(handle, nullptr, 0, chunk_size, timeout_ms);
        }

        pin_ptr<Byte> pinned = &bytes[0];
        const unsigned char* basePtr = reinterpret_cast<const unsigned char*>(pinned);

        return SendPinnedBytesToPython(handle, basePtr, totalSize, chunk_size, timeout_ms);
    }
    catch (const std::exception&) {
        return VS_ERR_SYSTEM;
    }
    catch (System::Exception^) {
        return VS_ERR_SYSTEM;
    }
}
